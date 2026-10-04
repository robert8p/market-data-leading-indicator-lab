"""Frozen-policy quote/trade execution replay, without market IO or live orders.

Produces explicitly MODELLED execution opportunities from certified as-traded
quote intervals and trade witnesses. A price touch alone never fills an order.
The caller must resolve source, policy, size/cost and timing receipts before
using reserved evidence. This pure kernel cannot certify those assumptions.
"""
from __future__ import annotations

from bisect import bisect_right
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import hashlib
import json
import math

VERSION = 'EQ20_QUOTE_TRADE_EXECUTION_REPLAY_V1'
POLICY_VERSION = 'EQ20_FIXED_QUOTE_TRADE_EXECUTION_POLICY_V1'
MAX_QUOTES = 200000
MAX_TRADES = 200000


class Rejected(ValueError):
    pass


def need(condition, reason):
    if not condition:
        raise Rejected(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def when(value):
    need(isinstance(value, str), 'EXECUTION_TIMESTAMP_REQUIRED')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    need(result.tzinfo is not None, 'EXECUTION_TIMEZONE_REQUIRED')
    return result.astimezone(timezone.utc)


def decimal(value, *, positive=False):
    need(type(value) in (int, float, str), 'EXECUTION_FINITE_NUMBER_REQUIRED')
    result = Decimal(str(value))
    need(result.is_finite() and (not positive or result > 0), 'EXECUTION_FINITE_NUMBER_REQUIRED')
    return result


def whole(value, *, minimum=0, maximum=100000000):
    need(type(value) is int and minimum <= value <= maximum, 'EXECUTION_INTEGER_BOUND')
    return value


def validate_policy(policy):
    need(isinstance(policy, dict) and policy.get('version') == POLICY_VERSION, 'FROZEN_EXECUTION_POLICY_REQUIRED')
    need(policy.get('execution_mode') == 'HYPOTHETICAL_RESEARCH_NO_LIVE_ORDERS'
         and policy.get('fill_model') == 'MARKETABLE_QUOTE_WITH_SUBSEQUENT_TRADE_WITNESS'
         and policy.get('commission_model') == 'MAX_MINIMUM_PER_ORDER_AND_PER_SHARE_PLUS_NOTIONAL'
         and policy.get('unavailable_or_stale_quote_action') == 'NO_FILL_FROZEN_OBSERVABILITY_GUARD'
         and policy.get('target_order_sizing_clock') == 'AFTER_PREVIOUS_IOC_WITNESS_COMPLETES'
         and policy.get('quote_capture_scope') == 'FROZEN_FIRST_SIGNAL_TO_SAME_SESSION_CLOSE'
         and policy.get('liquidation_order_type') == 'MARKETABLE_LIMIT_IOC_AT_BID_MINUS_FROZEN_SLIPPAGE'
         and policy.get('entry_order_type') == 'MARKETABLE_LIMIT_IOC'
         and policy.get('target_order_type') == 'MARKETABLE_LIMIT_IOC'
         and policy.get('exit_policy') == 'FIRST_EXECUTABLE_TWENTY_PERCENT_TARGET_ELSE_FIXED_CLOCK_LIQUIDATION'
         and policy.get('closing_auction_participation') is False
         and policy.get('last_print_as_fill') is False
         and policy.get('bar_touch_as_fill') is False, 'EXACT_EXECUTION_MODEL_SEMANTICS')
    need(policy.get('primary_response_delay_seconds') == 120
         and policy.get('response_sensitivity_seconds') == [30, 60, 120, 300], 'ORIGINAL_RESPONSE_SCENARIOS_REQUIRED')
    for field in ('notification_delay_seconds', 'order_submission_delay_seconds', 'entry_expiry_seconds',
                  'entry_cutoff_seconds_before_close', 'liquidation_start_seconds_before_close',
                  'liquidation_retry_seconds', 'last_order_seconds_before_close', 'maximum_quote_age_ms',
                  'minimum_quote_persistence_ms'):
        whole(policy.get(field), minimum=1 if field not in ('notification_delay_seconds',) else 0, maximum=3600)
    need(policy['liquidation_retry_seconds']*1000 >= policy['minimum_quote_persistence_ms'],
         'RETRY_INTERVAL_MUST_NOT_OVERLAP_LIQUIDITY_WITNESS_WINDOW')
    need(policy['liquidation_start_seconds_before_close'] > policy['last_order_seconds_before_close']
         and policy['entry_cutoff_seconds_before_close'] > policy['liquidation_start_seconds_before_close'],
         'FROZEN_ENTRY_EXIT_CLOCK_ORDER')
    for field in ('entry_reference_protection_bps', 'maximum_spread_bps', 'entry_slippage_bps',
                  'exit_slippage_bps', 'minimum_fee_per_order', 'fee_per_share', 'fee_notional_bps'):
        need(decimal(policy.get(field)) >= 0, 'NONNEGATIVE_FROZEN_COST_OR_PROTECTION')
    for field in ('maximum_displayed_participation', 'maximum_trade_participation'):
        need(0 < decimal(policy.get(field)) <= 1, 'FROZEN_PARTICIPATION_FRACTION')
    sizes = policy.get('notional_test_sizes')
    need(isinstance(sizes, list) and 1 <= len(sizes) <= 4
         and all(decimal(v, positive=True) <= 1000000 for v in sizes)
         and len(set(str(v) for v in sizes)) == len(sizes)
         and policy.get('primary_notional') in sizes, 'FROZEN_HYPOTHETICAL_NOTIONAL_TESTS')
    need(policy.get('partial_fill_primary_success') == 'REQUIRE_FULL_INTENDED_QUANTITY'
         and policy.get('unliquidated_inventory_state') == 'UNRESOLVED'
         and policy.get('fee_on_unfilled_orders') is False, 'RETAINED_PARTIAL_AND_UNLIQUIDATED_OUTCOMES')
    return digest(policy)


class Market:
    """Immutable interval index; no favourable quote or trade is selected back."""
    def __init__(self, payload, alert, policy_sha):
        need(isinstance(payload, dict) and payload.get('schema') == 'EQ20_CERTIFIED_QUOTE_TRADE_SESSION_V1',
             'CERTIFIED_QUOTE_TRADE_SESSION_SCHEMA')
        need(payload.get('session_date') == alert.get('session_date')
             and payload.get('security_id') == alert.get('security_id')
             and payload.get('execution_policy_sha256') == policy_sha, 'EXECUTION_INPUT_IDENTITY_AND_POLICY')
        self.open, self.close = when(payload['regular_open']), when(payload['regular_close'])
        self.identity = (payload['session_date'], payload['security_id'], policy_sha)
        capture_needed_from = when(alert['decision_ts']) if alert.get('decision_ts') else self.open
        self.coverage = payload.get('coverage', {})
        self.complete = (isinstance(self.coverage, dict)
             and all(self.coverage.get(name) is True for name in ('quotes_complete', 'trades_complete',
                 'timestamps_synchronized', 'quote_conditions_verified', 'trade_conditions_verified',
                 'identity_verified', 'as_traded_prices_verified', 'halts_complete'))
             and self.coverage.get('gaps') == []
             and when(self.coverage.get('start_at')) <= capture_needed_from
             and when(self.coverage.get('end_at')) >= self.close)
        self.quotes = []; self.trades = []
        quotes, trades = payload.get('quotes'), payload.get('trades')
        need(isinstance(quotes, list) and len(quotes) <= MAX_QUOTES
             and isinstance(trades, list) and len(trades) <= MAX_TRADES, 'EXECUTION_SOURCE_EVENT_BOUND')
        prior_end = None; quote_ids = set(); trade_ids = set(); prior_key = None
        for row in quotes:
            need(isinstance(row, dict), 'QUOTE_INTERVAL_OBJECT')
            start, end, available = when(row.get('start_at')), when(row.get('end_at')), when(row.get('available_at'))
            need(self.open <= start < end <= self.close and (prior_end is None or prior_end <= start),
                 'QUOTE_INTERVAL_OVERLAP_OR_SESSION_ORDER')
            need(isinstance(row.get('source_id'), str) and row['source_id'] and row['source_id'] not in quote_ids,
                 'UNIQUE_QUOTE_SOURCE_ID_REQUIRED')
            need(available >= start, 'QUOTE_RECEIPT_CANNOT_PRECEDE_EVENT')
            prior_end = end; quote_ids.add(row['source_id'])
            market_state = row.get('market_state')
            need(market_state in ('NORMAL', 'HALT', 'UNRESOLVED'), 'QUOTE_MARKET_STATE_REQUIRED')
            item = dict(row, start=start, end=end, available=available)
            if market_state == 'NORMAL':
                item.update(bid=decimal(row.get('bid'), positive=True), ask=decimal(row.get('ask'), positive=True),
                            bid_size=whole(row.get('bid_size')), ask_size=whole(row.get('ask_size')),
                            tick=decimal(row.get('tick_size'), positive=True))
                need(item['bid'] <= item['ask'], 'CROSSED_QUOTE_CANNOT_PROVE_EXECUTION')
            self.quotes.append(item)
        for row in trades:
            need(isinstance(row, dict), 'TRADE_RECORD_OBJECT')
            ts, available = when(row.get('trade_ts')), when(row.get('available_at'))
            key = (ts, whole(row.get('sequence')))
            need(self.open <= ts <= self.close and (prior_key is None or key > prior_key), 'EXACT_TRADE_TIME_SEQUENCE_ORDER')
            need(isinstance(row.get('source_id'), str) and row['source_id'] and row['source_id'] not in trade_ids, 'UNIQUE_TRADE_SOURCE_ID_REQUIRED')
            need(row.get('condition_state') in ('REGULAR_ELIGIBLE', 'EXCLUDED_CONDITION', 'UNRESOLVED'), 'TRADE_CONDITION_REQUIRED')
            need(available >= ts, 'TRADE_RECEIPT_CANNOT_PRECEDE_EVENT')
            prior_key = key; trade_ids.add(row['source_id'])
            self.trades.append(dict(row, ts=ts, available=available, price=decimal(row.get('price'), positive=True), size=whole(row.get('size'))))
        self.complete = self.complete and not any(q['market_state'] == 'UNRESOLVED' for q in self.quotes) and not any(
            t['condition_state'] == 'UNRESOLVED' for t in self.trades)
        self.starts = [q['start'] for q in self.quotes]
        self.trade_times = [t['ts'] for t in self.trades]
        self.consumed_quote_capacity = {}
        self.consumed_trade_capacity = {}

    def fork(self):
        # Parsed quotes/trades are immutable within this module. Each policy
        # scenario starts a separate hypothetical liquidity accounting ledger.
        result = object.__new__(Market)
        result.__dict__ = dict(self.__dict__)
        result.consumed_quote_capacity = {}
        result.consumed_trade_capacity = {}
        return result

    def at(self, timestamp):
        index = bisect_right(self.starts, timestamp)-1
        return self.quotes[index] if index >= 0 and timestamp < self.quotes[index]['end'] else None

    def opportunity(self, side, arrival, quantity, policy, *, limit=None):
        """One IOC at this fixed clock, not a search for a later better fill."""
        quote = self.at(arrival)
        if quote is None:
            return {'quantity': 0, 'state': 'NO_FILL' if self.complete else 'UNASSESSABLE', 'reason': 'NO_EXECUTABLE_QUOTE_AT_ORDER_CLOCK'}
        if quote['market_state'] != 'NORMAL':
            return {'quantity': 0, 'state': 'HALT_BLOCKED' if quote['market_state'] == 'HALT' else 'UNASSESSABLE',
                    'reason': 'HALT_OR_UNRESOLVED_QUOTE'}
        finish = arrival+timedelta(milliseconds=policy['minimum_quote_persistence_ms'])
        need(side in ('BUY', 'SELL'), 'FIXED_ORDER_SIDE')
        midpoint = (quote['bid']+quote['ask'])/2
        spread = (quote['ask']-quote['bid'])/midpoint*10000
        if quote['available'] > arrival or (arrival-quote['start']).total_seconds()*1000 > policy['maximum_quote_age_ms']:
            return {'quantity': 0, 'state': 'NO_FILL', 'reason': 'FROZEN_QUOTE_OBSERVABILITY_OR_STALENESS_DENIAL'}
        if finish >= quote['end'] or finish >= self.close or spread > decimal(policy['maximum_spread_bps']):
            return {'quantity': 0, 'state': 'NO_FILL', 'reason': 'FROZEN_PERSISTENCE_OR_SPREAD_CONSTRAINT'}
        px = quote['ask'] if side == 'BUY' else quote['bid']
        slip = decimal(policy['entry_slippage_bps' if side == 'BUY' else 'exit_slippage_bps'])/10000
        model_price = px*(1+slip if side == 'BUY' else 1-slip)
        model_price = (model_price/quote['tick']).to_integral_value(rounding=ROUND_CEILING if side == 'BUY' else ROUND_FLOOR)*quote['tick']
        need(model_price > 0, 'POSITIVE_MODELLED_EXECUTION_PRICE')
        if limit is not None and ((side == 'BUY' and model_price > limit) or (side == 'SELL' and model_price < limit)):
            return {'quantity': 0, 'state': 'NO_FILL', 'reason': 'FROZEN_LIMIT_NOT_MARKETABLE'}
        # Subsequent eligible trades witness liquidity; they do not imply our
        # order actually traded. Do not reuse pre-order volume or bar extremes.
        begin = bisect_right(self.trade_times, arrival); end = bisect_right(self.trade_times, finish)
        witnesses = [t for t in self.trades[begin:end] if t['condition_state'] == 'REGULAR_ELIGIBLE'
                     and t['available'] <= finish and ((side == 'BUY' and t['price'] >= px)
                                                      or (side == 'SELL' and t['price'] <= px))]
        trade_remaining = {t['source_id']: max(0, int((t['size']*decimal(policy['maximum_trade_participation'])).to_integral_value(
            rounding=ROUND_FLOOR))-self.consumed_trade_capacity.get(t['source_id'], 0)) for t in witnesses}
        trade_capacity = sum(trade_remaining.values())
        quote_key = (side, quote['source_id'])
        displayed_capacity = max(0, int((quote['ask_size' if side == 'BUY' else 'bid_size']*decimal(
            policy['maximum_displayed_participation'])).to_integral_value(rounding=ROUND_FLOOR))
            -self.consumed_quote_capacity.get(quote_key, 0))
        filled = min(quantity, trade_capacity, displayed_capacity)
        if filled <= 0:
            return {'quantity': 0, 'state': 'NO_FILL', 'reason': 'NO_SUFFICIENT_EXECUTABLE_LIQUIDITY_WITNESS'}
        self.consumed_quote_capacity[quote_key] = self.consumed_quote_capacity.get(quote_key, 0)+filled
        residual = filled; consumed = {}
        for trade in witnesses:
            key = trade['source_id']; used = min(residual, trade_remaining[key])
            if used:
                self.consumed_trade_capacity[key] = self.consumed_trade_capacity.get(key, 0)+used
                consumed[key] = used; residual -= used
            if not residual: break
        need(residual == 0, 'NO_REUSED_TRADE_LIQUIDITY_CAPACITY')
        return {'quantity': filled, 'state': 'FILLED' if filled == quantity else 'PARTIAL_FILL',
                'price': model_price, 'fill_ts': finish, 'quote_id': quote['source_id'],
                'trade_witness_ids': list(consumed), 'trade_witness_quantities': consumed,
                'model': 'MODELLED_NOT_ACTUAL_ORDER_FILL', 'displayed_capacity': displayed_capacity,
                'witness_trade_capacity': trade_capacity}


def fee(policy, price, quantity):
    if not quantity: return Decimal(0)
    return max(decimal(policy['minimum_fee_per_order']), decimal(policy['fee_per_share'])*quantity)+price*quantity*decimal(policy['fee_notional_bps'])/10000


def serial(value):
    if isinstance(value, Decimal): return str(value)
    if isinstance(value, datetime): return value.isoformat()
    if isinstance(value, dict): return {k: serial(v) for k, v in value.items()}
    if isinstance(value, list): return [serial(v) for v in value]
    return value


def replay_alert(alert, policy, market_payload, *, response_seconds=None, notional=None):
    """Replay one emitted alert; every branch retains its original denominator."""
    return _replay_alert(alert, policy, market_payload, response_seconds=response_seconds, notional=notional)


def _replay_alert(alert, policy, market_payload, *, response_seconds=None, notional=None, parsed_market=None):
    policy_sha = validate_policy(policy)
    need(isinstance(alert, dict) and alert.get('execution_policy_sha256') == policy_sha, 'ALERT_POLICY_HASH_PIN')
    decision = when(alert.get('decision_ts')); reference = decimal(alert.get('p_reference'), positive=True)
    response = policy['primary_response_delay_seconds'] if response_seconds is None else response_seconds
    size = policy['primary_notional'] if notional is None else notional
    need(response in policy['response_sensitivity_seconds'] and size in policy['notional_test_sizes'], 'ONLY_FROZEN_EXECUTION_SCENARIOS')
    market = Market(market_payload, alert, policy_sha) if parsed_market is None else parsed_market.fork()
    need(market.identity == (alert.get('session_date'), alert.get('security_id'), policy_sha), 'PARSED_EXECUTION_SOURCE_SCOPE')
    need(market.open+timedelta(minutes=10) <= decision <= market.close-timedelta(minutes=60), 'ORIGINAL_ALERT_WINDOW')
    received = decision+timedelta(seconds=policy['notification_delay_seconds'])
    ordered = received+timedelta(seconds=response)
    arrival = ordered+timedelta(seconds=policy['order_submission_delay_seconds'])
    label = {'decision_ts': alert['decision_ts'], 'execution_policy_sha256': policy_sha,
        'state': 'UNRESOLVED', 'fill_state': 'UNASSESSABLE', 'coverage_complete': market.complete,
        'ordering_ambiguity': not market.complete, 'reason_code': 'EXECUTION_INPUT_COVERAGE_NOT_CERTIFIED',
        'alert_received_ts': received.isoformat(), 'entry_order_ts': ordered.isoformat(),
        'model_scope': 'EXPLICITLY_MODELLED_QUOTE_TRADE_EXECUTION', 'n_original_alerts': 1,
        'hypothetical_notional': str(size), 'response_delay_seconds': response,
        'primary_scenario': response == 120 and size == policy['primary_notional'],
        'net_outcome': {'state': 'UNRESOLVED', 'net_return_on_filled_cost': None,
                        'net_return_on_hypothetical_notional': None, 'unliquidated_quantity': None},
        'research_objective_achieved': False, 'alpha_claimed': False}
    if not market.complete:
        return label
    if (arrival+timedelta(milliseconds=policy['minimum_quote_persistence_ms'])
            > ordered+timedelta(seconds=policy['entry_expiry_seconds'])
            or arrival >= market.close-timedelta(seconds=policy['entry_cutoff_seconds_before_close'])):
        return dict(label, state='NONQUALIFY', fill_state='EXPIRED', reason_code='FROZEN_ENTRY_CUTOFF')
    limit = reference*(1+decimal(policy['entry_reference_protection_bps'])/10000)
    intended = int((decimal(size, positive=True)/limit).to_integral_value(rounding=ROUND_FLOOR))
    if intended <= 0:
        return dict(label, state='NONQUALIFY', fill_state='NO_FILL', reason_code='HYPOTHETICAL_NOTIONAL_BELOW_ONE_SHARE')
    entry = market.opportunity('BUY', arrival, intended, policy, limit=limit)
    label.update(fill_state=entry['state'], intended_quantity=intended, filled_quantity=entry['quantity'],
                 entry_fill_evidence=serial(entry), reason_code=entry.get('reason'))
    if entry['quantity'] == 0:
        label['state'] = 'UNRESOLVED' if entry['state'] == 'UNASSESSABLE' else 'NONQUALIFY'
        return label
    qty, price, entry_at = entry['quantity'], entry['price'], entry['fill_ts']
    label.update(p_entry=float(price), entry_fill_ts=entry_at.isoformat(), ordering_ambiguity=False)
    entry_cost = price*qty+fee(policy, price, qty)
    remaining = qty; proceeds = Decimal(0); exit_fees = Decimal(0); exits = []; target_fills = []
    target_orders = []; uncertain_exits = []
    next_target_decision = entry_at+timedelta(microseconds=1)
    target = price*Decimal('1.20')
    liquidation = market.close-timedelta(seconds=policy['liquidation_start_seconds_before_close'])
    # A target trigger waits the frozen submission latency, then needs an
    # actually marketable bid plus full-size witnessed liquidity at that clock.
    for quote in market.quotes:
        trigger_at = max(quote['start'], quote['available'], next_target_decision)
        if trigger_at >= liquidation: break
        if quote['end'] <= trigger_at or quote['available'] > trigger_at or trigger_at < entry_at or quote['market_state'] != 'NORMAL' or quote['bid'] < target:
            continue
        exit_arrival = trigger_at+timedelta(seconds=policy['order_submission_delay_seconds'])
        outcome_observed_at = exit_arrival+timedelta(milliseconds=policy['minimum_quote_persistence_ms'])
        if outcome_observed_at >= liquidation: continue
        intended_exit = remaining
        offered = market.opportunity('SELL', exit_arrival, remaining, policy, limit=target)
        # Quantity from this IOC is not observable until its complete witness
        # window. Subsequent quote triggers cannot size an order using it early.
        next_target_decision = outcome_observed_at
        target_orders.append({'decision_at': trigger_at.isoformat(), 'arrival_at': exit_arrival.isoformat(),
            'outcome_observed_at': outcome_observed_at.isoformat(), 'intended_quantity': intended_exit,
            'fill_state': offered['state'], 'filled_quantity': offered['quantity']})
        if offered['state'] == 'UNASSESSABLE': uncertain_exits.append(serial(offered))
        if offered['quantity']:
            target_fills.append(offered); exits.append(offered); remaining -= offered['quantity']
            proceeds += offered['price']*offered['quantity']; exit_fees += fee(policy, offered['price'], offered['quantity'])
        if not remaining: break
    if remaining:
        at = max(liquidation, entry_at)+timedelta(seconds=policy['order_submission_delay_seconds'])
        end = market.close-timedelta(seconds=policy['last_order_seconds_before_close'])
        while remaining > 0 and at <= end:
            sale = market.opportunity('SELL', at, remaining, policy)
            if sale['state'] == 'UNASSESSABLE': uncertain_exits.append(serial(sale))
            if sale['quantity']:
                exits.append(sale); remaining -= sale['quantity']; proceeds += sale['price']*sale['quantity']
                exit_fees += fee(policy, sale['price'], sale['quantity'])
            at += timedelta(seconds=policy['liquidation_retry_seconds'])
    label['exit_fills'] = serial(exits)
    label['target_order_history'] = target_orders
    label['unassessable_exit_evidence'] = uncertain_exits
    label['net_outcome'] = {'state': 'UNRESOLVED' if remaining or uncertain_exits else 'MODELLED_SAME_SESSION_NET_OUTCOME',
        'entry_cost_including_fees': str(entry_cost), 'exit_proceeds_before_fees': str(proceeds),
        'exit_fees': str(exit_fees), 'unliquidated_quantity': remaining,
        'net_return_on_filled_cost': None if remaining or uncertain_exits else str((proceeds-exit_fees-entry_cost)/entry_cost),
        'net_return_on_hypothetical_notional': None if remaining or uncertain_exits else str((proceeds-exit_fees-entry_cost)/decimal(size)),
        'mark_to_last_print_used': False, 'actual_order_fill_claimed': False}
    if not uncertain_exits and sum(x['quantity'] for x in target_fills) == qty and qty == intended:
        label.update(state='QUALIFY', target_observation_valid=True,
            target_interval_start=(target_fills[0]['fill_ts']-timedelta(milliseconds=policy['minimum_quote_persistence_ms'])).isoformat(),
            target_interval_end=target_fills[-1]['fill_ts'].isoformat(), target_price=float(min(x['price'] for x in target_fills)),
            reason_code='FULL_SIZE_MODELLED_EXECUTABLE_TWENTY_PERCENT_OPPORTUNITY')
    elif uncertain_exits:
        label.update(state='UNRESOLVED', reason_code='UNASSESSABLE_EXIT_OPPORTUNITY_RETAINED')
    else:
        label.update(state='NONQUALIFY', reason_code='PARTIAL_PRIMARY_FILL_OR_NO_FULL_SIZE_TWENTY_PERCENT_OPPORTUNITY')
    return label


def build_execution_labels(contract, features, market_payload, first_indices):
    """Fixed selection precedes targeted capture; parse each source only once."""
    policy = contract.get('execution_policy')
    need(isinstance(features, dict), 'EXECUTION_SESSION_OBJECTS_REQUIRED')
    need(validate_policy(policy) == contract.get('execution_policy_sha256'), 'REGISTERED_EXECUTION_POLICY_CONTENT_HASH')
    need(isinstance(first_indices, list) and len(first_indices) <= 6
         and all(type(i) is int and 0 <= i < len(features['decisions']) for i in first_indices),
         'ONLY_FIRST_EMITTED_ALERTS_MAY_ENTER_EXECUTION_REPLAY')
    sessions = contract.get('official_sessions')
    need(isinstance(sessions, list) and len(sessions) == 252
         and all(isinstance(row, dict) for row in sessions), 'REGISTERED_OFFICIAL_SESSION_VECTOR')
    matches = [row for row in sessions if row.get('session_date') == features.get('session_date')]
    need(len(matches) == 1, 'EXACT_EXECUTION_MEMBER_AND_SESSION')
    official = matches[0]
    need(when(features.get('regular_open')) == when(official.get('open_at'))
         and when(features.get('regular_close')) == when(official.get('close_at')),
         'EXECUTION_SOURCE_MUST_MATCH_REGISTERED_OFFICIAL_CLOCKS')
    indices = sorted(set(first_indices))
    labels = [None]*len(features['decisions']); diagnostics = []
    shared = None
    if indices:
        need(isinstance(market_payload, dict)
             and market_payload.get('session_date') == features.get('session_date')
             and market_payload.get('security_id') == features.get('security_id'), 'EXACT_EXECUTION_MEMBER_AND_SESSION')
        need(when(market_payload.get('regular_open')) == when(official.get('open_at'))
             and when(market_payload.get('regular_close')) == when(official.get('close_at')),
             'EXECUTION_SOURCE_MUST_MATCH_REGISTERED_OFFICIAL_CLOCKS')
        shared = Market(market_payload, {'session_date': features['session_date'], 'security_id': features['security_id'],
            'decision_ts': features['decisions'][indices[0]]['decision_ts']}, contract['execution_policy_sha256'])
    elif market_payload is not None:
        # Even unneeded supplied data may not silently claim a different clock.
        need(isinstance(market_payload, dict) and market_payload.get('session_date') == features['session_date']
             and market_payload.get('security_id') == features['security_id']
             and when(market_payload.get('regular_open')) == when(official['open_at'])
             and when(market_payload.get('regular_close')) == when(official['close_at']),
             'EXECUTION_SOURCE_MUST_MATCH_REGISTERED_OFFICIAL_CLOCKS')
    for index in indices:
        row = features['decisions'][index]
        alert = {'session_date': features['session_date'], 'security_id': features['security_id'],
                 'decision_ts': row['decision_ts'], 'p_reference': row['p_reference'],
                 'execution_policy_sha256': contract['execution_policy_sha256']}
        labels[index] = _replay_alert(alert, policy, market_payload, parsed_market=shared)
        for response in policy['response_sensitivity_seconds']:
            for notional in policy['notional_test_sizes']:
                if response == 120 and notional == policy['primary_notional']: continue
                diagnostics.append(_replay_alert(alert, policy, market_payload,
                    response_seconds=response, notional=notional, parsed_market=shared))
    result = {'version': VERSION, 'execution_labels': labels, 'sensitivity_diagnostics': diagnostics,
              'primary_policy_sha256': contract['execution_policy_sha256'],
              'sensitivity_selection_permitted': False, 'secondary_hypotheses_certified': False,
              'source_payload_sha256': digest(market_payload) if market_payload is not None else None,
              'protected_outcomes_accessed': bool(indices), 'execution_source_required': bool(indices),
              'capture_scope': policy['quote_capture_scope'], 'parsed_source_passes': int(bool(indices)),
              'scenario_liquidity_ledgers_independent': True,
              'actual_orders_submitted': False, 'research_objective_achieved': False}
    result['receipt_sha256'] = digest(result)
    return result
