"""Synthetic quote/trade execution cases; no reserved or market data."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('execution_kernel_tests', ROOT/'app/eq20_execution_replay.py')
e = importlib.util.module_from_spec(spec); spec.loader.exec_module(e)


def policy_fixture():
    return {'version': e.POLICY_VERSION, 'execution_mode': 'HYPOTHETICAL_RESEARCH_NO_LIVE_ORDERS',
      'fill_model': 'MARKETABLE_QUOTE_WITH_SUBSEQUENT_TRADE_WITNESS',
      'commission_model': 'MAX_MINIMUM_PER_ORDER_AND_PER_SHARE_PLUS_NOTIONAL',
      'unavailable_or_stale_quote_action': 'NO_FILL_FROZEN_OBSERVABILITY_GUARD',
      'target_order_sizing_clock': 'AFTER_PREVIOUS_IOC_WITNESS_COMPLETES',
      'quote_capture_scope': 'FROZEN_FIRST_SIGNAL_TO_SAME_SESSION_CLOSE',
      'liquidation_order_type': 'MARKETABLE_LIMIT_IOC_AT_BID_MINUS_FROZEN_SLIPPAGE', 'entry_order_type': 'MARKETABLE_LIMIT_IOC', 'target_order_type': 'MARKETABLE_LIMIT_IOC',
      'exit_policy': 'FIRST_EXECUTABLE_TWENTY_PERCENT_TARGET_ELSE_FIXED_CLOCK_LIQUIDATION',
      'closing_auction_participation': False, 'last_print_as_fill': False, 'bar_touch_as_fill': False,
      'primary_response_delay_seconds': 120, 'response_sensitivity_seconds': [30,60,120,300],
      'notification_delay_seconds': 2, 'order_submission_delay_seconds': 1, 'entry_expiry_seconds': 5,
      'entry_cutoff_seconds_before_close': 600, 'liquidation_start_seconds_before_close': 300,
      'liquidation_retry_seconds': 30, 'last_order_seconds_before_close': 1,
      'maximum_quote_age_ms': 1000, 'minimum_quote_persistence_ms': 250,
      'entry_reference_protection_bps': 200, 'maximum_spread_bps': 50,
      'entry_slippage_bps': 10, 'exit_slippage_bps': 10, 'minimum_fee_per_order': 1,
      'fee_per_share': '0.01', 'fee_notional_bps': 1,
      'maximum_displayed_participation': '0.10', 'maximum_trade_participation': '0.01',
      'notional_test_sizes': [100,1000,10000], 'primary_notional': 1000,
      'partial_fill_primary_success': 'REQUIRE_FULL_INTENDED_QUANTITY',
      'unliquidated_inventory_state': 'UNRESOLVED', 'fee_on_unfilled_orders': False}


def fixture(target=True):
    p = policy_fixture(); sha = e.digest(p)
    decision = datetime(2027,1,4,14,40,35,tzinfo=timezone.utc)
    opened = decision.replace(hour=14,minute=30,second=0); closed = decision.replace(hour=21,minute=0,second=0)
    arrival = decision+timedelta(seconds=123)
    alert = {'session_date': '2027-01-04', 'security_id': 'SYNTHETIC_S1', 'decision_ts': decision.isoformat(),
             'p_reference': 10, 'execution_policy_sha256': sha}
    def quote(start,bid,ask,key):
        return {'start_at': start.isoformat(), 'end_at': (start+timedelta(seconds=5)).isoformat(),
                'available_at': start.isoformat(), 'bid': bid, 'ask': ask, 'bid_size': 2000, 'ask_size': 2000,
                'tick_size': '0.01', 'source_id': key, 'market_state': 'NORMAL'}
    def trade(at,price,key,index):
        return {'trade_ts': at.isoformat(), 'available_at': at.isoformat(), 'price': price, 'size': 10000,
                'source_id': key, 'sequence': index, 'condition_state': 'REGULAR_ELIGIBLE'}
    q = [quote(arrival,9.99,10,'ENTRY')]
    t = [trade(arrival+timedelta(milliseconds=100),10,'ENTRY_WITNESS',0)]
    if target:
        target_at = arrival+timedelta(seconds=30)
        q.append(quote(target_at,12.1,12.11,'TARGET'))
        t.append(trade(target_at+timedelta(seconds=1,milliseconds=100),12.1,'TARGET_WITNESS',1))
    liquidation = closed-timedelta(seconds=300)
    q.append(quote(liquidation,9,9.01,'LIQUIDATION'))
    t.append(trade(liquidation+timedelta(seconds=1,milliseconds=100),9,'LIQUIDATION_WITNESS',2))
    market = {'schema': 'EQ20_CERTIFIED_QUOTE_TRADE_SESSION_V1', 'session_date': alert['session_date'],
        'security_id': alert['security_id'], 'execution_policy_sha256': sha,
        'regular_open': opened.isoformat(), 'regular_close': closed.isoformat(), 'quotes': q, 'trades': t,
        'coverage': dict.fromkeys(('quotes_complete','trades_complete','timestamps_synchronized','quote_conditions_verified',
              'trade_conditions_verified','identity_verified','as_traded_prices_verified','halts_complete'), True)}
    market['coverage'].update(start_at=opened.isoformat(),end_at=closed.isoformat(),gaps=[])
    return p,alert,market


class ExecutionReplayTest(unittest.TestCase):
    def test_full_size_post_entry_executable_target_and_separate_net_costs(self):
        p,a,m = fixture(); out = e.replay_alert(a,p,m)
        self.assertEqual(out['state'],'QUALIFY'); self.assertEqual(out['fill_state'],'FILLED')
        self.assertEqual(out['n_original_alerts'],1)
        self.assertEqual(out['filled_quantity'],98)
        self.assertEqual(out['net_outcome']['state'],'MODELLED_SAME_SESSION_NET_OUTCOME')
        self.assertGreater(e.when(out['entry_order_ts']),e.when(a['decision_ts'])+timedelta(seconds=120))
        self.assertGreater(e.decimal(out['target_price']),e.decimal(out['p_entry'])*e.Decimal('1.20'))
        self.assertFalse(out['alpha_claimed']); self.assertFalse(out['research_objective_achieved'])
        gross=e.decimal(out['target_price'])/e.decimal(out['p_entry'])-1
        self.assertLess(e.decimal(out['net_outcome']['net_return_on_filled_cost']),gross)

    def test_last_print_or_bar_touch_cannot_create_fill(self):
        p,a,m=fixture(); m['trades'][0]['size']=0
        out=e.replay_alert(a,p,m)
        self.assertEqual(out['state'],'NONQUALIFY');self.assertEqual(out['fill_state'],'NO_FILL')
        self.assertEqual(out['n_original_alerts'],1)

    def test_bar_like_trade_target_without_executable_bid_cannot_qualify(self):
        p,a,m=fixture(); m['quotes'][1]['bid']=11; m['quotes'][1]['ask']=11.01
        self.assertEqual(e.replay_alert(a,p,m)['state'],'NONQUALIFY')

    def test_partial_fill_is_retained_and_not_primary_success(self):
        p,a,m=fixture();m['quotes'][0]['ask_size']=100
        out=e.replay_alert(a,p,m)
        self.assertEqual(out['fill_state'],'PARTIAL_FILL');self.assertEqual(out['filled_quantity'],10)
        self.assertEqual(out['state'],'NONQUALIFY');self.assertEqual(out['n_original_alerts'],1)

    def test_target_full_quantity_required_and_later_target_not_retroactive(self):
        p,a,m=fixture();m['quotes'][1]['bid_size']=10
        out=e.replay_alert(a,p,m)
        self.assertEqual(out['state'],'NONQUALIFY')
        self.assertEqual(out['exit_fills'][0]['quote_id'],'TARGET')
        self.assertEqual(out['exit_fills'][0]['quantity'],1)
        self.assertEqual(out['exit_fills'][1]['quote_id'],'LIQUIDATION')
        self.assertEqual(sum(x['quantity'] for x in out['exit_fills']),out['filled_quantity'])
        self.assertLess(e.decimal(out['net_outcome']['net_return_on_filled_cost']),0)

    def test_incomplete_capture_or_unknown_condition_is_unresolved_alert(self):
        for mutate in ('coverage','unknowntrade','unknownquote'):
            p,a,m=fixture()
            if mutate=='coverage':m['coverage']['trades_complete']=False
            elif mutate=='unknowntrade':m['trades'][0]['condition_state']='UNRESOLVED'
            else:m['quotes'][0]['market_state']='UNRESOLVED'
            out=e.replay_alert(a,p,m)
            self.assertEqual(out['state'],'UNRESOLVED');self.assertEqual(out['fill_state'],'UNASSESSABLE')
            self.assertEqual(out['n_original_alerts'],1)

    def test_halt_or_expired_quote_does_not_drop_alert(self):
        p,a,m=fixture();m['quotes'][0]['market_state']='HALT'
        out=e.replay_alert(a,p,m)
        self.assertEqual(out['fill_state'],'HALT_BLOCKED');self.assertEqual(out['n_original_alerts'],1)
        p,a,m=fixture();m['quotes'][0]['end_at']=(e.when(m['quotes'][0]['start_at'])+timedelta(milliseconds=50)).isoformat()
        self.assertEqual(e.replay_alert(a,p,m)['fill_state'],'NO_FILL')

    def test_fixed_primary_manual_delay_cannot_choose_favourable_earlier_quote(self):
        p,a,m=fixture();self.assertEqual(e.replay_alert(a,p,m)['state'],'QUALIFY')
        early=e.replay_alert(a,p,m,response_seconds=30)
        self.assertEqual(early['fill_state'],'NO_FILL')
        self.assertFalse(early['primary_scenario'])
        with self.assertRaises(e.Rejected):e.replay_alert(a,p,m,response_seconds=20)

    def test_unliquidated_inventory_is_not_sold_at_last_print(self):
        p,a,m=fixture(target=False);m['quotes'].pop();m['trades'].pop()
        out=e.replay_alert(a,p,m)
        self.assertEqual(out['state'],'NONQUALIFY')
        self.assertEqual(out['net_outcome']['state'],'UNRESOLVED')
        self.assertGreater(out['net_outcome']['unliquidated_quantity'],0)
        self.assertIsNone(out['net_outcome']['net_return_on_filled_cost'])
        self.assertFalse(out['net_outcome']['mark_to_last_print_used'])

    def test_price_protection_spread_costs_and_future_witnesses_enforced(self):
        for mutate in ('protection','spread','future','pretrade'):
            p,a,m=fixture()
            if mutate=='protection':m['quotes'][0].update(bid=10.49,ask=10.5)
            elif mutate=='spread':m['quotes'][0]['bid']=9
            elif mutate=='future':m['quotes'][0]['available_at']=(e.when(m['quotes'][0]['start_at'])+timedelta(seconds=1)).isoformat()
            else:m['trades'][0]['trade_ts']=m['quotes'][0]['start_at']
            out=e.replay_alert(a,p,m)
            self.assertNotEqual(out['state'],'QUALIFY');self.assertEqual(out['filled_quantity'],0)

    def test_duplicate_trades_and_overlapping_quotes_rejected(self):
        p,a,m=fixture();m['trades'].insert(1,deepcopy(m['trades'][0]))
        with self.assertRaises(e.Rejected):e.replay_alert(a,p,m)
        p,a,m=fixture();m['quotes'][1]['start_at']=m['quotes'][0]['start_at']
        with self.assertRaises(e.Rejected):e.replay_alert(a,p,m)

    def test_target_trigger_waits_for_actual_quote_availability(self):
        p,a,m=fixture(); q=m['quotes'][1]; start=e.when(q['start_at'])
        q['available_at']=(start+timedelta(milliseconds=500)).isoformat()
        # The sole trade witness at start+1.1s predates the observable trigger's
        # submission arrival at start+1.5s, so it cannot fill that later order.
        self.assertEqual(e.replay_alert(a,p,m)['state'],'NONQUALIFY')

    def test_quote_and_trade_capacity_cannot_be_reused_between_orders(self):
        p,a,m=fixture(); market=e.Market(m,a,e.digest(p)); arrival=e.when(m['quotes'][0]['start_at'])
        one=market.opportunity('BUY',arrival,98,p,limit=e.Decimal('10.2'))
        two=market.opportunity('BUY',arrival,98,p,limit=e.Decimal('10.2'))
        three=market.opportunity('BUY',arrival,98,p,limit=e.Decimal('10.2'))
        self.assertEqual([one['quantity'],two['quantity'],three['quantity']],[98,2,0])
        self.assertEqual(market.consumed_trade_capacity['ENTRY_WITNESS'],100)
        p['minimum_quote_persistence_ms']=1500;p['liquidation_retry_seconds']=1
        with self.assertRaisesRegex(e.Rejected,'RETRY_INTERVAL'):
            e.validate_policy(p)

    def test_known_stale_quote_is_explicit_frozen_policy_no_fill(self):
        p,a,m=fixture();p['maximum_quote_age_ms']=500
        a['execution_policy_sha256']=m['execution_policy_sha256']=e.digest(p)
        market=e.Market(m,a,e.digest(p));arrival=e.when(m['quotes'][1]['start_at'])+timedelta(seconds=1)
        denied=market.opportunity('SELL',arrival,1,p)
        self.assertEqual(denied['state'],'NO_FILL')
        self.assertEqual(denied['reason'],'FROZEN_QUOTE_OBSERVABILITY_OR_STALENESS_DENIAL')
        self.assertEqual(e.replay_alert(a,p,m)['state'],'NONQUALIFY')

    def test_target_order_sizing_waits_for_previous_ioc_observation(self):
        p,a,m=fixture();q=m['quotes'][1];start=e.when(q['start_at'])
        early=deepcopy(q);early.update(source_id='TARGET_EARLY',end_at=(start+timedelta(milliseconds=50)).isoformat())
        q['start_at']=q['available_at']=early['end_at']
        q['bid_size']=500
        m['quotes'].insert(1,early)
        # First order arrives in the later quote and partially executes there.
        # The later quote must not trigger a second order until +1.25seconds.
        t=deepcopy(m['trades'][1]);t.update(source_id='SECOND_WITNESS',sequence=2,
            trade_ts=(start+timedelta(seconds=2,milliseconds=350)).isoformat(),
            available_at=(start+timedelta(seconds=2,milliseconds=350)).isoformat())
        m['trades'].insert(2,t);m['trades'][-1]['sequence']=3
        out=e.replay_alert(a,p,m);history=out['target_order_history']
        self.assertGreaterEqual(len(history),2)
        self.assertGreaterEqual(e.when(history[1]['decision_at']),e.when(history[0]['outcome_observed_at']))
        self.assertEqual(history[1]['intended_quantity'],out['filled_quantity']-history[0]['filled_quantity'])

    def test_execution_source_cannot_extend_official_session(self):
        p,a,m=fixture();features={'session_date':a['session_date'],'security_id':a['security_id'],
            'regular_open':m['regular_open'],'regular_close':m['regular_close'],'decisions':[]}
        official={'session_date':a['session_date'],'open_at':m['regular_open'],'close_at':m['regular_close']}
        sessions=[official]+[dict(official,session_date='OTHER_'+str(i)) for i in range(251)]
        contract={'execution_policy':p,'execution_policy_sha256':e.digest(p),'official_sessions':sessions}
        good=e.build_execution_labels(contract,features,m,[])
        self.assertEqual(good['execution_labels'],[])
        m['regular_close']=(e.when(m['regular_close'])+timedelta(days=1)).isoformat()
        with self.assertRaisesRegex(e.Rejected,'REGISTERED_OFFICIAL_CLOCKS'):
            e.build_execution_labels(contract,features,m,[])

    def test_targeted_after_first_signal_capture_is_sufficient_for_b(self):
        p,a,m=fixture();m['coverage']['start_at']=a['decision_ts']
        self.assertEqual(e.replay_alert(a,p,m)['state'],'QUALIFY')
        m['coverage']['start_at']=(e.when(a['decision_ts'])+timedelta(seconds=1)).isoformat()
        self.assertEqual(e.replay_alert(a,p,m)['state'],'UNRESOLVED')

    def test_market_parse_is_shared_but_liquidity_is_not_borrowed_across_scenarios(self):
        p,a,m=fixture();features={'session_date':a['session_date'],'security_id':a['security_id'],
            'regular_open':m['regular_open'],'regular_close':m['regular_close'],'decisions':[a]}
        official={'session_date':a['session_date'],'open_at':m['regular_open'],'close_at':m['regular_close']}
        sessions=[official]+[dict(official,session_date='OTHER_'+str(i)) for i in range(251)]
        c={'execution_policy':p,'execution_policy_sha256':e.digest(p),'official_sessions':sessions}
        from unittest.mock import patch
        original=e.Market.__init__;calls=[]
        def counted(instance,*args,**kwargs):calls.append(1);return original(instance,*args,**kwargs)
        with patch.object(e.Market,'__init__',counted):out=e.build_execution_labels(c,features,m,[0])
        self.assertEqual(len(calls),1)
        self.assertEqual(out['execution_labels'][0],e.replay_alert(a,p,m))
        self.assertEqual(len(out['sensitivity_diagnostics']),11)
        empty=e.build_execution_labels(c,features,None,[])
        self.assertFalse(empty['execution_source_required']);self.assertEqual(empty['execution_labels'],[None])

    def test_policy_identity_or_cost_mutation_requires_new_frozen_hash(self):
        p,a,m=fixture();p['minimum_fee_per_order']=0
        with self.assertRaisesRegex(e.Rejected,'POLICY_HASH_PIN'):e.replay_alert(a,p,m)
        p,a,m=fixture();m['security_id']='ANOTHER'
        with self.assertRaisesRegex(e.Rejected,'INPUT_IDENTITY'):e.replay_alert(a,p,m)


if __name__=='__main__':unittest.main()
