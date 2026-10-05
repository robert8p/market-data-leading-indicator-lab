"""Outcome-blind single-decision production and resumable first-alert state.

The existing bounded collector supplies authenticated release and committed raw
prefix readbacks, then atomically persists the returned state before requesting
selected-alert execution evidence. This module has no network, fit or label API.
It neither certifies source truth nor authorizes work, capacity or evidence use.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys
import types
from zoneinfo import ZoneInfo

VERSION = 'EQ20_PROSPECTIVE_INCREMENTAL_SOURCE_V1'
PRIVATE_VERSION = 'EQ20_PROSPECTIVE_PRIVATE_INCREMENTAL_V1'
PRIVATE_ENTRYPOINT = 'eq20_prospective_incremental_v1.py'
BASE_ENTRYPOINT = 'eq20_prospective_arithmetic_v1.py'
BASE_ARITHMETIC_SHA256 = '5c210f1094bc2a002d2737cb786d484ca6d898ec5dced2db1efc3cbec8dbdeca'
SOURCE_MODULE_SHA256 = '675e12c6f5ff492c2d99489dc44a117423d9df85862f2d9261d5a44593013ab4'
KERNEL_SHA256 = '99077b64970ff02e6a6fcc9dc9b9da4f9411eef08bc396c51cf220f35f481987'
MAX_PREFIX_BYTES = 8*1024*1024
MAX_PREFIX_ROWS = 20000
RAW_KEYS = {'schema', 'session_contract_sha256', 'session_date', 'security_id', 'issuer_id',
    'instrument_key', 'issuer_cik', 'population_day_sha256', 'regular_open', 'regular_close',
    'identity_disposition', 'source_family_dispositions', 'source_capture', 'source_inputs'}
ROW_FAMILIES = ('facts', 'events', 'bars', 'short_interest_states', 'short_volume_states')
_HELPERS = {}


class Closed(ValueError):
    pass


def require(value, reason):
    if not value:
        raise Closed(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def digest(value):
    return sha(canonical(value))


def helper(name, pin):
    path = Path(__file__).with_name(name+'.py')
    require(sha(path.read_bytes()) == pin, 'INCREMENTAL_INSTALLED_COMPONENT_PIN')
    if name not in _HELPERS:
        spec = importlib.util.spec_from_file_location('incremental_'+name, path)
        obj = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = obj
        spec.loader.exec_module(obj)
        _HELPERS[name] = obj
    return _HELPERS[name]


def resolve(binding, kind, status):
    require(isinstance(binding, dict), 'INCREMENTAL_ACTUAL_REGISTRY_BINDING_REQUIRED')
    ref, row = binding.get('reference'), binding.get('artifact')
    require(isinstance(ref, dict) and isinstance(row, dict)
            and ref.get('kind') == kind and ref.get('status') == status
            and isinstance(ref.get('artifact_key'), str) and 0 < len(ref['artifact_key']) <= 256
            and re.fullmatch('[0-9a-f]{64}', ref.get('implementation_sha256', '')) is not None
            and all(ref.get(k) == row.get(k) for k in ('kind', 'status', 'artifact_key', 'implementation_sha256')),
            'INCREMENTAL_REGISTRY_IDENTITY')
    raw = row.get('evidence_text')
    require(isinstance(raw, str) and len(raw.encode()) <= 1024*1024
            and sha(raw.encode()) == ref['implementation_sha256'], 'INCREMENTAL_REGISTRY_READBACK_HASH')
    result = json.loads(raw)
    require(isinstance(result, dict), 'INCREMENTAL_REGISTRY_OBJECT')
    return result


class Prepared:
    def __init__(self, contract, source_binding, incremental_binding):
        self.kernel = helper('eq20_prospective_kernel', KERNEL_SHA256)
        self.source = helper('eq20_prospective_features', SOURCE_MODULE_SHA256)
        self.contract = deepcopy(contract)
        self.rules, self.union_ids, self.feature_names = self.kernel.family(self.contract)
        self.scope = self.kernel.scope(self.contract)
        release = resolve(incremental_binding, 'SUCCESSOR_INCREMENTAL_SOURCE_PRODUCER_RELEASE', 'VERIFIED_EXECUTABLE_RELEASE')
        require(release.get('version') == VERSION and release.get('private_version') == PRIVATE_VERSION
                and release.get('entrypoint') == PRIVATE_ENTRYPOINT
                and release.get('module_sha256') == sha(Path(__file__).read_bytes())
                and release.get('source_module_sha256') == SOURCE_MODULE_SHA256
                and release.get('kernel_sha256') == KERNEL_SHA256
                and release.get('base_arithmetic_sha256') == BASE_ARITHMETIC_SHA256
                and all(release.get(k) == v for k, v in self.scope.items()), 'INCREMENTAL_RELEASE_FULL_SCOPE_AND_PINS')
        require(release.get('source_producer_reference') == source_binding.get('reference')
                and release.get('source_truth_verified_by_this_module') is False
                and release.get('feature_only') is True and release.get('thresholds_refitted') is False
                and release.get('additional_paid_cost') == 0, 'INCREMENTAL_CAPABILITY_AND_BASE_RELEASE_BOUNDARY')
        require(self.kernel.stamp(incremental_binding['artifact'].get('created_at'))
                < self.kernel.stamp(self.contract['official_sessions'][0]['open_at']), 'INCREMENTAL_RELEASE_PRECEDES_RESERVED_START')
        private_binding = incremental_binding.get('private_code_binding')
        code = resolve(private_binding, 'EQ20_PROSPECTIVE_INCREMENTAL_SOURCE_CODE', 'VERIFIED')
        text = code.get('content_utf8')
        require(release.get('private_code_reference') == private_binding['reference']
                and code.get('name') == PRIVATE_ENTRYPOINT and isinstance(text, str)
                and len(text.encode()) <= 131072 and sha(text.encode()) == release.get('private_module_sha256')
                and code.get('content_sha256') == release.get('private_module_sha256'), 'INCREMENTAL_PRIVATE_CODE_BYTES_PIN')
        require(self.kernel.stamp(private_binding['artifact'].get('created_at'))
                < self.kernel.stamp(self.contract['official_sessions'][0]['open_at']), 'INCREMENTAL_PRIVATE_CODE_PRECEDES_RESERVED_START')
        base_text = source_binding.get('source_files', {}).get(BASE_ENTRYPOINT)
        require(isinstance(base_text, str) and sha(base_text.encode()) == BASE_ARITHMETIC_SHA256,
                'INCREMENTAL_UNCHANGED_BASE_ARITHMETIC_BYTES')
        base, base_release = self.source.load_producer(self.contract, source_binding)
        module = types.ModuleType('eq20_incremental_private_'+release['private_module_sha256'])
        module.__file__ = '<registered:'+PRIVATE_ENTRYPOINT+'>'
        sys.modules[module.__name__] = module
        exec(compile(text, module.__file__, 'exec'), module.__dict__)
        require(getattr(module, 'VERSION', None) == PRIVATE_VERSION
                and getattr(module, 'BASE_ARITHMETIC_SHA256', None) == BASE_ARITHMETIC_SHA256
                and callable(getattr(module, 'DecisionProducer', None)), 'INCREMENTAL_REGISTERED_PRIVATE_API')
        self.producer = module.DecisionProducer(base, base_text)
        self.grids = {d: tuple(v) for d, v in base.grids.items()}
        self.indices = {d: {at: i for i, at in enumerate(v)} for d, v in self.grids.items()}
        self.clocks = dict(base.clocks)
        for day, (opened, closed) in self.clocks.items():
            local_open = opened.astimezone(ZoneInfo('America/New_York'))
            require(local_open.date().isoformat() == day
                    and (local_open.hour, local_open.minute, local_open.second) == (9, 30, 0)
                    and timedelta(minutes=70) <= closed-opened <= timedelta(minutes=390),
                    'INCREMENTAL_ORIGINAL_OFFICIAL_SESSION_CLOCK')
        self.base = base
        self.base_source_manifest_sha256 = digest([{k: item[k] for k in ('name', 'sha256')} for item in base_release['code_files']])
        self.release_sha256 = incremental_binding['reference']['implementation_sha256']
        self.source_release_sha256 = source_binding['reference']['implementation_sha256']
        self.private_module_sha256 = release['private_module_sha256']

    def _prefix(self, raw, decision_ts, readback):
        require(isinstance(raw, dict) and set(raw) == RAW_KEYS
                and raw.get('schema') == 'EQ20_PROSPECTIVE_RAW_SECURITY_SESSION_V1', 'INCREMENTAL_EXACT_SOURCE_ONLY_ENVELOPE')
        day, security = raw.get('session_date'), raw.get('security_id')
        when = self.kernel.stamp(decision_ts)
        require(day in self.indices and when in self.indices[day]
                and isinstance(security, str) and 0 < len(security) <= 256
                and raw.get('session_contract_sha256') == self.scope['session_contract_sha256'], 'INCREMENTAL_SCOPE_AND_OFFICIAL_DECISION')
        require(self.kernel.stamp(raw.get('regular_open')) == self.clocks[day][0]
                and self.kernel.stamp(raw.get('regular_close')) == self.clocks[day][1], 'INCREMENTAL_EXACT_OFFICIAL_CLOCKS')
        self.kernel.hash_value(raw.get('population_day_sha256'))
        source = raw.get('source_inputs')
        require(isinstance(source, dict) and set(source) == set(ROW_FAMILIES)
                and all(isinstance(source[k], list) and all(isinstance(v, dict) for v in source[k]) for k in ROW_FAMILIES)
                and sum(len(source[k]) for k in ROW_FAMILIES) <= MAX_PREFIX_ROWS, 'INCREMENTAL_BOUNDED_COMPLETE_SOURCE_PREFIX')
        encoded = canonical(raw)
        require(len(encoded) <= MAX_PREFIX_BYTES, 'INCREMENTAL_PREFIX_BYTE_CEILING')
        require(isinstance(readback, dict) and set(readback) == {'receipt_evidence_text', 'receipt_sha256'},
                'INCREMENTAL_COMMITTED_PREFIX_READBACK_REQUIRED')
        text = readback.get('receipt_evidence_text')
        require(isinstance(text, str) and len(text.encode()) <= 65536
                and sha(text.encode()) == readback.get('receipt_sha256'), 'INCREMENTAL_PREFIX_RECEIPT_HASH')
        receipt = json.loads(text)
        require(isinstance(receipt, dict) and receipt.get('version') == 'EQ20_PROSPECTIVE_ASOF_PREFIX_RECEIPT_V1'
                and all(receipt.get(k) == v for k, v in self.scope.items())
                and receipt.get('source_producer_release_sha256') == self.source_release_sha256
                and receipt.get('incremental_release_sha256') == self.release_sha256
                and receipt.get('session_date') == day and receipt.get('security_id') == security
                and receipt.get('population_day_sha256') == raw['population_day_sha256']
                and receipt.get('decision_index') == self.indices[day][when]
                and self.kernel.stamp(receipt.get('as_of_at')) == when
                and receipt.get('raw_prefix_sha256') == sha(encoded)
                and isinstance(receipt.get('prefix_key'), str) and 0 < len(receipt['prefix_key']) <= 256
                and type(receipt.get('source_sequence')) is int and receipt['source_sequence'] >= 0
                and receipt.get('complete_eligible_source_prefix') is True
                and receipt.get('first_receipt_and_revision_identity_verified') is True
                and receipt.get('upstream_source_manifest_sha256') == raw.get('source_capture', {}).get('source_manifest_sha256'),
                'INCREMENTAL_ACTUAL_PREFIX_SCOPE_AND_CAUSAL_RECEIPT')
        identity = raw.get('identity_disposition') == 'IDENTITY_ADMITTED'
        issuer = identity and self.base.projector.normalize_cik(raw.get('issuer_cik')) is not None
        dispositions = raw.get('source_family_dispositions', {})
        admitted = {'bars': identity and dispositions.get('MARKET') == 'ADMITTED',
            'events': issuer and dispositions.get('SEC') == 'ADMITTED',
            'facts': issuer and any(dispositions.get(k) == 'ADMITTED' for k in ('FUNDAMENTAL', 'REPORTED_EPS')),
            'short_interest_states': identity and dispositions.get('SHORT_INTEREST') == 'ADMITTED',
            'short_volume_states': identity and dispositions.get('SHORT_VOLUME') == 'ADMITTED'}
        for family, values in source.items():
            if not admitted[family]:
                continue
            for row in values:
                received = self.kernel.stamp(row.get('first_received_at'))
                require(received <= when and isinstance(row.get('revision_id'), str)
                        and 0 < len(row['revision_id']) <= 256, 'INCREMENTAL_REAL_FIRST_RECEIPT_AND_REVISION_REQUIRED')
                if row.get('available_at') is not None:
                    require(self.kernel.stamp(row['available_at']) >= received, 'INCREMENTAL_AVAILABILITY_CANNOT_PRECEDE_RECEIPT')
                if row.get('source_sequence') is not None:
                    require(type(row['source_sequence']) is int and 0 <= row['source_sequence'] <= receipt['source_sequence'],
                            'INCREMENTAL_ROW_RECEIPT_SEQUENCE')
        return when, self.indices[day][when], receipt, readback['receipt_sha256']

    def produce_decision(self, raw_asof, decision_ts, prefix_readback):
        when, index, prefix, receipt_sha = self._prefix(raw_asof, decision_ts, prefix_readback)
        result = self.producer.produce(raw_asof, index, when)
        features, provenance = result.get('features'), result.get('provenance')
        require(isinstance(features, dict) and isinstance(provenance, dict)
                and features.get('session_date') == raw_asof['session_date']
                and features.get('security_id') == raw_asof['security_id']
                and isinstance(features.get('decisions'), list) and len(features['decisions']) == 1
                and provenance.get('session_contract_sha256') == self.scope['session_contract_sha256']
                and provenance.get('feature_names') == self.feature_names
                and provenance.get('original_source_files_sha256') == self.base_source_manifest_sha256
                and provenance.get('full_session_grid_computed') is False, 'INCREMENTAL_ACTUAL_PRODUCER_SCOPE')
        row = features['decisions'][0]
        self._row_truth(row, when)
        output = {'version': VERSION, **self.scope, 'session_date': raw_asof['session_date'],
            'security_id': raw_asof['security_id'], 'population_day_sha256': raw_asof['population_day_sha256'],
            'decision_index': index, 'decision_ts': when.isoformat(), 'decision_row': row,
            'decision_row_sha256': digest(row), 'source_sequence': prefix['source_sequence'],
            'raw_prefix_sha256': prefix['raw_prefix_sha256'], 'prefix_receipt_sha256': receipt_sha,
            'prefix_key': prefix['prefix_key'], 'producer_release_sha256': self.source_release_sha256,
            'incremental_release_sha256': self.release_sha256, 'private_module_sha256': self.private_module_sha256,
            'provenance': provenance, 'feature_only': True, 'outcome_label_arrays_opened': False,
            'thresholds_refitted': False, 'source_truth_verified_by_this_module': False,
            'verification_scope': 'EXACT_SINGLE_DECISION_ARITHMETIC_ON_AUTHENTICATED_SOURCE_PREFIX'}
        return dict(output, receipt_sha256=digest(output))

    def _row_truth(self, row, when):
        k = self.kernel
        require(isinstance(row, dict) and k.stamp(row.get('decision_ts')) == when, 'INCREMENTAL_EXACT_DECISION_ROW')
        require(row.get('reference_state') in k.REFERENCE_STATES, 'INCREMENTAL_REFERENCE_STATE')
        reference_ok = row['reference_state'] == 'AVAILABLE'
        if reference_ok:
            k.finite(row.get('p_reference'), positive=True)
            ended, available = k.stamp(row.get('reference_bar_end')), k.stamp(row.get('reference_available_at'))
            require(ended <= available <= when and (when-ended).total_seconds() <= self.contract['maximum_reference_staleness_seconds'],
                    'INCREMENTAL_REFERENCE_COMPLETION_AVAILABILITY_STALENESS')
        else:
            require(row.get('p_reference') is None, 'INCREMENTAL_UNKNOWN_REFERENCE_NOT_NUMERIC')
        cells = row.get('features')
        require(isinstance(cells, dict) and set(cells) == set(self.feature_names), 'INCREMENTAL_COMPLETE_FEATURE_CELLS')
        for value in cells.values():
            require(isinstance(value, dict) and set(value) == {'state', 'value', 'available_at'}
                    and value['state'] in k.VALUE_STATES, 'INCREMENTAL_CELL_SCHEMA')
            if value['state'] in k.KNOWN:
                k.finite(value['value'])
                require(k.stamp(value['available_at']) <= when, 'INCREMENTAL_POST_DECISION_FEATURE')
                require(value['state'] != 'OBSERVED_ZERO' or value['value'] == 0, 'INCREMENTAL_OBSERVED_ZERO_VALUE')
            else:
                require(value['value'] is None, 'INCREMENTAL_UNKNOWN_FEATURE_NOT_NUMERIC')
        truth = {}
        for rule in self.rules:
            states = []
            for gate in rule['gates']:
                value = cells[gate['feature']]
                if gate['threshold'] is None or value['state'] not in k.KNOWN:
                    states.append(None)
                else:
                    result = value['value'] >= gate['threshold'] if gate['operator'] == 'ge' else value['value'] <= gate['threshold']
                    states.append(not result if gate['negate'] else result)
            truth[rule['rule_id']] = False if False in states else None if None in states or not reference_ok else True
        return truth

    def advance_first_signals(self, previous_state, decision_receipt):
        require(isinstance(decision_receipt, dict), 'INCREMENTAL_DECISION_RECEIPT_REQUIRED')
        receipt = {k: v for k, v in decision_receipt.items() if k != 'receipt_sha256'}
        require(digest(receipt) == decision_receipt.get('receipt_sha256')
                and receipt.get('version') == VERSION and all(receipt.get(k) == v for k, v in self.scope.items())
                and receipt.get('producer_release_sha256') == self.source_release_sha256
                and receipt.get('incremental_release_sha256') == self.release_sha256
                and receipt.get('private_module_sha256') == self.private_module_sha256
                and receipt.get('feature_only') is True and receipt.get('outcome_label_arrays_opened') is False,
                'INCREMENTAL_DECISION_SCOPE_AND_CONTENT_CHAIN')
        day, security, index = receipt.get('session_date'), receipt.get('security_id'), receipt.get('decision_index')
        require(day in self.grids and type(index) is int and 0 <= index < len(self.grids[day])
                and isinstance(security, str) and security, 'INCREMENTAL_DECISION_IDENTITY')
        when = self.grids[day][index]
        require(self.kernel.stamp(receipt.get('decision_ts')) == when
                and digest(receipt.get('decision_row')) == receipt.get('decision_row_sha256'), 'INCREMENTAL_DECISION_ROW_HASH')
        for key in ('raw_prefix_sha256', 'prefix_receipt_sha256', 'population_day_sha256'):
            self.kernel.hash_value(receipt.get(key))
        require(type(receipt.get('source_sequence')) is int and receipt['source_sequence'] >= 0, 'INCREMENTAL_PREFIX_SEQUENCE')
        truth = self._row_truth(receipt['decision_row'], when)
        scope = {**self.scope, 'session_date': day, 'security_id': security,
            'population_day_sha256': receipt['population_day_sha256'], 'incremental_release_sha256': self.release_sha256,
            'producer_release_sha256': self.source_release_sha256}
        rule_ids = [r['rule_id'] for r in self.rules]
        if previous_state is None:
            require(index == 0, 'INCREMENTAL_FIRST_DECISION_MUST_START_GRID')
            state = {'version': 'EQ20_PROSPECTIVE_FIRST_SIGNAL_STATE_V1', **scope,
                'next_decision_index': 0, 'last_source_sequence': -1,
                'feature_prefix_chain_sha256': digest([VERSION, scope]),
                'first_indices': dict.fromkeys(rule_ids), 'unknown_gate_decisions': dict.fromkeys(rule_ids, 0),
                'union_first_index': None, 'union_attribution': None, 'first_signals': {}}
            prior_sha = None
        else:
            require(isinstance(previous_state, dict), 'INCREMENTAL_PRIOR_STATE_OBJECT')
            state = {k: deepcopy(v) for k, v in previous_state.items() if k != 'state_sha256'}
            prior_sha = previous_state.get('state_sha256')
            require(digest(state) == prior_sha and state.get('version') == 'EQ20_PROSPECTIVE_FIRST_SIGNAL_STATE_V1'
                    and all(state.get(k) == v for k, v in scope.items())
                    and state.get('expected_decisions') == len(self.grids[day])
                    and state.get('next_decision_index') == index
                    and set(state.get('first_indices', {})) == set(rule_ids)
                    and set(state.get('unknown_gate_decisions', {})) == set(rule_ids), 'INCREMENTAL_COMMITTED_PRIOR_STATE_SCOPE_AND_CURSOR')
        require(type(state['last_source_sequence']) is int and state['last_source_sequence'] <= receipt['source_sequence'],
                'INCREMENTAL_SOURCE_HISTORY_CANNOT_MOVE_BACKWARD')
        require(type(state['next_decision_index']) is int and state['next_decision_index'] == index,
                'INCREMENTAL_EXACT_SEQUENTIAL_CURSOR')
        new = {}
        for rule_id in rule_ids:
            old = state['first_indices'][rule_id]
            require(old is None or type(old) is int and 0 <= old < index, 'INCREMENTAL_PRIOR_FIRST_SIGNAL_INDEX')
            count = state['unknown_gate_decisions'][rule_id]
            require(type(count) is int and 0 <= count <= index, 'INCREMENTAL_PRIOR_UNKNOWN_COUNT')
            if truth[rule_id] is None:
                state['unknown_gate_decisions'][rule_id] += 1
            elif truth[rule_id] and old is None:
                state['first_indices'][rule_id] = index
                new[rule_id] = {'decision_index': index, 'decision_ts': when.isoformat(),
                    'row_sha256': receipt['decision_row_sha256'], 'raw_prefix_sha256': receipt['raw_prefix_sha256'],
                    'prefix_receipt_sha256': receipt['prefix_receipt_sha256'], 'decision_receipt_sha256': decision_receipt['receipt_sha256']}
        members = sorted(rule_id for rule_id in self.union_ids if truth[rule_id] is True)
        if state['union_first_index'] is None and members:
            state['union_first_index'], state['union_attribution'] = index, members[0]
            new['__UNION__'] = dict(new.get(members[0]) or state['first_signals'][members[0]], attribution_rule_id=members[0])
        state['first_signals'].update(new)
        state.update(next_decision_index=index+1, expected_decisions=len(self.grids[day]),
            last_source_sequence=receipt['source_sequence'], last_raw_prefix_sha256=receipt['raw_prefix_sha256'],
            last_decision_receipt_sha256=decision_receipt['receipt_sha256'], previous_state_sha256=prior_sha,
            feature_prefix_chain_sha256=digest([state['feature_prefix_chain_sha256'], decision_receipt['receipt_sha256'],
                receipt['raw_prefix_sha256'], receipt['source_sequence'], index]),
            new_first_signals=new, complete=index+1 == len(self.grids[day]), thresholds_refitted=False,
            outcome_label_arrays_opened=False, research_objective_achieved=False,
            persistence_requirement='AUTHORITATIVE_PRIOR_STATE_SHA_AND_CURSOR_COMPARE_AND_SWAP_BEFORE_EXECUTION_CAPTURE')
        state['selection_sha256'] = digest([day, security, state['first_indices'], state['union_first_index'],
            state['union_attribution'], self.contract['candidate_family_sha256']])
        return dict(state, state_sha256=digest(state))


def prepare_incremental(contract, source_producer_bindings, incremental_release_binding):
    """Authenticate metadata once per bounded collector batch, before raw reads."""
    return Prepared(contract, source_producer_bindings, incremental_release_binding)


def produce_decision(contract, raw_asof, decision_ts, bindings):
    prepared = prepare_incremental(contract, bindings['source_producer'], bindings['incremental_release'])
    return prepared.produce_decision(raw_asof, decision_ts, bindings['prefix_readback'])
