"""Sequential continuation of the exact existing remediation generations.

The database owns authorization, stop controls and one-time transitions. This
worker never decides that an idle acquisition queue means full remediation is
complete. Provider rate limits and serial database commits remain unchanged.
"""
import os
import threading
import time
import uuid
from . import remediation_feature_native_lane_v1 as interleave
from . import remediation_coinbase_cohort_v1 as cohort
from . import remediation_expedite_v2 as expedite

VERSION = 'remediation_sequential_handoff_20261008_v1'
BINANCE_GENERATION = 'binance_um_daily_remaining_17533_20261008_v1'
BINANCE_MANIFEST = '2825d3786055e65d9aa27e4947b836ce5a9b43cfcbb3ab35f20a23b874b5196c'


class PipelineWorker:
    def __init__(self, base, rpc, environ=None, materializer=None):
        self.base, self.rpc = base, rpc
        self.env = dict(os.environ if environ is None else environ)
        self.materializer = materializer or interleave.create_worker(base, rpc, self.env)
        self.stop = threading.Event()
        self.materializer.stop = self.stop
        self.worker_id = 'pipeline-' + str(uuid.uuid4())
        self.stage = None
        self.source = None
        self.cohort = None
        self.last_poll = 0
        self.noncrypto_enabled = False
        self.noncrypto_turn = True
        self.overlap_enabled = False
        self.source_thread = None
        self.expedite_thread = None
        self.benchmark_enabled = False
        self.validation_enabled = False
        self.noncrypto_source = None
        self.source_rpc = None
        self.source_lock = threading.Lock()
        self.max_requests = None
        self.max_batches = None

    def refresh(self):
        if self.stop.is_set():
            return 'STOPPED'
        result = self.rpc.call('pipeline', {'run_id': self.base.RUN_ID,
            'version': VERSION, 'worker_id': self.worker_id})
        if result.get('run_id') != self.base.RUN_ID or result.get('version') != VERSION:
            raise self.base.WorkerFault('pipeline_response_scope_mismatch')
        stage = result.get('status')
        if stage not in {'FEATURES', 'BINANCE', 'COINBASE', 'PAUSED', 'BUSY', 'ACQUISITION_DRAINED'}:
            raise self.base.WorkerFault('pipeline_response_stage_invalid')
        if stage == 'BINANCE' and (result.get('generation_id') != BINANCE_GENERATION
                or result.get('manifest_sha256') != BINANCE_MANIFEST):
            raise self.base.WorkerFault('pipeline_binance_scope_mismatch')
        if stage == 'COINBASE' and (result.get('generation_id') != cohort.FULL_GENERATION_ID
                or result.get('manifest_sha256') != cohort.FULL_MANIFEST_SHA256):
            raise self.base.WorkerFault('pipeline_coinbase_scope_mismatch')
        self.stage = stage
        self.noncrypto_enabled = result.get("noncrypto_enabled") is True
        self.overlap_enabled = self.noncrypto_enabled and result.get("noncrypto_overlap_enabled") is True
        self.benchmark_enabled = result.get("benchmark_enabled") is True
        self.validation_enabled = result.get("validation_enabled") is True
        self.last_poll = time.monotonic()
        self.base.emit('remediation_pipeline_state', version=VERSION, stage=stage,
                       reason=result.get('reason'), full_remediation_complete=False)
        return stage

    def noncrypto_tick(self):
        if not self.source_lock.acquire(blocking=False):
            return 'WAIT'
        try:
            return self._noncrypto_tick()
        finally:
            self.source_lock.release()

    def _noncrypto_tick(self):
        if self.noncrypto_source is None:
            self.noncrypto_source = self.base.Worker(self.source_rpc or self.rpc, self.env)
            self.noncrypto_source.stop = self.stop
        claim = (self.source_rpc or self.rpc).call('noncrypto_claim', {'run_id': self.base.RUN_ID,
            'worker_id': self.noncrypto_source.worker_id,
            'version': 'noncrypto_equity_handoff_20261009_v1',
            'source_capabilities': self.base.SOURCE_CAPABILITIES})
        if claim.get('status') != 'claimed':
            self.base.emit('noncrypto_queue_state', status=claim.get('status'), reason=claim.get('reason'),
                           full_remediation_complete=False)
            return 'WAIT'
        batch = claim.get('batch', {})
        req = batch.get('request_json', {})
        warmup = (batch.get('source_type') == 'massive_reference_tickers'
                  and req.get('required_parser_version') == 'massive_native_warmup_reference_page_20261008_v1'
                  and req.get('warmup_contract') == 'RTH_DAILY_LOOKBACK61_PREWINDOW_2025-06-04_2025-08-29_V1')
        prices = (batch.get('source_type') == 'massive_candles'
                  and req.get('noncrypto_scope') == 'LISTED_EQUITY_GAPS_20261009_V1'
                  and req.get('asset_class') == 'stocks'
                  and ':' not in batch.get('symbol', ':'))
        if (claim.get('version') != 'noncrypto_equity_handoff_20261009_v1'
                or batch.get('run_id') != self.base.RUN_ID or batch.get('provider') != 'massive'
                or not (warmup or prices)):
            raise self.base.WorkerFault('noncrypto_claim_scope_mismatch')
        if self.stop.is_set():
            return 'STOPPED'
        self.noncrypto_source.batch_count += 1
        self.noncrypto_source.process(batch)
        return 'SOURCE_BATCH_PROCESSED'

    def tick(self):
        if self.stop.is_set():
            return 'STOPPED'
        if self.stage is None or time.monotonic() - self.last_poll >= 30:
            self.refresh()
        if self.stop.is_set():
            return 'STOPPED'
        if self.noncrypto_enabled and not self.overlap_enabled and (self.noncrypto_turn or self.stage != 'FEATURES'):
            self.noncrypto_turn = False
            result = self.noncrypto_tick()
            if result != 'WAIT' or self.stage != 'FEATURES':
                return result
        if self.stage == 'FEATURES':
            result = self.materializer.tick()
            if result in {'BATCH_COMPLETE', 'ALREADY_COMPLETE', 'ALL_LANES_IDLE'}:
                self.noncrypto_turn = True
            if result == 'ALL_LANES_IDLE':
                self.stage = None
                return 'WAIT'
            return result
        if self.stage == 'BINANCE':
            if self.source is None:
                self.source = self.base.Worker(self.rpc, self.env)
                self.source.stop = self.stop
            claim = self.rpc.call('claim', {'run_id': self.base.RUN_ID,
                'worker_id': self.source.worker_id,
                'source_capabilities': self.base.SOURCE_CAPABILITIES})
            if claim.get('status') != 'claimed':
                self.stage = None
                return 'WAIT'
            batch = claim.get('batch', {})
            if (claim.get('generation_id') != BINANCE_GENERATION
                    or claim.get('manifest_sha256') != BINANCE_MANIFEST
                    or batch.get('provider') != 'binance_archive'
                    or batch.get('request_json', {}).get('source_generation_id') != BINANCE_GENERATION):
                raise self.base.WorkerFault('pipeline_claim_scope_mismatch')
            self.source.batch_count += 1
            self.source.process(batch)
            return 'SOURCE_BATCH_PROCESSED'
        if self.stage == 'COINBASE':
            if self.cohort is None:
                env = {**self.env,
                    'MARKET_DATA_REMEDIATION_EXECUTION_LANE': 'source',
                    'MARKET_DATA_REMEDIATION_COINBASE_COHORT': 'true',
                    'MARKET_DATA_REMEDIATION_FOMC_PROBE_VERSION': '',
                    'MARKET_DATA_REMEDIATION_COHORT_GENERATION': cohort.FULL_GENERATION_ID,
                    'MARKET_DATA_REMEDIATION_COHORT_MANIFEST_SHA256': cohort.FULL_MANIFEST_SHA256,
                    'MARKET_DATA_REMEDIATION_COHORT_BUDGET_VERSION': cohort.MANIFEST_BUDGET_VERSION,
                    'MARKET_DATA_REMEDIATION_COHORT_CONCURRENCY': '8'}
                self.cohort = cohort.create_worker(self.base, self.rpc, env)
                self.cohort.stop = self.stop
                self.cohort.rate.stop = self.stop
            # Existing cohort gates enforce every subsequent claim and resume.
            self.cohort.run()
            return 'STOPPED'
        return 'WAIT'

    def acquisition_loop(self):
        # Separate transport and serial provider loop: no shared opener/DB lock.
        # Each claim/heartbeat/commit independently rechecks the database stop gate.
        while not self.stop.is_set():
            if not self.overlap_enabled:
                self.stop.wait(1)
                continue
            try:
                result = self.noncrypto_tick()
                if result == 'WAIT':
                    self.stop.wait(2)
            except self.base.WorkerFault as error:
                self.base.emit('noncrypto_acquisition_waiting', code=error.code,
                               retryable=error.retryable)
                self.stop.wait(5 if error.retryable else 30)
            except Exception as error:
                self.base.emit('noncrypto_acquisition_unexpected_failure',
                               exception_class=type(error).__name__)
                self.stop.wait(30)

    def run(self):
        self.source_rpc = self.base.RpcClient(self.env)
        self.source_rpc.budgets_removed = self.rpc.budgets_removed
        self.source_thread = threading.Thread(target=self.acquisition_loop,
            name='noncrypto-acquisition', daemon=True)
        self.source_thread.start()
        maintenance = expedite.ExpediteWorker(self.base, self.env, self.stop)
        self.expedite_thread = threading.Thread(target=maintenance.run,
            args=(lambda: (self.benchmark_enabled, self.validation_enabled),),
            name='remediation-validation', daemon=True)
        self.expedite_thread.start()
        self.base.emit('noncrypto_independent_acquisition_loop_started',
                       source_concurrency=1, feature_concurrency=1)
        try:
            self.run_foreground()
        finally:
            self.stop.set()
            self.source_thread.join(timeout=40)
            self.expedite_thread.join(timeout=50)

    def run_foreground(self):
        while not self.stop.is_set():
            try:
                result = self.tick()
                if result == 'RETRY_WAIT':
                    self.stop.wait(2)
                elif result in {'WAIT', 'STOPPED'}:
                    self.stop.wait(30)
            except self.base.WorkerFault as error:
                self.base.emit('remediation_pipeline_waiting', code=error.code,
                               retryable=error.retryable)
                self.stage = None
                self.stop.wait(30)


def create_worker(base, rpc, environ=None):
    return PipelineWorker(base, rpc, environ)

