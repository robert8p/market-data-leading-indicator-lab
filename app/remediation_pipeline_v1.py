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
        self.last_poll = time.monotonic()
        self.base.emit('remediation_pipeline_state', version=VERSION, stage=stage,
                       reason=result.get('reason'), full_remediation_complete=False)
        return stage

    def tick(self):
        if self.stop.is_set():
            return 'STOPPED'
        if self.stage is None or time.monotonic() - self.last_poll >= 30:
            self.refresh()
        if self.stop.is_set():
            return 'STOPPED'
        if self.stage == 'FEATURES':
            result = self.materializer.tick()
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

    def run(self):
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
