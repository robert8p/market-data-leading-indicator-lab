"""One coordinator: one retained native page after each completed feature batch.

Both finite clients keep their own existing checkpoints and transport bounds.
SQL owns the shared original WAL/capacity allocation and expiry. No source FETCH.
"""
from __future__ import annotations

import os
import threading
import time
from . import remediation_compact_features_v1 as compact
from . import remediation_native_listing_v1 as native


class InterleavedWorker:
    def __init__(self, base, existing, environ=None, feature=None, listing=None):
        env = os.environ if environ is None else environ
        if env.get('MARKET_DATA_REMEDIATION_NATIVE_INTERLEAVE', '').lower() != 'true':
            raise base.WorkerFault('native_interleave_not_enabled')
        if env.get('MARKET_DATA_REMEDIATION_EXECUTION_LANE') != 'compact_features':
            raise base.WorkerFault('native_interleave_requires_feature_lane')
        if env.get('MARKET_DATA_REMEDIATION_COINBASE_COHORT', '').lower() == 'true':
            raise base.WorkerFault('native_interleave_conflicts_with_source_cohort')
        self.base = base
        self.feature = feature if feature is not None else compact.FeatureWorker(base, existing, env)
        self.listing = listing if listing is not None else native.NativeWorker(base, existing, env)
        self.stop = threading.Event()
        self.max_requests = 0
        self.max_batches = compact.MAX_PROCESS_STEPS + native.MAX_PAGES
        self.native_turn = False
        self.failures = {'feature': 0, 'native': 0}

    def tick(self):
        if self.feature.terminal and self.listing.terminal:
            return 'ALL_LANES_IDLE'
        lane = 'native' if self.feature.terminal or (self.native_turn and not self.listing.terminal) else 'feature'
        worker = self.listing if lane == 'native' else self.feature
        try:
            status = worker.tick()
            self.failures[lane] = 0
        except self.base.WorkerFault as error:
            self.failures[lane] += 1
            self.base.emit('finite_interleave_rpc_failure', lane=lane, code=error.code,
                           retryable=error.retryable, attempt=self.failures[lane])
            if not error.retryable or self.failures[lane] >= 3:
                worker.terminal = 'BLOCKED'
            self.native_turn = False
            return 'RETRY_WAIT'
        except Exception as error:
            worker.terminal = 'BLOCKED'
            self.base.emit('finite_interleave_unexpected_failure', lane=lane, exception_class=type(error).__name__)
            self.native_turn = False
            return 'RETRY_WAIT'
        if lane == 'feature':
            if status in ('BATCH_COMPLETE', 'ALREADY_COMPLETE') and not self.listing.terminal:
                self.native_turn = True
            if status in compact.WAITING:
                return 'RETRY_WAIT'
        else:
            # PAGE_READY is only a read; finish its atomic commit before another feature batch.
            # Any wait returns control to feature work; a pending exact commit remains retained.
            self.native_turn = status == 'PAGE_READY'
            if status in native.WAITING:
                return 'RETRY_WAIT'
        return status

    def run(self):
        self.base.emit('finite_feature_native_interleave_started', native_pages_per_feature_batch=1,
                       feature_max_steps=compact.MAX_PROCESS_STEPS, native_max_pages=native.MAX_PAGES,
                       max_feature_rpc_calls=compact.MAX_RPC_CALLS, max_native_rpc_calls=native.MAX_CALLS,
                       external_source_requests=0, source_FETCH_enabled=False)
        last_idle = 0
        while not self.stop.is_set():
            status = self.tick()
            if status == 'ALL_LANES_IDLE':
                if time.monotonic() - last_idle > 60:
                    self.base.emit('finite_feature_native_interleave_idle', feature_status=self.feature.terminal,
                                   native_status=self.listing.terminal, source_FETCH_enabled=False)
                    last_idle = time.monotonic()
                self.stop.wait(30)
            elif status == 'RETRY_WAIT':
                self.stop.wait(2)


def create_worker(base, existing, environ=None):
    return InterleavedWorker(base, existing, environ)
