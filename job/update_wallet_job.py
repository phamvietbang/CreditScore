import random
import time

from base.executors.batch_work_executor import BatchWorkExecutor
from base.jobs.base_job import BaseJob
from config import get_logger

logger = get_logger('Update Wallet Job')


class UpdateWalletJob(BaseJob):
    def __init__(self, batch_size, max_workers, graph, wallets_batch, multichain=True):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)

        self._graph = graph
        self.wallets_batch = wallets_batch
        self.multichain = multichain

    def _start(self):
        pass

    def _end(self):
        self.batch_work_executor.shutdown()

    def _export(self):
        self.batch_work_executor.execute(
            self.wallets_batch,
            self._export_batch,
            total_items=len(self.wallets_batch)
        )

    def _export_batch(self, wallets_batch):
        for batch in wallets_batch:
            time.sleep(random.random())
            start_time = time.time()
            if self.multichain:
                self._graph.update_multichain_wallets(batch)
            else:
                self._graph.update_wallets(batch)
            logger.info(f'Flag {len(batch)} wallets takes {time.time() - start_time} seconds')
