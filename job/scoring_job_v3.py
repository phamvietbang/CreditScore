import json
import time

from base.executors.batch_work_executor import BatchWorkExecutor
from base.jobs.base_job import BaseJob
from calculate.services.scores_service import update_scores_properties
from calculate.services.wallet_score_v3 import calculate_credit_score
from calculate.wallet_credit_score import WalletsCreditScore
from config import get_logger
from database.cs_mongodb_klg import MongoDB

logger = get_logger('Wallet Score Job')


class WalletScoresJob(BaseJob):
    def __init__(self,
                 batch_size,
                 max_workers,
                 graph,
                 wallets_batch,
                 statistics,
                 tokens,
                 k,
                 h,
                 multichain=True,
                 chain_id=None,
                 n_cpu=1,
                 cpu=1,
                 wallet_batch_size=10000,
                 start_time=1719792000,
                 end_time=1727740800
                 ):
        self.end_time = end_time
        self.start_time = start_time
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)

        self.n_cpu = n_cpu
        self.cpu = cpu - 1

        self._graph = graph
        self.multichain = multichain
        self.chain_id = chain_id

        self.number_of_wallets_batch = wallets_batch
        self.statistics = statistics
        self.tokens = tokens
        self.k = k
        self.h = h

        self.wallet_batch_size = wallet_batch_size

    def _start(self):
        # self.work_iterable = list(range(1, self.number_of_wallets_batch + 1))
        self.work_iterable = [idx for idx in range(1, self.number_of_wallets_batch + 1) if
                              (idx - 1) % self.n_cpu == self.cpu]
        # self.scores = []

    def _end(self):
        self.batch_work_executor.shutdown()

        # csv_file_path = 'data/multichain_wallet_credit_score.csv' if self.multichain else 'data/wallet_credit_score.csv'
        # write_csv(self.scores, csv_file_path)

    def _export(self):
        self.batch_work_executor.execute(
            self.work_iterable,
            self._export_batch,
            total_items=len(self.work_iterable)
        )

    def _export_batch(self, wallets_batch_indicates):
        for batch_idx in wallets_batch_indicates:
            try:
                start_time = time.time()
                wallets = self._graph.get_all_multichain_wallets()
                updated_wallets = []
                for wallet in wallets:
                    try:
                        updated = {"address": wallet.get("address"), "scores": {}}
                        timestamps = self.get_scoring_timestamp(wallet)
                        for timestamp in timestamps:
                            credit_score, elements = calculate_credit_score(
                                wallet, self.statistics, self.tokens, k=self.k, h=self.h,
                                current_time=int(timestamp), return_elements=True
                            )
                            updated["scores"][str(timestamp)] = update_scores_properties(
                                wallet, credit_score, elements, int(timestamp), multichain=self.multichain)
                        # with open(f"score/{updated['address']}.json", 'w') as f:
                        #     json.dump(updated, f, indent=1)
                        updated_wallets.append(updated)
                    except Exception as ex:
                        logger.exception(ex)
                        continue

                for w in updated_wallets:
                    w['flagged'] = batch_idx

                _graph = MongoDB("mongodb://localhost:27017/")
                _graph.update_multichain_wallet_scores(updated_wallets)
                # if not self.multichain:
                #     self._graph.update_wallet_scores(updated_wallets)
                # else:
                #     self._graph.update_multichain_wallet_scores(updated_wallets)

                logger.info(f'Time to update score of batch {batch_idx} is {time.time() - start_time} seconds')
                # self.scores.extend(convert_data(updated_wallets))
            except Exception as ex:
                logger.exception(ex)
                continue


    def get_scoring_timestamp(self, wallet):
        liquidate_data = wallet.get("liquidationLogs", {}).get("liquidatedWallet", {})
        timestamps = []
        min_, max_ = int(time.time()), 0
        for liquidator, logs in liquidate_data.items():
            for timestamp in logs:
                if  self.start_time > int(timestamp) or int(timestamp) >= self.end_time:
                    continue
                if timestamp not in timestamps:
                    timestamps += [str(int(timestamp) - 3600), str(int(timestamp) + 3600), timestamp]

                if int(timestamp) < min_:
                    min_ = int(timestamp)

                if int(timestamp) > max_:
                    max_ = int(timestamp)

        min_ -= 7*86400
        max_ += 7*86400
        for i in range(int(min_), int(max_), 86400):
            if i not in timestamps:
                timestamps.append(str(i))

        return timestamps
