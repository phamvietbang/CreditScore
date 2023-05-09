import json
import time

from calculate.services.scores_service import update_scores_properties, update_scores_history, convert_data
from calculate.services.wallet_score import calculate_credit_score, calculate_credit_score_with_info_return
from config import get_logger, TokenMongoDBConfig
from constants import GraphCreditScoreConfigKeys
from database.arangodb_klg import ArangoDbKLG
from database.mongodb_token import MongoDbToken
from job.update_wallet_job import UpdateWalletJob
from job.scoring_job import WalletScoresJob
from utils.csv_utils import write_csv

logger = get_logger('Wallet Credit Score')


class WalletsCreditScore:
    def __init__(self, graph, statistic_path='data', k=30, tokens=None):
        self._graph = ArangoDbKLG(graph)
        self._k = k
        self._statistic_path = statistic_path

        if tokens is None:
            _token_graph = MongoDbToken(TokenMongoDBConfig.CONNECTION_URL)
            tokens_info = _token_graph.get_tokens_credit_score()
            self.tokens = {k: v['score'] for k, v in tokens_info.items()}
        else:
            self.tokens = tokens

    def get_statistics(self):
        _statistics = self._graph.get_wallet_statistics()
        return _statistics

    def calculate_wallet(self, address, chain_id, h=10):
        current_time = int(time.time())
        wallet = self._graph.get_wallet(address, chain_id=chain_id)
        if not wallet:
            logger.warning(f'Wallet with address {address} in chain {chain_id} not found')
            return None

        _statistics = self.get_statistics()

        credit_score, elements = calculate_credit_score(
            wallet, _statistics, self.tokens, k=self._k, h=h, current_time=current_time, return_elements=True)
        updated = update_scores_properties(wallet, credit_score, elements, current_time)

        self._graph.update_wallet_scores([updated])
        logger.info(f'Calculated credit score for wallet {address} after {time.time() - current_time} seconds')
        return updated

    def calculate_multichain_wallet(self, wallet, h=10, save=True, is_backup=False):
        current_time = int(time.time())
        _statistics = self.get_statistics()

        credit_score, elements = calculate_credit_score(
            wallet, _statistics, self.tokens, k=self._k, h=h, current_time=current_time, return_elements=True)
        updated = update_scores_properties(wallet, credit_score, elements, current_time, multichain=True)

        if save:
            if is_backup:
                updated['isBackup'] = True
            self._graph.update_multichain_wallet_scores([updated])
        logger.info(
            f'Calculated credit score for wallet {wallet["address"]} after {time.time() - current_time} seconds')
        return updated

    def calculate_merged_wallet(self, wallet, h=10, save=True):
        current_time = int(time.time())
        _statistics = self.get_statistics()

        credit_score, elements = calculate_credit_score(
            wallet, _statistics, self.tokens, k=self._k, h=h, current_time=current_time, return_elements=True)
        updated = update_scores_properties(wallet, credit_score, elements, current_time, merged=True)
        if save:
            self._graph.update_merged_wallet_scores([updated])
        logger.info(f'Calculated credit score for merged wallet {wallet["mergedWalletId"]}'
                    f' after {time.time() - current_time} seconds')
        return updated

    def calculate_wallet_with_info(self, wallet, h=10, save=True, is_backup=False):
        current_time = int(time.time())
        # wallet = self._graph.get_wallet(address, chain_id=chain_id)
        # if not wallet:
        #     logger.warning(f'Wallet with address {address} in chain {chain_id} not found')
        #     return None

        _statistics = self.get_statistics()

        credit_score, elements, info = calculate_credit_score_with_info_return(
            wallet, _statistics, self.tokens, k=self._k, h=h, current_time=current_time)

        if save:
            updated = update_scores_properties(wallet, credit_score, elements, current_time, multichain=True)
            if is_backup:
                updated['isBackup'] = True
            self._graph.update_multichain_wallet_scores([updated])
        logger.info(f'Calculated credit score for wallet {wallet["address"]}'
                    f' after {time.time() - current_time} seconds')

        x1, x2, x3, x4, x5 = elements
        results = {
            'address': wallet['address'],
            'credit_score': credit_score,
            'scores': {
                'total_current_asset': x1[0],
                'average_total_asset': x1[1],
                'age_of_accounts': x2[0],
                'transaction_amount': x2[1],
                'frequency_of_transaction': x2[2],
                'number_of_liquidations': x2[3],
                'total_value_of_liquidations': x2[4],
                'loan-to-balance_ratio': x3[0],
                'loan-to-investment_ratio': x3[1],
                'investment-to-total-asset_ratio': x4[0],
                # 'return_on_investment': x4[1],
                'token_score': x5[0],
                'nft_score': x5[1]
            },
            'detail': info
        }
        return results

    def wallet_flag(self, chain_id=None, batch_size=50000, max_workers=4, reset=False):
        logger.info('Start Flagging')
        start_time = time.time()
        cnt = 0
        cursor = self._graph.get_wallets_with_flagged(batch_size=batch_size, chain_id=chain_id, reset=reset)
        logger.info(f'Create cursor and fetch first batch take {time.time() - start_time} seconds')

        flagged_state = self._graph.get_wallet_flagged_state(chain_id=chain_id)
        if flagged_state and not reset:
            batch_idx = flagged_state['batch_idx']
            n_wallets_current_batch = flagged_state['n_wallets_current_batch']
        else:
            batch_idx, n_wallets_current_batch = 1, 0

        wallets_multi_batch = []
        updated_wallets = []
        try:
            while True:
                wallets = list(cursor.batch())
                cursor.batch().clear()

                for w in wallets:
                    n_wallets_current_batch += 1
                    if chain_id is None:
                        updated = {'address': w, 'flagged': batch_idx}
                    else:
                        updated = {'chainId': chain_id, 'address': w, 'flagged': batch_idx}

                    updated_wallets.append(updated)
                    if n_wallets_current_batch >= batch_size:
                        batch_idx += 1
                        n_wallets_current_batch = 0
                        wallets_multi_batch.append(updated_wallets)
                        updated_wallets = []

                        if batch_idx % max_workers == 0:
                            update_job = UpdateWalletJob(
                                batch_size=1,
                                max_workers=max_workers,
                                graph=self._graph,
                                wallets_batch=wallets_multi_batch,
                                multichain=not chain_id
                            )
                            update_job.run()
                            wallets_multi_batch = []

                cnt += len(wallets)
                logger.info(f'Flag {cnt} wallets takes {time.time() - start_time} seconds')

                if cursor.has_more():
                    start_time1 = time.time()
                    cursor.fetch()
                    logger.info(f'Fetch {len(wallets)} addresses take {time.time() - start_time1} seconds')
                else:
                    break
        except Exception as ex:
            logger.exception(ex)

        flagged_state = {
            'batch_idx': batch_idx,
            'n_wallets_current_batch': n_wallets_current_batch
        }
        if chain_id is None:
            flagged_state['_key'] = GraphCreditScoreConfigKeys.multichain_wallets_flagged_state
        else:
            flagged_state['_key'] = GraphCreditScoreConfigKeys.wallets_flagged_state + '_' + chain_id
        self._graph.update_configs([flagged_state], _type='replace')

        if updated_wallets:
            wallets_multi_batch.append(updated_wallets)

        if wallets_multi_batch:
            update_job = UpdateWalletJob(
                batch_size=1,
                max_workers=max_workers,
                graph=self._graph,
                wallets_batch=wallets_multi_batch,
                multichain=not chain_id
            )
            update_job.run()

        logger.info(f'Flag {cnt} wallets takes {time.time() - start_time} seconds')

    def calculate_wallets(self, chain_id, h=10, max_workers=4, n_cpu=1, cpu=1, wallet_batch_size=10000):
        current_time = time.time()
        _statistics = self.get_statistics()
        logger.info(str(_statistics))

        flagged_state = self._graph.get_wallet_flagged_state(chain_id=chain_id)
        batch_idx = flagged_state['batch_idx']

        wallet_scores_job = WalletScoresJob(
            batch_size=8,
            max_workers=max_workers,
            graph=self._graph,
            tokens=self.tokens,
            wallets_batch=batch_idx,
            statistics=_statistics,
            k=self._k,
            h=h,
            multichain=False,
            chain_id=chain_id,
            n_cpu=n_cpu,
            cpu=cpu,
            wallet_batch_size=wallet_batch_size
        )
        wallet_scores_job.run()
        logger.info(f'Calculated credit score for all wallets take {time.time() - current_time} seconds')

    def calculate_multichain_wallets(
            self, h=10, max_workers=4, n_cpu=1, cpu=1, wallet_batch_size=10000, current_time=int(time.time())):
        _statistics = self.get_statistics()
        logger.info(str(_statistics))

        # state_filename = 'multichain_flagged_state.txt'
        # state_file = os.path.join(self._statistic_path, state_filename)
        # with open(state_file) as f:
        #     batch_idx, n_wallets_current_batch = [int(r.strip()) for r in f.readlines()]

        flagged_state = self._graph.get_wallet_flagged_state()
        batch_idx = flagged_state['batch_idx']

        wallet_scores_job = WalletScoresJob(
            batch_size=8,
            max_workers=max_workers,
            graph=self._graph,
            tokens=self.tokens,
            wallets_batch=batch_idx,
            statistics=_statistics,
            k=self._k,
            h=h,
            n_cpu=n_cpu,
            cpu=cpu,
            wallet_batch_size=wallet_batch_size
        )
        wallet_scores_job.run()
        logger.info(f'Calculated credit score for all wallets take {time.time() - current_time} seconds')

    def calculate_multichain_wallets_history(self, chain_id, pool_address, history_days=30, h=10, batch_size=1000):
        _statistics = self.get_statistics()
        logger.info(str(_statistics))

        start_calculate = time.time()
        current_time = int(time.time())
        scores = []
        cnt = 0
        try:
            wallets = self._graph.get_multichain_wallets_lending(pool_address, chain_id, batch_size=batch_size)

            updated_wallets = []
            for wallet in wallets:
                updated_wallet = {}
                for idx in range(history_days, 0, -1):
                    current_time = int(start_calculate) - (idx - 1) * 86400
                    credit_score, elements = calculate_credit_score(wallet, _statistics, self.tokens, k=self._k, h=h,
                                                                    current_time=current_time, return_elements=True)
                    updated = update_scores_properties(wallet, credit_score, elements, current_time, multichain=True)
                    updated_wallet = update_scores_history(updated_wallet, updated)
                updated_wallets.append(updated_wallet)
                cnt += 1

                if len(updated_wallets) >= batch_size:
                    self._graph.update_multichain_wallet_scores(updated_wallets)
                    scores.extend(updated_wallets)
                    updated_wallets = []
                    logger.info(f'Time to update score of {cnt} wallets is {time.time() - start_calculate} seconds')

            self._graph.update_multichain_wallet_scores(updated_wallets)
            scores.extend(updated_wallets)
            logger.info(f'Time to update score of {cnt} wallets is {time.time() - start_calculate} seconds')

        except Exception as err:
            logger.exception(err)

        with open(f'{self._statistic_path}/multichain_wallet_history_scores.json', 'w') as f:
            json.dump(scores, f)
        logger.info(f'Calculated credit score for all {len(scores)} wallets take {time.time() - current_time} seconds')

    def trava_scores(self, pool_address, batch_size=1000):
        _statistics = self.get_statistics()
        logger.info(str(_statistics))

        start_calculate = time.time()
        scores = []
        cnt = 0
        try:
            wallets = self._graph.get_wallets_lending(pool_address, batch_size=batch_size)
            # wallets = list(wallets)
            # with open(f'{self._statistic_path}/wallet_tmp.json') as f:
            #     wallets = json.load(f)
            # logger.info(f'Load {len(wallets)} took {time.time() - start_calculate} seconds')

            updated_wallets = []
            current_time = int(time.time())
            for wallet in wallets:
                credit_score, elements = calculate_credit_score(wallet, _statistics, self.tokens, k=self._k, h=10,
                                                                current_time=current_time, return_elements=True)
                updated = update_scores_properties(wallet, credit_score, elements, current_time)
                updated_wallets.append(updated)
                cnt += 1

                if len(updated_wallets) >= batch_size:
                    self._graph.update_wallet_scores(updated_wallets)
                    scores.extend(convert_data(updated_wallets))
                    updated_wallets = []
                    logger.info(f'Time to update score of {cnt} wallets is {time.time() - start_calculate} seconds')

            self._graph.update_wallet_scores(updated_wallets)
            scores.extend(convert_data(updated_wallets))
            logger.info(f'Time to update score of {cnt} wallets is {time.time() - start_calculate} seconds')

        except Exception as err:
            logger.exception(err)

        write_csv(scores, f'{self._statistic_path}/trava_score.csv')
        return scores


def round_timestamp_to_date(timestamp):
    timestamp_a_day = 86400
    timestamp_unit_day = timestamp / timestamp_a_day
    recover_to_unit_second = int(timestamp_unit_day) * timestamp_a_day
    return recover_to_unit_second


def check_logs(timestamp, log_timestamps):
    """
    Check if timestamp and latest log timestamps is in a day
    """
    if not log_timestamps:
        return False
    return round_timestamp_to_date(timestamp) == round_timestamp_to_date(log_timestamps[-1])


if __name__ == '__main__':
    trava_pool_address = '0x75de5f7c91a89c16714017c7443eca20c7a8c295'
    chain = '0x38'

    calculator = WalletsCreditScore('arangodb@read_only:read_only_123@http://139.59.226.71:1012',
                                    statistic_path='../data')
    # calculator.trava_scores(trava_pool_address, batch_size=200)
    calculator.calculate_multichain_wallets_history(chain, trava_pool_address, batch_size=200)
