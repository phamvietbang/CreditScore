class ImporterInterface:
    def __init__(self, url=""):
        self.url = url

    def set_enricher_id(self, enricher_id=""):
        self._enricher_id = enricher_id

    def get_enricher_id(self):
        return self._enricher_id

    def get_latest_block_updated(self):
        """

        :param collector_id:
        :return:
        """
        return 0

    def get_oldest_block_updated(self):
        """

        :param collector_id:
        :return:
        """
        return 0

    def get_transfer_in_block_range(self, start_block, end_block):
        """

        :param start_block:
        :param end_block:
        :return:
        """
        return []

    def get_token(self, token_address):
        """
        
        :param token_address:
        :return:
        """
        return {
            "decimals": 18,
            "price": 0
        }

    def get_num_tx_of_address_after_block(self, block_number, smart_contract_address):
        """

        :param block_number:
        :param smart_contract_address:
        :return:
        """
        return 0

    def get_num_tx_of_address_between_block(self, start_block_number, end_block_number, smart_contract_address):
        """

        :param start_block_number:
        :param end_block_number:
        :param smart_contract_address:
        :return:
        """
        return 0

    def get_lending_in_block_range(self, start_block, end_block):
        """

        :param start_block:
        :param end_block:
        :return:
        """
        return []

    def get_sort_txs_in_range(self, start_timestamp, end_timestamp):
        """

        """

    def get_all_top_token(self):
        """

        """
