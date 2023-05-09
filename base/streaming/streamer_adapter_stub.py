class StreamerAdapterStub:

    def open(self):
        pass

    def get_current_block_number(self):
        return 0

    def export_all(self, start_block=0, end_block=0):
        pass

    def close(self):
        pass
