class ExporterInterface:
    def open(self):
        pass

    def export_updated(self, updated_data):
        pass

    def close(self):
        pass
