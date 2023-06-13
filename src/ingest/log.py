import logging

class IngestLogger:

    def __init__(self, log_file:str, log_level:str='INFO'):
        self.log_file = log_file
        self.log_level = log_level
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)
        self.formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
        self.file_handler = logging.FileHandler(self.log_file)
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)

    def log(self, message:str):
        self.logger.info(message)

    def get_last_date_ingested(self):
        with open(self.log_file, 'r') as f:
            lines = f.readlines()
            last_date_ingested = lines[-1].split(' ')[0]
            for line in reversed(lines):
                if 'Ingest complete' in line:
                    last_date_ingested = line.split(' ')[0]
                    break
        
        return last_date_ingested
