import logging, os

class Logger:
    
    def __init__(self):
        
        logging.basicConfig(
        filename=os.path.join(os.getcwd(), "logs/retails_sales_dwh.log"),  # Log file name
        level=logging.INFO,  # Logging level
        format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
        datefmt="%Y-%m-%d %H:%M:%S") # Date format
        
        self.logger = logging.getLogger(__name__)
    
    def info(self, message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def debug(self, message):
        self.logger.debug(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def success(self, message):
        self.logger.info(f"SUCCESS: {message}")


logger = Logger()