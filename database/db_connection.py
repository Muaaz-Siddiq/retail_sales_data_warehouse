from sqlalchemy import create_engine
from utilities.loggers import logger
import os
from dotenv import load_dotenv
load_dotenv()


try:
    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
    
    # print("Engine created successfully")
    logger.info("Engine created successfully")

except Exception as e:
    print(f"Error creating engine: {e}")
    logger.error(f"Error creating engine: {e}")
    raise e