from utilities.loggers import logger
import polars as pl
from IPython.display import display
from database.db_connection import engine
from datetime import datetime
import os, traceback, sys
from dotenv import load_dotenv
load_dotenv()

def bronze_layer_func() -> None:
    try:
        
        # Read the CSV file using Polars
        logger.info("Reading CSV file")
        df = pl.read_csv(os.getenv('FILE_PATH'), has_header=True, infer_schema=True)
        logger.success("CSV file read successfully")
        
        # display(df.head())
        
        logger.info("Adding data_ingestion_timestamp column")
        df = df.with_columns( [pl.lit(datetime.now().replace(microsecond=0)).alias('data_ingestion_timestamp_bronze') ])
        logger.success("data_ingestion_timestamp column added successfully")
        
        # display(df.head())
        
        # Convert the Polars DataFrame to a Pandas DataFrame
        logger.info("Converting Polars DataFrame to Pandas DataFrame")
        df_to_pandas = df.to_pandas()
        logger.success("Conversion successful")
        
        # Saving Data to table in the Bronze Layer as-is
        logger.info("Saving Data to table in the Bronze Layer as-is")
        df_to_pandas.to_sql(name="retail_sales_bronze", con=engine, schema="retail_sales_dwh_bronze", if_exists="replace", 
                            index=False)
        logger.success("Data saved to Bronze Layer successfully")


    except Exception as e:
        logger.error(f"Error in Bronze Layer: {e}")
        raise e



if __name__ == "__main__":
    
    try:
        print("Running Bronze Layer...")
        
        logger.info("Running Bronze Layer...")
        bronze_layer_func()
        logger.success("Bronze Layer executed successfully")
        
        print("Bronze Layer completed successfully")
    
    except Exception as e:
        logger.error(f"Error in Bronze Layer: {e}")
        print(f"Error in Bronze Layer: {e}")
        
        traceback.print_exc()
        sys.exit(1)