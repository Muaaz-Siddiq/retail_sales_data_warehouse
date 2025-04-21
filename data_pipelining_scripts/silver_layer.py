from utilities.loggers import logger
import polars as pl
from IPython.display import display
from database.db_connection import engine
from datetime import datetime
import os, traceback
from dotenv import load_dotenv
load_dotenv()


def silver_layer_func() -> None:
    try:
        
        # Read Data from Bronze Layer Table
        logger.info("Reading Data from Bronze Layer Table")
        df = pl.read_database(query="SELECT * FROM retail_sales_dwh_bronze.retail_sales_bronze", connection=engine, infer_schema_length=None)
        logger.success("Data read from Bronze Layer Table successfully")
        
        
        # Dropping rows with all null values and duplicate rows
        logger.info("Dropping rows with all null values and duplicate rows")
        df = df.filter(pl.any_horizontal(pl.col('*').is_not_null() )).unique()
        logger.success("Rows with all null values and duplicate rows dropped successfully")
    
    
        # Fill null values in postal code with N/A as there is not much information available to fill it.
        logger.info("Filling null values in Postal Code with N/A")
        df = df.with_columns([ pl.col('Postal Code').cast(int).fill_null('N/A') ])
        logger.success("Null values in Postal Code filled with N/A successfully")
    
    
        # Normalizing column names
        logger.info("Normalizing column names")
        df = df.rename( {col : col.rstrip().replace(" ", "_").lower() for col in df.columns } )
        logger.success("Column names normalized successfully")
    
    
        # Fixing Order ID prefix
        logger.info("Fixing Order ID prefix")
        df = df.with_columns([
            pl.col('Order ID').str.replace('^CA', 'US')
        ])
        logger.success("Order ID prefix fixed successfully")
    
    
        # Dropping unwanted columns
        logger.info("Dropping Row ID")
        df = df.drop('row_id')
        logger.success("Row ID dropped successfully")
    
    
        # Converting Dates columns to ISO standard format and changing the data type to Date
        logger.info("Converting Date columns to ISO standard format")
        df = df.with_columns([
            pl.col('order_date').str.strptime(pl.Date, format="%d/%m/%Y").alias('order_date'),
            pl.col('ship_date').str.strptime(pl.Date, format="%d/%m/%Y").alias('ship_date')
        ])
    
    
        # Sorting the DataFrame by Order Date in ascending order
        logger.info("Sorting the DataFrame by Order Date in ascending order")
        df = df.sort('order_date')
        logger.success("DataFrame sorted by Order Date in ascending order successfully")
    
    
        # Rounding the Sales values to 2 decimal points
        logger.info("Rounding the Sales values to 2 decimal points")
        df = df.with_columns([
            pl.col('sales').round(2)
        ])
        logger.success("Sales values rounded to 2 decimal points successfully")
    
    
        # Removing test data through name column as it contains test data
        logger.info("Removing test data through name column")
        df = df.filter(
            ~ pl.col('customer_name').str.to_lowercase().str.contains_any(['test', 'sample', 'demo', 'example'])
        )
        logger.success("Test data removed successfully")
    
    
        # Removing duplicate orders by keeping only the latest order
        logger.info("Removing duplicate Orders")
        df = df.unique(subset=['order_id', 'product_id'], keep='last')
        logger.success("Duplicate Orders removed successfully")
    
    
        dim_order = df.select( 'order_id', 'order_date').with_columns([
            pl.col('order_id').cum_count().alias('dim_order_key')
        ])
        
        display(df.join(dim_order.select(['order_id','dim_order_key']), how='left', left_on='order_id', right_on='order_id').drop('order_id', 'order_date').sort('dim_order_key', descending=True).limit(5))
    
    
    except Exception as e:
        logger.error(f"Error in Silver Layer: {e}")
        raise e





if __name__ == "__main__":
    
    try:
        print("Running Silver Layer...")
        
        logger.info("Running SilverBronze Layer...")
        silver_layer_func()
        logger.success("Silver Layer completed successfully")
        
        print("Silver Layer completed successfully")
    
    except Exception as e:
        logger.error(f"Error in Silver Layer: {e}")
        print(f"Error in Silver Layer: {e}")
        
        traceback.print_exc()
        exit(1)