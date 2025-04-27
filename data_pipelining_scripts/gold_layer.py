from utilities.loggers import logger
from IPython.display import display
from database.db_connection import engine
from sqlalchemy import text
import traceback, sys
from utilities.query_executor import commit_query_executor


def gold_layer_func() -> None:
    try:
        
        logger.info("Creating dim product view")

        query = """
        CREATE OR REPLACE VIEW retail_sales_dwh_gold.dim_product_view
        AS
        SELECT t.*, ROW_NUMBER() OVER(ORDER BY t.product_id) AS dim_product_key
        FROM
        (SELECT DISTINCT product_id, product_name, category, sub_category
        FROM retail_sales_dwh_silver.retail_sales_silver) AS t
        """
        commit_query_executor(query=query)
        logger.success("Dim product view created successfully")
        
        
        logger.info("Loading / Upserting dim_customer")
        query = """
        INSERT INTO retail_sales_dwh_gold.dim_product
        SELECT * FROM retail_sales_dwh_gold.dim_product_view
        ON CONFLICT (product_id)
        DO UPDATE SET
        (product_name, category, sub_category) = (EXCLUDED.product_name, EXCLUDED.category, EXCLUDED.sub_category)
        """
        commit_query_executor(query=query)
        logger.success("Dim product loaded successfully")
    
    
    except Exception as e:
        logger.error(f"Error in Gold Layer: {e}")
        traceback.print_exc()
        raise e



if __name__ == "__main__":
        
        try:
            print("Running Gold Layer...")
            
            logger.info("Running Gold Layer...")
            gold_layer_func()
            logger.success("Gold Layer executed successfully")
            
            print("Gold Layer completed successfully")
        
        except Exception as e:
            logger.error(f"Error in Gold Layer: {e}")
            print(f"Error in Gold Layer: {e}")
            
            sys.exit(1)