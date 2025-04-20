from db_connection import engine
from sqlalchemy import text
from utilities.loggers import logger



def create_schemas() -> None:
    try:
        '''
        This function creates the schemas for the Bronze, Silver, and Gold layers in the database.
        '''
        
        # Create the schema for the Bronze layer
        logger.info("Creating schema for the Bronze layer")
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS retail_sales_dwh_bronze"))
            conn.commit()
        logger.success("Schema for the Bronze layer created successfully")
        
        
        # Create the schema for the Silver layer
        logger.info("Creating schema for the Silver layer")
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS retail_sales_dwh_silver"))
            conn.commit()
        logger.success("Schema for the Silver layer created successfully")
        
        
        # Create the schema for the Gold layer
        logger.info("Creating schema for the Gold layer")
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS retail_sales_dwh_gold"))
            conn.commit()
        logger.success("Schema for the Gold layer created successfully")


    except Exception as e:
        raise e


if __name__ == "__main__":
    
    try:
        print("Creating schemas...")
        
        create_schemas()
        logger.success("Schemas created successfully")
        
        print("Schemas created successfully")
    
    except Exception as e:
        logger.error(f"Error in creating schemas: {e}")
        print(f"Error in creating schemas: {e}")
        exit(1)