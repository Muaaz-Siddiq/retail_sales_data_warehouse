from db_connection import engine
from sqlalchemy import text
from utilities.loggers import logger
import sys



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



def create_facts_dimension_tables() -> None:
    try:
        '''
        This function creates the fact and dimension tables in the database.
        '''
        
        # Create the fact table for the Silver layer
        logger.info("Creating dim orders table")
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS retail_sales_dwh_gold.dim_orders (
                    order_id VARCHAR(80) UNIQUE,
                    dim_order_key INT
                )
            """))
            conn.commit()
        logger.success("Dim orders table created successfully")
        
        
        logger.info('Creating dim location table')
        with engine.connect() as conn:
            conn.execute(
                text(
                    """  
                    CREATE TABLE IF NOT EXISTS retail_sales_dwh_gold.dim_location (
                        country VARCHAR(50),
                        region VARCHAR(50),
                        state VARCHAR(50),
                        city VARCHAR(50),
                        postal_code INT,
                        dim_location_key INT
                    )
                    """
                ))
            conn.commit()
        logger.success("Dim location table created successfully")
        
        
        logger.info('Creating dim shipment table')
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS retail_sales_dwh_gold.dim_shipment(
                        ship_mode VARCHAR(80),
                        dim_shipment_key INT
                    )
                    """
                ))
            conn.commit()
            logger.success("Dim shipment table created successfully")
        
        
        logger.info('Creating dim customer table')
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS retail_sales_dwh_gold.dim_customer(
                        customer_id VARCHAR(80) UNIQUE,
                        customer_name VARCHAR(100),
                        customer_segment VARCHAR(60),
                        dim_customer_key INT
                        )
                    """
                )
            )
            conn.commit()
        logger.success("Dim customer table created successfully")
        
        
        logger.info('Creating dim product table')
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS retail_sales_dwh_gold.dim_product(
                        product_id VARCHAR(80) UNIQUE,
                        product_name VARCHAR(300),
                        category VARCHAR(100),
                        sub_category VARCHAR(100),
                        dim_product_key INT
                    )
                    """
                )
            )
            conn.commit()
        logger.success("Dim product table created successfully")
        
        
        logger.info('Creating dim date table')
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS retail_sales_dwh_gold.dim_date(
                        dates DATE,
                        dim_date_key INT
                    )
                    """
                ))
            conn.commit()
        logger.success("Dim date table created successfully")
        
        
        
        logger.info('Creating Fact sales table')
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS retail_sales_dwh_gold.fact_sales(
                        dim_order_key INT,
                        dim_order_date_key INT,
                        dim_location_key INT,
                        dim_shipment_key INT,
                        dim_shipment_date_key INT,
                        dim_customer_key INT,
                        dim_product_key INT,
                        sales_amount FLOAT,
                        potential_delivery_charges FLOAT,
                        potential_selling_price FLOAT,
                        potential_quantities INT
                        )
                        """
                )
            )
            conn.commit()
        logger.success("Fact sales table created successfully")


    except Exception as e:
        raise e


if __name__ == "__main__":
    
    try:
        logger.info("Creating schemas...")
        print("Creating schemas...")
        
        create_schemas()
        logger.success("Schemas created successfully")
        
        print("Schemas created successfully")
        
        logger.info("Creating fact and dimension tables...")
        print("Creating fact and dimension tables...")
        create_facts_dimension_tables()
        logger.success("Fact and dimension tables created successfully")
        print("Fact and dimension tables created successfully")
    
    except Exception as e:
        logger.error(f"Error in creating schemas: {e}")
        print(f"Error in creating schemas: {e}")
        sys.exit(1)