from database.db_connection import engine
from utilities.loggers import logger
from sqlalchemy import text

"""
This function can be use to execute commit query instead of definig the whole sql alchemy object every time
pass your query as string in this function
"""

def commit_query_executor(query:str) -> None:
    """
    Execute a SQL query against the database.

    Args:
        query (str): The SQL query to execute.

    Returns:
        query object.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
            return ''
    
    
    except Exception as e:
        logger.error(f"Error execute query: {e}")
        raise e