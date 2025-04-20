from utilities.loggers import logger
import polars as pl
from IPython.display import display
from database.db_connection import engine
import os, traceback


df = pl.read_database(query="SELECT * FROM retail_sales_dwh_bronze.retail_sales_bronze", connection=engine, infer_schema_length=None)

display(df)


# Dropping all the null and duplicate rows
df = df.filter(pl.any_horizontal(pl.col('*').is_not_null())).unique()


# Check if entire row is null.
df.filter(pl.all_horizontal(pl.col('*').is_null())).height

# Check if entire column is null.
df.select(
    [
        (pl.col(cols).is_null().all()).alias(cols) for cols in df.columns
    ]
)

# Check if there are duplicate rows.
df.is_duplicated().sum()

# Checking for data integrity
df.count()

# Check for null values in each column (count).
df.select(
    [
        (pl.col(cols).is_null().sum()).alias(cols) for cols in df.columns
    ]
)


# Check to fill null values in required columns.
df.select(
    'Customer ID', 'Customer Name', 'Country', 'City', 'State', 'Region', 'Postal Code').filter(
        pl.col('Postal Code').is_null()
    )


# Check for relevent data to fill
df.select(
    'Customer ID', 'Customer Name', 'Country', 'City', 'State', 'Region', 'Postal Code').\
        filter(
            pl.col('City') == 'Burlington' ,
            pl.col('State') == 'Vermont' ,
            pl.col('Region') == 'East' ).with_columns([
    pl.col('Postal Code')
])


# Casting int to remove extra decimal points in postal code and fill it with N/A converting it to string
df.with_columns([
    pl.col('Postal Code').cast(int).fill_null('N/A')
])

# Check for all unique values in each column
df.select(pl.col('Country')).unique().sort('Country')
df.select(pl.col('Region')).unique().sort('Region')
df.select(pl.col('State')).unique().sort('State')
df.select(pl.col('City')).unique().sort('City')
df.select(pl.col('Segment')).unique().sort('Segment')
df.select(pl.col('Category')).unique().sort('Category')
df.select(pl.col('Sub-Category')).unique().sort('Sub-Category')
df.select(pl.col('Sales')).unique().sort('Sales')
df.select(pl.col('Product Name')).unique().sort('Product Name')


# Normalizing column names
df.rename(
    {col : col.strip().replace(" ", "_").lower() for col in df.columns }
)


# fix date format and convert to datetime
nd = df.with_columns([
    pl.col("Order Date").str.strptime(pl.Date, "%d/%m/%Y").alias("Order Date"),
    pl.col("Ship Date").str.strptime(pl.Date, "%d/%m/%Y").alias("Ship Date")
])


# Changing Sales values to 2 decimal places
df.with_columns([
    pl.col("Sales").round(2)
])



# Check if there any row where ship date is before order date
nd.filter(
    pl.col('Ship Date') < pl.col('Order Date')
).height


# Checking for prefix in Order Id as accordifg to the business rule it should be 'country-year-order_id' 
df.select(
    pl.col('Order ID').str.split('-').list.first().unique().alias('prefix'),
)


df.with_columns([
    pl.col('Order ID').str.replace('^CA', 'US')
]).filter(
    pl.col('Order ID').str.starts_with('CA')
).height


df.unique(subset=['Customer ID']).height



# Check if Customer ID is same as the initials of Customer Name
# For example: Customer Name = John Doe, Customer ID = JD-1234
# NOTE: THIS IS JUST A SAMPLE CHECK, NOT A BUSINESS RULE
df.select('Customer ID', 'Customer Name').filter(
    pl.col('Customer ID').str.split('-').list.first()
    !=
    (pl.col('Customer Name').str.split(' ').list.first().str.slice(0, 1) + 
    pl.col('Customer Name').str.split(' ').list.last().str.slice(0, 1))
)

initials = (pl.col('Customer Name').str.split(' ').list.first().str.slice(0, 1)) + (pl.col('Customer Name').str.split(' ').list.last().str.slice(0, 1))


df.with_columns([
    pl.when(pl.col('Customer ID').str.split('-').list.first()
    != initials
    ).then(
        pl.col('Customer ID').str.replace(
            r"^[^-]+",
            initials
        )
    ).otherwise(pl.col('Customer ID'))
]).select('Customer ID', 'Customer Name').filter(
    pl.col('Customer ID').str.split('-').list.first()
    !=
    (pl.col('Customer Name').str.split(' ').list.first().str.slice(0, 1) + 
    pl.col('Customer Name').str.split(' ').list.last().str.slice(0, 1))
).height