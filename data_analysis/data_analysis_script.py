import polars as pl
from IPython.display import display
from database.db_connection import engine


tdf = pl.read_database(query="SELECT * FROM retail_sales_dwh_bronze.retail_sales_bronze", connection=engine, infer_schema_length=None)

display(tdf.head(3))


# Dropping all the null and duplicate rows
tdf = tdf.filter(pl.any_horizontal(pl.col('*').is_not_null())).unique()


# Check if entire row is null.
tdf.filter(pl.all_horizontal(pl.col('*').is_null())).height

# Check if entire column is null.
tdf.select(
    [
        (pl.col(cols).is_null().all()).alias(cols) for cols in tdf.columns
    ]
)

# Check if there are duplicate rows.
tdf.is_duplicated().sum()

# Checking for data integrity
tdf.count()

# Check for null values in each column (count).
tdf.select(
    [
        (pl.col(cols).is_null().sum()).alias(cols) for cols in tdf.columns
    ]
)


# Check to fill null values in required columns.
tdf.select(
    'Customer ID', 'Customer Name', 'Country', 'City', 'State', 'Region', 'Postal Code').filter(
        pl.col('Postal Code').is_null()
    )


# Check for relevent data to fill
tdf.select(
    'Customer ID', 'Customer Name', 'Country', 'City', 'State', 'Region', 'Postal Code').\
        filter(
            pl.col('City') == 'Burlington' ,
            pl.col('State') == 'Vermont' ,
            pl.col('Region') == 'East' ).with_columns([
    pl.col('Postal Code')
])


# Casting int to remove extra decimal points in postal code and fill it with N/A converting it to string
tdf.with_columns([
    pl.col('Postal Code').cast(int).fill_null('N/A')
])

# Check for all unique values in each column
tdf.select(pl.col('Country')).unique().sort('Country')
tdf.select(pl.col('Region')).unique().sort('Region')
tdf.select(pl.col('State')).unique().sort('State')
tdf.select(pl.col('City')).unique().sort('City')
tdf.select(pl.col('Segment')).unique().sort('Segment')
tdf.select(pl.col('Category')).unique().sort('Category')
tdf.select(pl.col('Sub-Category')).unique().sort('Sub-Category')
tdf.select(pl.col('Sales')).unique().sort('Sales')
tdf.select(pl.col('Product Name')).unique().sort('Product Name')
tdf.select(pl.col('Customer Name')).unique().sort('Customer Name')


# Normalizing column names
tdf.rename(
    {col : col.strip().replace(" ", "_").lower() for col in tdf.columns }
)


# fix date format and convert to datetime
nd = tdf.with_columns([
    pl.col("Order Date").str.strptime(pl.Date, "%d/%m/%Y").alias("Order Date"),
    pl.col("Ship Date").str.strptime(pl.Date, "%d/%m/%Y").alias("Ship Date")
])


# Changing Sales values to 2 decimal places
tdf.with_columns([
    pl.col("Sales").round(2)
])



# Check if there any row where ship date is before order date
nd.filter(
    pl.col('Ship Date') < pl.col('Order Date')
).height


# Checking for prefix in Order Id as accordifg to the business rule it should be 'country-year-order_id' 
tdf.select(
    pl.col('Order ID').str.split('-').list.first().unique().alias('prefix'),
)


tdf = tdf.with_columns([
    pl.col('Order ID').str.replace('^CA', 'US')
])

tdf.select('Order ID')

tdf.filter(
    pl.col('Order ID').str.starts_with('CA')
)


tdf.unique(subset=['Customer ID']).height



# Check if Customer ID is same as the initials of Customer Name
# For example: Customer Name = John Doe, Customer ID = JD-1234
# NOTE: THIS IS JUST A SAMPLE CHECK, NOT A BUSINESS RULE
tdf.select('Customer ID', 'Customer Name').filter(
    pl.col('Customer ID').str.split('-').list.first()
    !=
    (pl.col('Customer Name').str.split(' ').list.first().str.slice(0, 1) + 
    pl.col('Customer Name').str.split(' ').list.last().str.slice(0, 1))
)

initials = (pl.col('Customer Name').str.split(' ').list.first().str.slice(0, 1)) + (pl.col('Customer Name').str.split(' ').list.last().str.slice(0, 1))


tdf.with_columns([
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


# In Customer Name there is a test entry with name " Sample Company A" (most probably a test entry) therefore we need to remove it
tdf.filter(
    ~pl.col('Customer Name').str.to_lowercase().str.contains_any(['sample', 'test'])
).select("Customer Name")


tdf.group_by('Order ID', 'Product ID').agg(
    pl.len().alias('count'),
).filter(
    pl.col('count') > 1 )


tdf = tdf.sort('Order Date').unique(subset=['Order ID', 'Product ID'], keep='last')


# Check for sales values difference
display(tdf.group_by(['Category',
    'Sub-Category',
    'Product Name',
    'City']).agg(pl.col('Sales').max().alias('Max_Sales'), pl.col('Sales').min().alias('Min_Sales')
                ).sort(['Product Name', 'City']).head(15))



tdf.with_columns([
    (
        ((pl.col("Sales") - pl.col("Sales").min().over(["Category", "Sub-Category"])) * 100)
        .round(0)  # round to integer after scaling
        / 100
    ).alias('potential_delivery_charge')
]).select('Category', 'Sub-Category', 'Product Name', 'City', 'Sales', 'potential_delivery_charge')



tdf.with_columns([
        (pl.col('Sales') - pl.col("Sales").min()).over(["Category", "Sub-Category"])\
        .alias('potential_delievery_charge')
]).select('Category', 'Sub-Category', 'Product Name', 'City', 'Sales', 
        'potential_delievery_charge').filter(pl.col('potential_delievery_charge') == 0)



tdf.with_columns([
    (
        (pl.col('Sales') / pl.col('Sales').min().over(["Category", "Sub-Category"])).floor()
    ).alias('potential_quantity')
])