from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="dim_date",
    comment="Date dimension for BI drill-downs"
)
def dim_date():
    return spark.sql("""
        SELECT 
            date AS date_key,
            YEAR(date) AS year,
            QUARTER(date) AS quarter,
            MONTH(date) AS month,
            WEEKOFYEAR(date) AS week_of_year,
            DATE_FORMAT(date, 'MMMM') AS month_name
        FROM (
            SELECT EXPLODE(SEQUENCE(
                TO_DATE('2020-01-01'),
                TO_DATE('2030-12-31'),
                INTERVAL 1 DAY
            )) AS date
        )
    """)

