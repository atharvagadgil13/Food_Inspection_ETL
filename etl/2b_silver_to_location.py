from pyspark import pipelines as dp
import pyspark.sql.functions as sf

location_schema = """
    location_id string,
    address_line string,
    zip_code int,
    latitude double,
    longitude double,
    last_updated timestamp
"""

dp.create_streaming_table(
    name = "dim_rpl_location",
    schema = location_schema
)

@dp.table()
def pl5_gold_location():
    df = spark.sql('select uuid() as location_id, address_line, zip_code, latitude, longitude, last_updated from pl4_silver_combined')
    return df

dp.create_auto_cdc_flow(
    source="pl5_gold_location",
    target="dim_rpl_location",
    keys = ["address_line", "zip_code"],
    sequence_by="last_updated"
)

