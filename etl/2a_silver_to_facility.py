from pyspark import pipelines as dp
import pyspark.sql.functions as sf

facility_schema = """
  facility_id string,
  facility_name string,
  aka_name string,
  facility_type string,
  __START_AT timestamp,
  __END_AT timestamp
"""

dp.create_streaming_table(
    name="dim_rpl_facility",
    schema = facility_schema
)

@dp.table()
def pl5_gold_facility():
    df = spark.sql('SELECT uuid() as facility_id, facility_name, aka_name, facility_type, last_updated FROM workspace.damg7370.pl4_silver_combined')
    return df

dp.create_auto_cdc_flow(
    target="dim_rpl_facility",
    source="pl5_gold_facility",
    keys=["facility_name"],
    sequence_by=sf.col("last_updated"),
    stored_as_scd_type=2,
    except_column_list = ["last_updated"]
)