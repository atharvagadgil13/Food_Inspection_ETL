from pyspark import pipelines as dp
import pyspark.sql.functions as sf

violation_schema = """
    violation_id string,
    violation_description string
"""

dp.create_streaming_table(
    name = "dim_rpl_violation",
    schema = violation_schema
)

@dp.table()
def pl5_gold_violation():
    df = spark.sql('select uuid() as violation_id, explode(clean_violations) as violation_description from pl4_silver_combined')
    return df

dp.create_auto_cdc_flow(
    source="pl5_gold_violation",
    target="dim_rpl_violation",
    keys = ["violation_description"],
    sequence_by='violation_id'
)