from pyspark import pipelines as dp
import pyspark.sql.functions as sf

@dp.table()
def dim_rpl_inspection_violation_bridge():
    df = spark.sql(
        """
        select inspection_key, explode(clean_violations) as violation_description
        from pl4_silver_combined
        """
    ).distinct().join(
        spark.table('dim_rpl_violation'),
        on="violation_description",
        how="left"
    )
    return df.drop('violation_description')


