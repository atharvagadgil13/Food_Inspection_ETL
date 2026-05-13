for t in [
    'workspace.damg7370.fact_rpl_inspection_result',
    'workspace.damg7370.dim_rpl_date',
    'workspace.damg7370.dim_rpl_facility',
    'workspace.damg7370.dim_rpl_location',
    'workspace.damg7370.dim_rpl_violation',
    'workspace.damg7370.dim_rpl_inspection_violation_bridge'
]:
    df = spark.table(t)
    output_path = f'/Volumes/workspace/damg7370/restaurant_data/output3/{t.split(".")[-1]}'
    df.write.mode("overwrite").format("csv").option("header", "true").save(output_path)
# Volumes paths are not supported for this operation; use /tmp/ for file output.