from pyspark import pipelines as dp
import pyspark.sql.functions as sf

def numerical_result(x):
    return (
        sf.when(sf.lower(x) == "pass", 90)
        .when(sf.lower(x) == "pass w/ conditions", 80)
        .when(sf.lower(x) == "fail", 70)
        .when(sf.lower(x) == "no entry", 0)
        .otherwise(None)
    )

@dp.table(table_properties={"delta.columnMapping.mode": "name"})
def pl1_bronze_chicago():
    df = spark.read\
            .format("csv")\
            .option("delimiter", "\t")\
            .option("multiLine", "true")\
            .option("header", "true")\
            .load("/Volumes/workspace/damg7370/restaurant_data/chicago")
    return df

@dp.table(table_properties={"delta.columnMapping.mode": "name"})
def pl1_bronze_dallas():
    df = spark.read\
            .format("csv")\
            .option("delimiter", "\t")\
            .option("multiLine", "true")\
            .option("header", "true")\
            .load("/Volumes/workspace/damg7370/restaurant_data/dallas")
    return df

@dp.table(table_properties={"delta.columnMapping.mode": "name"})
@dp.expect_or_drop("chi_name_not_null", '`DBA Name` is not null')
@dp.expect_or_drop("chi_inspection_date_not_null", '`Inspection Date` is not null')
@dp.expect_or_drop("chi_inspection_type_not_null", '`Inspection Type` is not null')
@dp.expect_or_drop("chi_valid_zip", "Zip is not null and Zip >= 0 and Zip < 100000")
@dp.expect_or_drop("chi_results_not_null", "Results is not null")
def pl2_silver_chicago():
    df = spark.read.table("pl1_bronze_chicago")
    return df

@dp.table(table_properties={"delta.columnMapping.mode": "name"})
@dp.expect_or_drop("dal_name_not_null", '"Restaurant Name" is not null')
@dp.expect_or_drop("dal_inspection_date_not_null", '`Inspection Date` is not null')
@dp.expect_or_drop("dal_inspection_type_not_null", '`Inspection Type` is not null')
@dp.expect_or_drop("dal_zip_code_valid", '`Zip Code` is not null and `Zip Code` >= 0 and `Zip Code` < 100000')
@dp.expect_or_drop("dal_valid_score", '`Inspection Score` is not null and `Inspection Score` >= 0 and `Inspection Score` <= 100')
def pl2_silver_dallas():
    df = spark.read.table("pl1_bronze_dallas")
    df = df.withColumn("Inspection Score", sf.col("Inspection Score").cast("int"))
    df = df.withColumn("Zip Code", sf.col("Zip Code").cast("int"))
    return df

@dp.table(table_properties={"delta.columnMapping.mode": "name"})
def pl3_silver_chicago():
    df = spark.readStream.table("pl2_silver_chicago")
    # facility info
    df = df.withColumn("facility_name", sf.col("DBA Name")).drop("DBA Name")
    df = df.withColumn("aka_name", sf.col("AKA Name")).drop("AKA Name")
    df = df.withColumn("facility_type", sf.col("Facility Type")).drop("Facility Type")

    #location info
    df = df.withColumn("address_line", sf.col("Address")).drop("Address")
    df = df.withColumn("zip_code", sf.col("Zip").cast("int")).drop("Zip")
    df = df.withColumn("latitude", sf.col("Latitude").cast("double"))
    df = df.withColumn("longitude", sf.col("Longitude").cast("double"))

    # inspection info
    df = df.withColumn("full_date", sf.col("Inspection Date").cast("date")).drop("Inspection Date")
    df = df.withColumn("inspection_type", sf.col("Inspection Type")).drop("Inspection Type")
    df = df.withColumn("violation_score", numerical_result(sf.col("Results"))).drop("Results")
    df = df.withColumn("clean_violations", sf.regexp_extract_all(
        "Violations",
        sf.lit(r"[0-9]+\.\s*(.*?)\s*- Comments:")
    )).drop("Violations")
    df = df.withColumn("city_name", sf.lit("Chicago"))
    return df.drop("Location").drop("License #").drop("Risk").drop("City").drop("State").drop("Inspection ID")

@dp.table(table_properties={"delta.columnMapping.mode": "name"})
def pl3_silver_dallas():
    df = spark.readStream.table("pl2_silver_dallas")

    #facility info
    df = df.withColumn("facility_name", sf.col("Restaurant Name"))
    df = df.withColumn("aka_name", sf.col("Restaurant Name")).drop("Restaurant Name")
    df = df.withColumn("facility_type", sf.lit("Restaurant"))

    #location info
    df = df.withColumn("address_line", sf.col("Street Address")).drop("Street Address")
    df = df.withColumn("zip_code", sf.col("Zip Code").cast("int")).drop("Zip Code")
    df = df.withColumn("latitude", sf.regexp_extract(sf.col("Lat Long Location"), r"\((-?[0-9]+\.[0-9]+),", 1).cast("double"))
    df = df.withColumn("longitude", sf.regexp_extract(sf.col("Lat Long Location"), r",\s*(-?[0-9]+\.[0-9]+)\)", 1).cast("double"))

    #inspection info
    df = df.withColumn("full_date", sf.col("Inspection Date").cast("date")).drop("Inspection Date")
    df = df.withColumn("inspection_type", sf.col("Inspection Type")).drop("Inspection Type")
    df = df.withColumn("violation_score", sf.col("Inspection Score")).drop("Inspection Score")
    df = df.withColumn("city_name", sf.lit("Dallas"))
    violation_cols = [sf.col(f"Violation Description - {i}") for i in range(1, 25)]
    df = df.withColumn("clean_violations", sf.array_compact(sf.array(*violation_cols)))
    for kw in ["Description", "Points", "Detail", "Memo"]:
        df = df.drop(*[sf.col(f"Violation {kw} - {i}") for i in range(1, 26)])
    return df.drop("Street Number").drop("Street Name").drop("Street Direction").drop("Street Type").drop("Street Unit").drop("Violation  Memo - 20").drop("Inspection Month").drop("Inspection Year").drop("Lat Long Location")

@dp.expect('clean_violations_nn', 'clean_violations is not null')
@dp.table()
def pl35_silver_combined():
    chi = spark.read.table("pl3_silver_chicago")
    dal = spark.read.table("pl3_silver_dallas")
    return chi.unionByName(dal)\
        .withColumn("last_updated", sf.current_timestamp())\
        .withColumn("date_key", sf.date_format(sf.col("full_date"), "yyyyMMdd"))

@dp.table()
def pl4_silver_combined():
    return spark.sql(
        """
        select uuid() as inspection_key
        , *
        from pl35_silver_combined"""
    )
    