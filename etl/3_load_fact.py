from pyspark import pipelines as dp
import pyspark.sql.functions as sf

@dp.table()
def fact_rpl_inspection_result():
    return spark.sql(
        """
        SELECT
            i.inspection_key,
            i.date_key,
            f.facility_id as facility_key,
            l.location_id as location_key,
            i.inspection_type,
            i.violation_score,
            i.city_name,
            i.last_updated as loaded_timestamp
        FROM
            pl4_silver_combined i
            LEFT JOIN
            dim_rpl_facility f
            ON (i.facility_name=f.facility_name)
            LEFT JOIN
            dim_rpl_location l
            ON (i.address_line=l.address_line) AND (i.zip_code=l.zip_code)
        """
    )
