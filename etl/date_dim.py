date_dim_sql = """
drop table if exists dim_date;
create table dim_date as
select
  date_format(date_add('2010-01-01', seq), 'yyyyMMdd') as date_str,
  date_add('2010-01-01', seq) as full_date,
  day(date_add('2010-01-01', seq)) as day_of_month,
  month(date_add('2010-01-01', seq)) as month_number,
  date_format(date_add('2010-01-01', seq), 'MMMM') as month_name,
  quarter(date_add('2010-01-01', seq)) as `quarter`,
  year(date_add('2010-01-01', seq)) as `year`,
  weekofyear(date_add('2010-01-01', seq)) as week_of_year,
  case when dayofweek(date_add('2010-01-01', seq)) in (1,7) then true else false end as is_weekend_flag
from
  (select posexplode(array_repeat(1, 50000)) as (seq, _)) t;
"""