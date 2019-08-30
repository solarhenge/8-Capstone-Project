class SqlQueries:

    insert_into_fct_cic = ("""
INSERT INTO fct_cic
(
    cicid,
    i94cit,
    i94res,
    i94port,
    arrdate,
    i94mode,
    i94addr,
    depdate,
    i94bir,
    i94visa,
    i94count,
    matflag,
    biryear,
    gender,
    airline,
    fltno,
    visatype
)
SELECT
    cicid,
    i94cit,
    i94res,
    i94port,
    to_timestamp(arrdate, 'YYYYMMDD')::timestamp without time zone,
    i94mode,
    i94addr,
    to_timestamp(depdate, 'YYYYMMDD')::timestamp without time zone,
    i94bir,
    i94visa,
    i94count,
    matflag,
    biryear,
    gender,
    airline,
    fltno,
    visatype
FROM stg_cic
""")

    insert_into_dim_time = ("""
INSERT INTO dim_time
SELECT distinct to_timestamp(stg_cic.arrdate, 'YYYYMMDD')::timestamp without time zone as starttime
      ,extract(year from starttime) as year
      ,extract(month from starttime) as month
      ,extract(day from starttime) as day
      ,extract(week from starttime) as week
      ,extract(dayofweek from starttime) as weekday
FROM stg_cic
""")
