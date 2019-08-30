create_table_dim_addr = """
CREATE TABLE IF NOT EXISTS dim_addr
(
    i94addr character varying(256) NOT NULL,
    addrname character varying(256)
);
"""

create_table_dim_airline = """
CREATE TABLE IF NOT EXISTS dim_airline
(
    airline_name character varying(256) NOT NULL,
    airline character varying(256) NOT NULL,
    iata_code character varying(256),
    icao_designator character varying(256),
    country_territory character varying(256)
);
"""

create_table_dim_airport_codes = """
CREATE TABLE IF NOT EXISTS dim_airport_codes
(
    ident character varying(256) NOT NULL,
    type character varying(256),
    name character varying(256),
    elevation_ft double precision,
    continent character varying(256),
    iso_country character varying(256),
    iso_region character varying(256),
    municipality character varying(256),
    gps_code character varying(256),
    icao_designator character varying(256),
    local_code character varying(256),
    coordinates character varying(256)
);
"""

create_table_dim_cnty = """
CREATE TABLE IF NOT EXISTS dim_cnty
(
    i94cnty integer NOT NULL,
    cntyname character varying(256)
);
"""

create_table_dim_gender = """
CREATE TABLE IF NOT EXISTS dim_gender
(
    gender character varying(256) NOT NULL,
    gendername character varying(256)
);
"""

create_table_dim_mode = """
CREATE TABLE IF NOT EXISTS dim_mode
(
    i94mode integer NOT NULL,
    modename character varying(256)
);
"""

create_table_dim_port = """
CREATE TABLE IF NOT EXISTS dim_port
(
    i94port character varying(256) NOT NULL,
    us_port_of_entry_location character varying(256)
);
"""

create_table_dim_time = """
CREATE TABLE IF NOT EXISTS dim_time
(
    starttime timestamp without time zone NOT NULL,
    year integer,
    month integer,
    day integer,
    week integer,
    weekday character varying(256)
);
"""

create_table_fct_cic = """
CREATE TABLE IF NOT EXISTS fct_cic
(
    cicid integer,
    i94cit integer,
    i94res integer,
    i94port character varying(256),
    arrdate timestamp without time zone,
    i94mode integer,
    i94addr character varying(256),
    depdate timestamp without time zone,
    i94bir integer,
    i94visa integer,
    i94count integer,
    matflag character varying(256),
    biryear integer,
    gender character varying(256),
    airline character varying(256),
    fltno character varying(256),
    visatype character varying(256)
);
"""

create_table_stg_cic = """
CREATE TABLE IF NOT EXISTS stg_cic
(
    cicid integer,
    i94cit integer,
    i94res integer,
    i94port character varying(256),
    arrdate character varying(256),
    i94mode integer,
    i94addr character varying(256),
    depdate character varying(256),
    i94bir integer,
    i94visa integer,
    i94count integer,
    matflag character varying(256),
    biryear integer,
    gender character varying(256),
    airline character varying(256),
    fltno character varying(256),
    visatype character varying(256)
);
"""

create_table_dim_visas= """
CREATE TABLE IF NOT EXISTS dim_visas
(
    i94visa integer NOT NULL,
    visaname character varying(256)
);
"""

create_table_dim_visatype = """
CREATE TABLE IF NOT EXISTS dim_visatype
(
    visatype character varying(256) NOT NULL,
    description character varying
);
"""
