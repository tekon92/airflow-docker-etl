TRUNCATE TABLE dim_district;

INSERT INTO dim_district
select
    distinct
    kode_kab::int as district_id,
    kode_prov::int as province_id,
    nama_kab as district_name
from stg_covid_data;
