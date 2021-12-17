TRUNCATE TABLE dim_province;

INSERT INTO dim_province
SELECT
    DISTINCT
    kode_prov::int as province_id,
    nama_prov as province_name
FROM stg_covid_data;
