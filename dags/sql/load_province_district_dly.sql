DELETE FROM fact_province_district_dly WHERE {{ params.condition }} = '{{ params.curr_date }}';

INSERT INTO fact_province_district_dly (province_id, district_id, tanggal, case_id, total)
WITH ds as (
    SELECT kode_prov::int      as province_id,
           kode_kab::int       as district_id,
           tanggal::date       as tanggal,
           status_id           as case_id,
           sum(case
                   when dc.status_detail in ('SUSPECT') then coalesce(stg.suspect, 0)
                   when dc.status_detail in ('SUSPECT_DISCARDED') then coalesce(stg.suspect_discarded, 0)
                   when dc.status_detail in ('SUSPECT_MENINGGAL') then coalesce(stg.suspect_meninggal, 0)
                   when dc.status_detail in ('SUSPECT_DIISOLASI') then coalesce(stg.SUSPECT_DIISOLASI, 0)
                   when dc.status_detail in ('PROBABLE') then coalesce(stg.probable, 0)
                   when dc.status_detail in ('PROBABLE_DIISOLASI') then coalesce(stg.probable_diisolasi, 0)
                   when dc.status_detail in ('PROBABLE_DISCARDED') then coalesce(stg.probable_discarded, 0)
                   when dc.status_detail in ('PROBABLE_MENINGGAL') then coalesce(stg.probable_meninggal, 0)
                   when dc.status_detail in ('CONFIRMATION') then coalesce(stg.confirmation, 0)
                   when dc.status_detail in ('CONFIRMATION_SEMBUH') then coalesce(stg.confirmation_sembuh, 0)
                   when dc.status_detail in ('CONFIRMATION_MENINGGAL') then coalesce(stg.confirmation_meninggal, 0)
                   when dc.status_detail in ('CLOSECONTACT') then coalesce(stg.closecontact, 0)
                   when dc.status_detail in ('CLOSECONTACT_DIKARANTINA') then coalesce(stg.closecontact_dikarantina, 0)
                   when dc.status_detail in ('CLOSECONTACT_DISCARDED') then coalesce(stg.closecontact_discarded, 0)
                   when dc.status_detail in ('CLOSECONTACT_MENINGGAL') then coalesce(stg.closecontact_meninggal, 0)
                   else 0 end) as total
    FROM stg_covid_data stg,
         dim_case dc
    WHERE {{ params.condition }} = '{{ params.curr_date }}'
    GROUP BY province_id, district_id, tanggal, case_id
)
SELECT province_id, district_id, tanggal, case_id, total
FROM ds
WHERE total > 0;
