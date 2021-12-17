DELETE FROM fact_district_dly WHERE {{ params.condition }} = '{{ params.curr_date }}';
INSERT INTO fact_district_dly (tanggal, district_id, case_id, total)
SELECT
       tanggal,
       district_id,
       case_id,
       sum(total) as total
FROM fact_province_district_dly
WHERE {{ params.condition }} = '{{ params.curr_date }}'
GROUP BY tanggal, district_id, case_id;
