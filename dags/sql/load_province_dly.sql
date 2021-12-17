DELETE FROM fact_province_dly WHERE {{ params.condition }} = '{{ params.curr_date }}';

INSERT INTO fact_province_dly (tanggal, province_id, case_id, total)
SELECT
       tanggal,
       province_id,
       case_id,
       sum(total) as total
FROM fact_province_district_dly
WHERE {{ params.condition }} = '{{ params.curr_date }}'
GROUP BY
         province_id,
         tanggal,
         case_id;
