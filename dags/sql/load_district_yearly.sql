DELETE FROM fact_district_yearly WHERE {{ params.condition }} = {{ params.curr_year}};
INSERT INTO fact_district_yearly (district_id, year_sk_id, case_id, total)
SELECT
       district_id,
       left(month_sk_id::varchar, 4)::int as year_sk_id,
       case_id,
       sum(total)
FROM fact_district_mth
WHERE left(month_sk_id::varchar, 4)::int = {{ params.curr_year}}
GROUP BY district_id, year_sk_id, case_id;
