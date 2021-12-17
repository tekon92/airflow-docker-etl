DELETE FROM fact_province_year WHERE {{ params.condition }} = {{ params.curr_year }};
INSERT INTO fact_province_year (province_id, year_sk_id, case_id, total)
SELECT
    province_id,
    left(month_sk_id::varchar, 4)::int as year_sk_id ,
    case_id,
    sum(total) as total
FROM fact_province_mth
WHERE left(month_sk_id::varchar, 4)::int = {{ params.curr_year }}
GROUP BY
    province_id,
    month_sk_id,
    case_id;
