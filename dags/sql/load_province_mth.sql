DELETE FROM fact_province_mth WHERE {{ params.condition }} = {{ params.curr_month }};

INSERT INTO fact_province_mth (province_id, month_sk_id, case_id, total)
SELECT
       province_id,
       to_char(tanggal, 'YYYYMM')::int as month_sk_id,
       case_id,
       sum(total) as total
FROM fact_province_dly
WHERE to_char(tanggal, 'YYYYMM')::int = {{ params.curr_month }}
GROUP BY province_id, month_sk_id, case_id;
