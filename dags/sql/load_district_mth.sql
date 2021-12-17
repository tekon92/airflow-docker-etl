DELETE FROM fact_district_mth WHERE {{ params.condition }} = {{ params.curr_month }};

INSERT INTO fact_district_mth (district_id, month_sk_id, case_id, total)
SELECT district_id, to_char(tanggal, 'YYYYMM')::int as month_sk_id, case_id, sum(total) as total
FROM fact_district_dly
WHERE to_char(tanggal, 'YYYYMM')::int = {{ params.curr_month }}
GROUP BY district_id, month_sk_id, case_id;
