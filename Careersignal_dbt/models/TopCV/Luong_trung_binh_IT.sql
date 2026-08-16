WITH salary_usd AS (
    SELECT
        salary_min * 26000 AS salary_min,
        salary_max * 26000 AS salary_max,
        'VND' AS salary_currency
    FROM {{ source('source', 'topcv') }}
    WHERE salary_currency LIKE '%USD%'
),
salary_vietnam AS (
    SELECT
        salary_min,
        salary_max,
        salary_currency
    FROM {{ source('source', 'topcv') }}
    WHERE salary_currency LIKE '%VND%'
),
Luong AS (
SELECT *
FROM salary_usd
UNION ALL
SELECT *
FROM salary_vietnam
)
SELECT AVG(salary_min) AS LUONG_TRUNG_BINH_MIN,AVG(salary_max) AS LUONG_TRUNG_BINH_MAX
FROM Luong