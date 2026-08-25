WITH table_data_engineer AS (
    SELECT *
    FROM {{ source('source', 'itviec') }}
    WHERE (LOWER(title) LIKE '%data engineer%'
        OR LOWER(title) LIKE '%data engineering%'
        OR LOWER(title) LIKE '%kỹ sư dữ liệu%')
)
SELECT
    skill,
    COUNT(DISTINCT job_id) AS job_count
FROM table_data_engineer
CROSS JOIN UNNEST(skills) AS t(skill)
GROUP BY skill
ORDER BY job_count DESC
LIMIT 10