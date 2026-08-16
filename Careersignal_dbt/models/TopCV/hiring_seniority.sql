SELECT
    seniority,
    COUNT(*) AS total_jobs
FROM {{ source('source', 'topcv') }}
WHERE seniority IS NOT NULL
GROUP BY seniority
ORDER BY total_jobs DESC