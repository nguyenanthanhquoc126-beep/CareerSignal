SELECT
    primary_role,
    ROUND(AVG(experience_years), 1) AS avg_experience_years,
    COUNT(*) AS total_jobs
FROM silver.topcv
WHERE experience_years IS NOT NULL
  AND primary_role IS NOT NULL 
  AND LOWER(primary_role) LIKE '%data%'
GROUP BY primary_role
ORDER BY total_jobs DESC