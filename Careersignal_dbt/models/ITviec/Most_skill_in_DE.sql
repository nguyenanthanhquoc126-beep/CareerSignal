SELECT
    skill,
    COUNT(DISTINCT job_id) AS job_count
FROM {{ ref('dataenginerr_career_in_itviec') }}
CROSS JOIN UNNEST(skills) AS t(skill)
GROUP BY skill
ORDER BY job_count DESC
LIMIT 10