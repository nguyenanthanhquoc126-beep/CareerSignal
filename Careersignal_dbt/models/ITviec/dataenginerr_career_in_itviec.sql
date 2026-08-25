SELECT COUNT(*) AS NumberofDataEngineer
FROM {{ source('source', 'itviec') }}
WHERE (LOWER(title) LIKE '%data engineer%'
   OR LOWER(title) LIKE '%data engineering%'
   OR LOWER(title) LIKE '%kỹ sư dữ liệu%')