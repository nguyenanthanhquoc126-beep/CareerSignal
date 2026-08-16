select city, count(job_id) AS TongCongViec
FROM {{ source('source', 'topcv') }}
GROUP BY city
ORDER BY TongCongViec DESC