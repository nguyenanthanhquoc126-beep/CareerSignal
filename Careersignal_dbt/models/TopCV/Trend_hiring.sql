select primary_role, count(primary_role) AS TongSoTuyenDung
from {{ source('source', 'topcv') }}
GROUP BY(primary_role)
ORDER BY TongSoTuyenDung DESC