import requests
import trino
from datetime import date
import os
from dotenv import load_dotenv
load_dotenv()

WEBHOOK_URL = os.getenv('WEBHOOK_URL')

conn = trino.dbapi.connect(
    host="localhost",
    port=8085,
    user="airflow",
    catalog="nessie",
    schema="gold",
)
cursor = conn.cursor()

# ===== 1. Tổng số tin Data Engineer =====
cursor.execute("""
    SELECT *
    FROM career_data_engineer
""")
total_de_jobs = cursor.fetchone()[0]

# ===== 2. Top kỹ năng DE =====
cursor.execute("""
    SELECT skill, job_count
    FROM skil_de_need
    ORDER BY job_count DESC
    LIMIT 10
""")
top_skills = cursor.fetchall()

# ===== 3. Phân bố theo cấp bậc (toàn ngành IT) =====
cursor.execute("""
    SELECT seniority, total_jobs
    FROM hiring_seniority
    ORDER BY total_jobs DESC
""")
seniority_data = cursor.fetchall()

# ===== 4. Lương trung bình toàn ngành IT =====
cursor.execute("""
    SELECT luong_trung_binh_min, luong_trung_binh_max
    FROM luong_trung_binh_it
""")
salary_min, salary_max = cursor.fetchone()

# ===== 5. Top thành phố có nhiều tin tuyển dụng nhất (toàn ngành IT) =====
cursor.execute("""
    SELECT city, tongcongviec
    FROM phan_bo_viec_lam
    ORDER BY tongcongviec DESC
    LIMIT 5
""")
top_cities = cursor.fetchall()

# ===== 6. Top vị trí liên quan Data theo kinh nghiệm & số tin (toàn ngành IT) =====
cursor.execute("""
    SELECT primary_role, avg_experience_years, total_jobs
    FROM themostcvindata
    ORDER BY total_jobs DESC
    LIMIT 10
""")
most_cv_data = cursor.fetchall()

# ===== 7. Xu hướng tuyển dụng theo vị trí (toàn ngành IT) =====
cursor.execute("""
    SELECT primary_role, tongsotuyendung
    FROM trend_hiring
    ORDER BY tongsotuyendung DESC
    LIMIT 10
""")
trend_hiring = cursor.fetchall()

# ===== Build từng khối văn bản động =====
def build_lines(rows, fmt):
    if not rows:
        return "Không có dữ liệu"
    return "\n".join(fmt(i, row) for i, row in enumerate(rows))

skill_lines = build_lines(
    top_skills, lambda i, r: f"{i+1}. {r[0]} — {r[1]} tin"
)
seniority_lines = build_lines(
    seniority_data, lambda i, r: f"• {r[0]}: {r[1]} tin"
)
city_lines = build_lines(
    top_cities, lambda i, r: f"{i+1}. {r[0]} — {r[1]} tin"
)
most_cv_data_lines = build_lines(
    most_cv_data, lambda i, r: f"{i+1}. {r[0]} — {r[2]} tin (KN TB: {r[1]} năm)"
)
trend_hiring_lines = build_lines(
    trend_hiring, lambda i, r: f"{i+1}. {r[0]} — {r[1]} tin"
)

# ===== Ghép tất cả vào 1 nội dung =====
content = f"""📊 **BÁO CÁO TUYỂN DỤNG IT — {date.today().strftime('%d/%m/%Y')}**

🆕 [Data Engineer] Tổng số tin tuyển dụng: **{total_de_jobs}**

🛠️ [Data Engineer] Top kỹ năng được yêu cầu nhiều nhất:
{skill_lines}

🧑‍💼 [Toàn ngành IT] Phân bố theo cấp bậc:
{seniority_lines}

💰 [Toàn ngành IT] Lương trung bình (VNĐ): **{salary_min:,.0f} — {salary_max:,.0f}**

📍 [Toàn ngành IT] Top 5 thành phố nhiều tin tuyển dụng nhất:
{city_lines}

📈 [Toàn ngành IT] Top vị trí liên quan Data (theo số tin & KN trung bình):
{most_cv_data_lines}

🔥 [Toàn ngành IT] Xu hướng tuyển dụng theo vị trí:
{trend_hiring_lines}
"""

# ===== Gửi lên Discord =====
payload = {"content": content}
response = requests.post(WEBHOOK_URL, json=payload)

if response.status_code == 204:
    print("Gửi thành công!")
else:
    print(f"Thất bại: {response.status_code}, {response.text}")

cursor.close()
conn.close()