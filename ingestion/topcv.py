import json
import re
from datetime import datetime
from urllib.parse import (
    parse_qs,
    parse_qsl,
    urlencode,
    urljoin,
    urlsplit,
    urlunsplit,
)

from bs4 import BeautifulSoup
from logging_config import logging, set_up_log
from playwright.sync_api import (
    Locator,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

set_up_log()


# ============================================================
# 1. CẤU HÌNH CHUNG
# ============================================================

HOME_URL = "https://www.topcv.vn/"

# Đường dẫn ngành Công nghệ thông tin trên toàn quốc.
# Không còn phần:
#   -tai-ho-chi-minh-l2cr257
IT_SEARCH_PATH = "/tim-viec-lam-cong-nghe-thong-tin-cr257"

# Các query parameter đúng theo cURL người dùng đã cung cấp.
#
# Lưu ý:
# - Không có locations=l2, nên không lọc Hồ Chí Minh.
# - Vẫn giữ company_type=1 và saturday_status=0 để khớp đúng cURL.
IT_SEARCH_PARAMS = {
    "type_keyword": "1",
    "category_family": "r257",
    "saturday_status": "0",
}

# Thời gian chờ tối đa của Playwright, tính bằng mili giây.
TIMEOUT = 60_000

# Nghỉ giữa hai request phân trang.
REQUEST_DELAY_MS = 500


def first_visible(locator: Locator, description: str) -> Locator:
    """Return the visible desktop/mobile variant of a locator."""

    for index in range(locator.count()):
        candidate = locator.nth(index)

        if candidate.is_visible():
            return candidate

    raise RuntimeError(
        f"Khong tim thay phan tu dang hien thi: {description}."
    )


def accept_cookie_banner(page: Page) -> None:
    """Close the cookie banner when it covers the navigation menu."""

    accept_buttons = page.get_by_role(
        "button",
        name="Chấp nhận tất cả",
        exact=True,
    )

    for index in range(accept_buttons.count()):
        button = accept_buttons.nth(index)

        if button.is_visible():
            button.click()
            logging.info("[TopCV] Đã đóng thông báo cookie.")
            return


# ============================================================
# 2. KIỂM TRA RESPONSE CÓ PHẢI RESPONSE IT CẦN LẤY KHÔNG
# ============================================================

def is_it_search_response(response):
    """
    Chỉ nhận response trang đầu của bộ dữ liệu:

        Công nghệ thông tin
        + không lọc địa điểm
        + company_type=1
        + saturday_status=0

    Điều kiện:
    1. Request là POST.
    2. Request là Fetch/XHR.
    3. Đúng đường dẫn /tim-viec-lam-cong-nghe-thong-tin-cr257.
    4. Các query parameter khớp với cURL.
    5. Không có locations hoặc exp.
    6. Là trang đầu: không có page hoặc page=1.
    """

    request = response.request

    if request.method != "POST":
        return False

    if request.resource_type not in {"xhr", "fetch"}:
        return False

    parsed_url = urlsplit(response.url)

    request_path = parsed_url.path.rstrip("/")
    expected_path = IT_SEARCH_PATH.rstrip("/")

    if request_path != expected_path:
        return False

    query = parse_qs(
        parsed_url.query,
        keep_blank_values=True,
    )

    for parameter_name, expected_value in IT_SEARCH_PARAMS.items():
        if query.get(parameter_name) != [expected_value]:
            return False

    # u_sr_id is dynamic tracking data. Validate its presence without
    # hard-coding the value from a single browser session.
    tracking_values = query.get("u_sr_id", [])

    if len(tracking_values) != 1 or not tracking_values[0].strip():
        return False

    # Match the supplied cURL: fixed search parameters + page + u_sr_id.
    allowed_parameters = set(IT_SEARCH_PARAMS) | {"page", "u_sr_id"}

    if set(query) != allowed_parameters:
        return False

    # Không lấy response có lọc địa điểm hoặc kinh nghiệm.
    if "locations" in query:
        return False

    if "exp" in query:
        return False

    # Response đầu phải là trang 1.
    if query.get("page") != ["1"]:
        return False

    return True


# ============================================================
# 3. LẤY HEADER CẦN THIẾT TỪ REQUEST THẬT
# ============================================================

def get_reusable_headers(original_request):
    """
    Lấy những header quan trọng từ request trình duyệt
    đã gửi thành công.

    Cookie không được chép thủ công vì context.request dùng chung
    cookie jar với BrowserContext.
    """

    original_headers = original_request.all_headers()

    reusable_headers = {}

    header_names = [
        "accept",
        "accept-language",
        "content-type",
        "origin",
        "referer",
        "user-agent",
        "x-requested-with",
        "x-csrf-token",
        "x-xsrf-token",
    ]

    for header_name in header_names:
        header_value = original_headers.get(header_name)

        if header_value is not None:
            reusable_headers[header_name] = header_value

    return reusable_headers


# ============================================================
# 4. THAY SỐ TRANG TRONG URL
# ============================================================

def set_page_number(url, page_number):
    """
    Giữ nguyên toàn bộ query parameter của request thật
    và chỉ thay tham số page.

    Ví dụ:
        URL trang 1 không có page
        -> thêm page=2

        URL đã có page=2
        -> thay bằng page=3
    """

    if page_number < 1:
        raise ValueError(
            "page_number phải lớn hơn hoặc bằng 1."
        )

    parsed_url = urlsplit(url)

    query_pairs = parse_qsl(
        parsed_url.query,
        keep_blank_values=True,
    )

    new_query_pairs = []
    page_was_replaced = False

    for key, value in query_pairs:
        if key == "page":
            if not page_was_replaced:
                new_query_pairs.append(
                    ("page", str(page_number))
                )
                page_was_replaced = True
            continue

        new_query_pairs.append((key, value))

    if not page_was_replaced:
        new_query_pairs.append(
            ("page", str(page_number))
        )

    new_query = urlencode(
        new_query_pairs,
        doseq=True,
    )

    return urlunsplit(
        (
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            new_query,
            parsed_url.fragment,
        )
    )


def build_it_search_url(page_number, tracking_id):
    """Build the POST URL in the same order as the captured cURL."""

    if page_number < 1:
        raise ValueError("page_number must be at least 1.")

    if not tracking_id:
        raise ValueError("tracking_id must not be empty.")

    query = urlencode(
        [
            ("type_keyword", IT_SEARCH_PARAMS["type_keyword"]),
            ("page", str(page_number)),
            (
                "category_family",
                IT_SEARCH_PARAMS["category_family"],
            ),
            (
                "saturday_status",
                IT_SEARCH_PARAMS["saturday_status"],
            ),
            ("u_sr_id", tracking_id),
        ]
    )

    return urljoin(HOME_URL, IT_SEARCH_PATH) + "?" + query


def get_it_api_session(page):
    """Read the dynamic CSRF token and tracking id from the loaded page."""

    csrf_token = page.locator(
        'meta[name="csrf-token"]'
    ).get_attribute("content")

    if not csrf_token:
        raise RuntimeError(
            "Khong tim thay CSRF token tren trang TopCV."
        )

    job_cards = page.locator(
        "div.job-item-search-result[data-u-sr-id]"
    )

    if job_cards.count() == 0:
        raise RuntimeError(
            "Khong tim thay job card de lay u_sr_id."
        )

    tracking_id = job_cards.nth(0).get_attribute(
        "data-u-sr-id"
    )

    if not tracking_id:
        raise RuntimeError(
            "Job card khong co data-u-sr-id."
        )

    parsed_page_url = urlsplit(page.url)
    origin = urlunsplit(
        (
            parsed_page_url.scheme,
            parsed_page_url.netloc,
            "",
            "",
            "",
        )
    ).rstrip("/")

    headers = {
        "accept": "*/*",
        "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "origin": origin,
        "referer": page.url,
        "x-csrf-token": csrf_token,
        "x-requested-with": "XMLHttpRequest",
    }

    return tracking_id, headers


# ============================================================
# 5. HÀM HỖ TRỢ BEAUTIFULSOUP
# ============================================================

def clean_text(element):
    """
    Lấy chữ trong một phần tử HTML và chuẩn hóa khoảng trắng.
    """

    if element is None:
        return None

    text = element.get_text(
        separator=" ",
        strip=True,
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    )

    return text or None


def get_text(parent, selector):
    """
    Tìm phần tử con đầu tiên khớp CSS selector rồi lấy chữ.
    """

    element = parent.select_one(selector)

    return clean_text(element)


def get_attribute(
    parent,
    selector,
    attribute,
):
    """
    Tìm phần tử con rồi lấy một thuộc tính HTML.
    """

    element = parent.select_one(selector)

    if element is None:
        return None

    value = element.get(attribute)

    if not isinstance(value, str):
        return None

    value = value.strip()

    return value or None


def make_absolute_url(url):
    """
    Chuyển URL tương đối thành URL đầy đủ.
    """

    if not url:
        return None

    return urljoin(
        HOME_URL,
        url,
    )


# ============================================================
# 6. BÓC DỮ LIỆU CỦA MỘT CÔNG VIỆC
# ============================================================

def parse_job_card(job_card):
    """
    Nhận một thẻ:

        <div class="job-item-search-result">...</div>

    rồi chuyển thành dictionary Python.
    """

    job_classes = job_card.get(
        "class",
        [],
    )

    if not isinstance(job_classes, list):
        job_classes = []

    # --------------------------------------------------------
    # 6.1. Tên công việc
    # --------------------------------------------------------

    title_element = job_card.select_one(
        "h3.title a[href] span[title]"
    )

    title = None

    if title_element is not None:
        # Thuộc tính title thường chứa tên đầy đủ.
        title_attribute = title_element.get(
            "title"
        )

        if isinstance(title_attribute, str):
            title_attribute = title_attribute.strip()

            if title_attribute:
                title = title_attribute

        if title is None:
            title = clean_text(title_element)

    if title is None:
        title = get_attribute(
            job_card,
            ".avatar > a",
            "aria-label",
        )

    # --------------------------------------------------------
    # 6.2. Link chi tiết công việc
    # --------------------------------------------------------

    job_url = get_attribute(
        job_card,
        "h3.title a[href]",
        "href",
    )

    if job_url is None:
        job_url = get_attribute(
            job_card,
            ".avatar > a[href]",
            "href",
        )

    job_url = make_absolute_url(job_url)

    # --------------------------------------------------------
    # 6.3. Tên và link công ty
    # --------------------------------------------------------

    company_name = get_text(
        job_card,
        ".company-name",
    )

    if company_name is None:
        company_name = get_attribute(
            job_card,
            ".avatar img",
            "alt",
        )

    company_url = get_attribute(
        job_card,
        "a.company[href]",
        "href",
    )

    company_url = make_absolute_url(
        company_url
    )

    # --------------------------------------------------------
    # 6.4. Logo công ty
    # --------------------------------------------------------

    logo_element = job_card.select_one(
        ".avatar img"
    )

    logo_url = None

    if logo_element is not None:
        logo_url = (
            logo_element.get("data-src")
            or logo_element.get("src")
        )

        if isinstance(logo_url, str):
            logo_url = make_absolute_url(
                logo_url.strip()
            )
        else:
            logo_url = None

    # --------------------------------------------------------
    # 6.5. Lương
    # --------------------------------------------------------

    salary = get_text(
        job_card,
        ".info .salary span",
    )

    if salary is None:
        salary = get_text(
            job_card,
            ".title-salary",
        )

    # --------------------------------------------------------
    # 6.6. Địa điểm và kinh nghiệm
    # --------------------------------------------------------

    # Không dùng địa điểm làm bộ lọc nữa,
    # nhưng vẫn lưu địa điểm của từng công việc.
    city = get_text(
        job_card,
        ".city-text",
    )

    experience = get_text(
        job_card,
        "label.exp span",
    )

    # --------------------------------------------------------
    # 6.7. Nhãn: Tin mới, GẤP...
    # --------------------------------------------------------

    labels = []

    for label_element in job_card.select(
        ".box-label-top .label"
    ):
        label_text = clean_text(
            label_element
        )

        if label_text:
            labels.append(label_text)

    # --------------------------------------------------------
    # 6.8. Tag đang hiển thị
    # --------------------------------------------------------

    visible_tags = []


    for tag_element in job_card.select(
        ".tag > .item-tag"
    ):
        tag_text = clean_text(
            tag_element
        )

        if tag_text:
            visible_tags.append(tag_text)

    # --------------------------------------------------------
    # 6.9. Tag bị thu gọn trong +7, +15...
    # --------------------------------------------------------

    remaining_tags = get_attribute(
        job_card,
        ".tag > .remaining-items",
        "title",
    )

    # --------------------------------------------------------
    # 6.10. Thời gian đăng/cập nhật
    # --------------------------------------------------------

    posted_time = get_text(
        job_card,
        "label.label-update",
    )

    updated_time = get_attribute(
        job_card,
        "label.label-update",
        "title",
    )

    # --------------------------------------------------------
    # 6.11. Nút ứng tuyển
    # --------------------------------------------------------

    apply_button = job_card.select_one(
        "button.btn-apply-now"
    )

    apply_text = None
    apply_url = None

    if apply_button is not None:
        apply_text = clean_text(
            apply_button
        )

        apply_url = (
            apply_button.get("data-apply-url")
            or apply_button.get(
                "data-redirect-to"
            )
        )

        if isinstance(apply_url, str):
            apply_url = make_absolute_url(
                apply_url.strip()
            )
        else:
            apply_url = None

    # --------------------------------------------------------
    # 6.12. Xác thực nhà tuyển dụng
    # --------------------------------------------------------

    verified_element = job_card.select_one(
        ".icon-verified-employer"
    )

    is_verified = (
        verified_element is not None
    )

    verification_level = None

    if verified_element is not None:
        verified_classes = (
            verified_element.get(
                "class",
                [],
            )
        )

        if isinstance(verified_classes, list):
            for class_name in verified_classes:
                if class_name.startswith(
                    "level-"
                ):
                    verification_level = (
                        class_name
                    )
                    break

    # --------------------------------------------------------
    # 6.13. Kết quả
    # --------------------------------------------------------

    return {
        # Nhận dạng.
        "job_id": job_card.get(
            "data-job-id"
        ),
        "position": job_card.get(
            "data-job-position"
        ),
        "tracking_id": job_card.get(
            "data-u-sr-id"
        ),
        "box_type": job_card.get(
            "data-box"
        ),

        # Công việc.
        "title": title,
        "job_url": job_url,
        "salary": salary,
        "city": city,
        "experience": experience,

        # Công ty.
        "company_name": company_name,
        "company_url": company_url,
        "logo_url": logo_url,

        # Tag và nhãn.
        "labels": labels,
        "visible_tags": visible_tags,
        "remaining_tags": remaining_tags,

        # Thời gian.
        "posted_time": posted_time,
        "updated_time": updated_time,

        # Ứng tuyển.
        "apply_text": apply_text,
        "apply_url": apply_url,

        # Trạng thái.
        "is_hot": (
            job_card.select_one(
                ".is-hot-job"
            )
            is not None
        ),
        "is_urgent": (
            job_card.select_one(
                ".is-urgent"
            )
            is not None
        ),
        "is_pro_company": (
            job_card.select_one(
                ".job-pro-icon"
            )
            is not None
        ),
        "is_verified": is_verified,
        "verification_level": (
            verification_level
        ),
        "is_highlight": (
            "bg-highlight"
            in job_classes
        ),
        "is_flash_job": (
            "bg-flash-job"
            in job_classes
        ),
        "is_diamond_employer": (
            "bg-diamond-employer"
            in job_classes
            or job_card.select_one(
                ".tag-diamond-employer"
            )
            is not None
        ),
    }


# ============================================================
# 7. BÓC TẤT CẢ CÔNG VIỆC TRONG RESPONSE
# ============================================================

def parse_jobs(response_data):
    """
    Response TopCV có dạng:

        {
            "status": "success",
            "data": {
                "html_job": "<div>...</div>"
            }
        }

    BeautifulSoup chỉ xử lý data["html_job"].
    """

    if not isinstance(response_data, dict):
        raise TypeError(
            "response_data phải là dictionary."
        )

    if response_data.get("status") != "success":
        raise RuntimeError(
            "TopCV trả trạng thái thất bại: "
            f"{response_data.get('message')}"
        )

    data = response_data.get("data")

    if not isinstance(data, dict):
        raise RuntimeError(
            "Response không có data hợp lệ."
        )

    html_job = data.get("html_job")

    if not isinstance(html_job, str):
        raise RuntimeError(
            "Response không có html_job hợp lệ."
        )

    soup = BeautifulSoup(
        html_job,
        "html.parser",
    )

    job_cards = soup.select(
        "div.job-list-search-result "
        "> div.job-item-search-result"
    )

    jobs = []

    for job_card in job_cards:
        jobs.append(
            parse_job_card(job_card)
        )

    return jobs


# ============================================================
# 8. ĐỌC VÀ KIỂM TRA METADATA TRONG JSON
# ============================================================

def get_response_info(response_data):
    """
    Đọc các trường JSON thuần:

        total
        total_page
        current_page

    Không cần dùng BeautifulSoup cho các trường này.
    """

    if not isinstance(response_data, dict):
        raise TypeError(
            "response_data phải là dictionary."
        )

    if response_data.get("status") != "success":
        raise RuntimeError(
            "TopCV trả trạng thái thất bại: "
            f"{response_data.get('message')}"
        )

    data = response_data.get("data")

    if not isinstance(data, dict):
        raise RuntimeError(
            "Response không có data hợp lệ."
        )

    try:
        total = int(
            data.get("total", 0)
        )

        total_page = int(
            data.get("total_page", 0)
        )

        current_page = int(
            data.get("current_page", 0)
        )

    except (TypeError, ValueError) as error:
        raise RuntimeError(
            "Metadata phân trang không hợp lệ."
        ) from error

    if total_page < 1:
        raise RuntimeError(
            "total_page phải lớn hơn hoặc bằng 1."
        )

    if current_page < 1:
        raise RuntimeError(
            "current_page phải lớn hơn hoặc bằng 1."
        )

    return {
        "total": total,
        "total_page": total_page,
        "current_page": current_page,
    }


# ============================================================
# 9. ĐỌC SỐ TRANG TRÊN GIAO DIỆN
# ============================================================

def get_total_pages_from_dom(page):
    """
    Đọc dòng ví dụ:

        1 / 9 trang

    trên giao diện.
    """

    possible_locators = [
        page.locator(
            "#job-listing-paginate-text"
        ),
        page.get_by_text(
            re.compile(
                r"\d+\s*/\s*\d+\s*trang",
                re.IGNORECASE,
            )
        ),
    ]

    for locator in possible_locators:
        count = locator.count()

        for index in range(count):
            element = locator.nth(index)

            if not element.is_visible():
                continue

            text = element.inner_text().strip()

            match = re.search(
                r"\d+\s*/\s*(\d+)\s*trang",
                text,
                re.IGNORECASE,
            )

            if match:
                return int(match.group(1))

    return None


# ============================================================
# 10. HÀM CHÍNH: THU THẬP VIỆC LÀM IT TOÀN QUỐC
# ============================================================

def career_it(page: int | None = None) -> list[dict[str, object]]:
    """
    Luồng chính:

    1. Mở trang chủ TopCV.
    2. Hover menu Việc làm.
    3. Click Việc làm IT và chờ trang tìm kiếm tải xong.
    4. Đọc CSRF token và u_sr_id động từ trang.
    5. Chủ động POST API trang 1 bằng session của BrowserContext.
    6. Đọc JSON trang đầu.
    7. Bóc html_job bằng BeautifulSoup.
    8. Dùng URL request thật để lấy tối đa ``page`` trang.
    9. Kiểm tra current_page.
    10. Loại trùng và trả về toàn bộ kết quả trong biến career.

    Nếu ``page`` là None, hàm cào toàn bộ số trang server trả về.
    Số trang thực tế luôn bằng ``min(page, total_pages)``.

    Phần hover được giữ nguyên như code cũ.
    """

    if page is not None and page < 1:
        raise ValueError("page phải lớn hơn hoặc bằng 1.")

    page_limit = page

    logging.info(
        "[TopCV] Bắt đầu crawl việc làm từ %s%s; giới hạn trang=%s.",
        HOME_URL.rstrip("/"),
        IT_SEARCH_PATH,
        "toàn bộ" if page_limit is None else page_limit,
    )

    career: list[dict[str, object]] = []
    all_jobs: dict[str, dict[str, object]] = {}

    with sync_playwright() as playwright:

        browser = playwright.chromium.launch(
            headless=False
        )

        context = browser.new_context(
            locale="vi-VN",
        )

        page = context.new_page()

        try:
            # ------------------------------------------------
            # 10.1. Mở trang chủ TopCV
            # ------------------------------------------------

            page.goto(
                HOME_URL,
                wait_until="domcontentloaded",
                timeout=TIMEOUT,
            )

            accept_cookie_banner(page)

            logging.info(
                "[TopCV] Đã mở trang chủ: %s.",
                page.url,
            )

            # ------------------------------------------------
            # 10.2. Hover menu “Việc làm”
            # ------------------------------------------------

            job_menu = first_visible(
                page.locator(
                    "nav a[href='/viec-lam'], "
                    "nav a[href='https://www.topcv.vn/viec-lam']"
                ),
                "menu Viec lam",
            )

            job_menu.hover()

            logging.info("[TopCV] Đã mở menu Việc làm.")

            # ------------------------------------------------
            # 10.3. Tìm link “Việc làm IT”
            # ------------------------------------------------

            it_job_link = first_visible(
                page.locator(
                    "nav a[href='/viec-lam-it'], "
                    "nav a[href='https://www.topcv.vn/viec-lam-it']"
                ),
                "link Viec lam IT",
            )

            it_job_link.wait_for(
                state="visible",
                timeout=TIMEOUT,
            )

            # ------------------------------------------------
            # 10.4. Vào trang IT rồi chủ động gọi POST JSON
            # ------------------------------------------------
            it_job_link.click()

            # Đợi URL trang chính chuyển sang ngành IT.
            page.wait_for_url(
                re.compile(
                    r"tim-viec-lam-cong-nghe-thong-tin-cr257"
                ),
                timeout=TIMEOUT,
            )

            page.wait_for_load_state(
                "domcontentloaded"
            )

            first_job_card = page.locator(
                "div.job-item-search-result[data-u-sr-id]"
            ).nth(0)

            first_job_card.wait_for(
                state="attached",
                timeout=TIMEOUT,
            )

            tracking_id, reusable_headers = (
                get_it_api_session(page)
            )

            first_request_url = build_it_search_url(
                1,
                tracking_id,
            )

            # This request shares cookies with the browser context.
            logging.info(
                "[TopCV] Đang crawl trang 1/chưa xác định tổng số trang. URL=%s",
                first_request_url,
            )
            first_response = context.request.post(
                first_request_url,
                headers=reusable_headers,
                timeout=TIMEOUT,
            )

            logging.info(
                "[TopCV] Đã vào trang Việc làm IT: giao diện=%s; "
                "response API trang 1 có HTTP %s.",
                page.url,
                first_response.status,
            )

            # ------------------------------------------------
            # 10.5. Kiểm tra response đầu
            # ------------------------------------------------

            if not first_response.ok:
                logging.error(
                    "[TopCV] Crawl trang 1 thất bại: HTTP %s, URL=%s, "
                    "response=%r.",
                    first_response.status,
                    first_request_url,
                    first_response.text()[:500],
                )
                return career

            try:
                first_data = (
                    first_response.json()
                )

            except Exception as error:
                logging.error(
                    "[TopCV] Không thể đọc JSON trang 1: %s. "
                    "HTTP %s, URL=%s, response=%r.",
                    error,
                    first_response.status,
                    first_request_url,
                    first_response.text()[:500],
                    exc_info=True,
                )
                return career

            first_info = get_response_info(
                first_data
            )

            total_jobs = first_info["total"]
            total_pages = (
                first_info["total_page"]
            )
            last_page = (
                total_pages
                if page_limit is None
                else min(page_limit, total_pages)
            )
            current_page = (
                first_info["current_page"]
            )

            if current_page != 1:
                raise RuntimeError(
                    "Response đầu không phải trang 1: "
                    f"current_page={current_page}."
                )

            logging.info(
                "[TopCV] Kế hoạch crawl: %s/%s trang, tổng %s việc làm; "
                "JSON hiện tại là trang %s.",
                last_page,
                total_pages,
                total_jobs,
                current_page,
            )

            # Chờ giao diện cập nhật.
            page.wait_for_timeout(1_000)

            dom_total_pages = (
                get_total_pages_from_dom(page)
            )

            logging.info(
                "[TopCV] Đối chiếu phân trang: giao diện=%s, JSON=%s.",
                dom_total_pages,
                total_pages,
            )

            # Nếu đọc được DOM thì hai số phải giống nhau.
            if (
                dom_total_pages is not None
                and dom_total_pages
                != total_pages
            ):
                raise RuntimeError(
                    "Số trang trên giao diện và JSON "
                    "không giống nhau.\n"
                    f"DOM={dom_total_pages}, "
                    f"JSON={total_pages}\n"
                    f"URL response={first_response.url}"
                )

            # ------------------------------------------------
            # 10.6. Bóc công việc trang đầu
            # ------------------------------------------------

            first_page_jobs = parse_jobs(
                first_data
            )

            scraped_at = datetime.now().isoformat(
                timespec="seconds"
            )

            for job in first_page_jobs:
                job["source_page"] = 1
                job["scraped_at"] = scraped_at

                unique_key = (
                    job.get("job_id")
                    or job.get("job_url")
                )

                if unique_key:
                    all_jobs[str(unique_key)] = (
                        job
                    )

            logging.info(
                "[TopCV] Hoàn tất trang 1/%s: HTTP %s, lấy được %s job; "
                "tổng job không trùng hiện tại=%s.",
                last_page,
                first_response.status,
                len(first_page_jobs),
                len(all_jobs),
            )

            # ------------------------------------------------
            # 10.7. Lấy header và URL từ request thật
            # ------------------------------------------------

            logging.info(
                "[TopCV] Các header dùng lại cho API: %s.",
                ", ".join(reusable_headers),
            )

            # ------------------------------------------------
            # 10.8. Lấy trang 2 đến giới hạn đã yêu cầu
            # ------------------------------------------------

            for page_number in range(
                2,
                last_page + 1,
            ):
                page_url = set_page_number(
                    first_request_url,
                    page_number,
                )

                logging.info(
                    "[TopCV] Đang crawl trang %s/%s. URL=%s",
                    page_number,
                    last_page,
                    page_url,
                )

                response = context.request.post(
                    page_url,
                    headers=reusable_headers,
                    timeout=TIMEOUT,
                )

                if not response.ok:
                    logging.error(
                        "[TopCV] Crawl trang %s/%s thất bại: HTTP %s, "
                        "URL=%s, response=%r.",
                        page_number,
                        last_page,
                        response.status,
                        page_url,
                        response.text()[:500],
                    )
                    continue

                try:
                    response_data = (
                        response.json()
                    )

                except Exception as error:
                    logging.error(
                        "[TopCV] Không thể đọc JSON trang %s/%s: %s. "
                        "HTTP %s, URL=%s, response=%r.",
                        page_number,
                        last_page,
                        error,
                        response.status,
                        page_url,
                        response.text()[:500],
                        exc_info=True,
                    )
                    continue

                page_info = get_response_info(
                    response_data
                )

                if (
                    page_info["current_page"]
                    != page_number
                ):
                    raise RuntimeError(
                        f"Yêu cầu page={page_number}, "
                        "nhưng server trả "
                        f"current_page="
                        f"{page_info['current_page']}."
                    )

                if (
                    page_info["total_page"]
                    != total_pages
                ):
                    raise RuntimeError(
                        "total_page thay đổi giữa chừng: "
                        f"ban đầu={total_pages}, "
                        f"trang {page_number}="
                        f"{page_info['total_page']}."
                    )

                page_jobs = parse_jobs(
                    response_data
                )

                for job in page_jobs:
                    job["source_page"] = (
                        page_number
                    )
                    job["scraped_at"] = (
                        scraped_at
                    )

                    unique_key = (
                        job.get("job_id")
                        or job.get("job_url")
                    )

                    if unique_key:
                        all_jobs[
                            str(unique_key)
                        ] = job

                logging.info(
                    "[TopCV] Hoàn tất trang %s/%s: HTTP %s, lấy được %s job; "
                    "tổng job không trùng hiện tại=%s.",
                    page_number,
                    last_page,
                    response.status,
                    len(page_jobs),
                    len(all_jobs),
                )

                if page_number < last_page:
                    page.wait_for_timeout(
                        REQUEST_DELAY_MS
                    )

            # ------------------------------------------------
            # 10.9. Tổng hợp kết quả (không ghi file)
            # ------------------------------------------------

            career = list(
                all_jobs.values()
            )

            logging.info(
                "[TopCV] Kết thúc phạm vi crawl trang 1-%s trên tổng %s trang: "
                "%s job không trùng.",
                last_page,
                total_pages,
                len(career),
            )

        except PlaywrightTimeoutError as error:
            logging.error(
                "[TopCV] Playwright chờ quá thời gian tại URL=%s: %s.",
                page.url,
                error,
                exc_info=True,
            )

        except Exception as error:
            logging.error(
                "[TopCV] Crawl gặp lỗi tại URL=%s (%s: %s).",
                page.url,
                type(error).__name__,
                error,
                exc_info=True,
            )

        finally:
            browser.close()

    # Trả về toàn bộ job đã được biến đổi từ JSON. Nếu có lỗi sau khi đã
    # thu thập một phần dữ liệu, vẫn trả lại các job đã xử lý thành công.
    career = list(all_jobs.values())
    logging.info(
        "[TopCV] Kết thúc phiên crawl; trả về %s job đã xử lý.",
        len(career),
    )
    return career


# ============================================================
# 11. CHỈ CHẠY KHI FILE ĐƯỢC THỰC THI TRỰC TIẾP
# ============================================================

if __name__ == "__main__":
    career = career_it()