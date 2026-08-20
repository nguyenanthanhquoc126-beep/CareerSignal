import csv
import json
import os
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag
from logging_config import logging, set_up_log
from playwright.sync_api import (
    BrowserContext,
    Locator,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

set_up_log()


HOME_URL = "https://itviec.com/"
SEARCH_URL = "https://itviec.com/it-jobs"
OUTPUT_FILE = "ITViec_jobs.csv"
TIMEOUT = 60_000
REQUEST_DELAY_MS = 5_000
MAX_REQUEST_ATTEMPTS = 5
DEFAULT_RETRY_DELAY_SECONDS = 30
MAX_RETRY_DELAY_SECONDS = 60

CSV_FIELDS = [
    "job_id",
    "slug",
    "title",
    "job_url",
    "company_name",
    "company_url",
    "company_logo",
    "salary",
    "job_category",
    "working_model",
    "location",
    "skills",
    "benefits",
    "label",
    "posted_at",
    "source_page",
    "scraped_at",
]


def load_local_env() -> None:
    """Load the project's .env without overwriting exported variables."""

    env_file = Path(__file__).resolve().parents[1] / ".env"

    if not env_file.is_file():
        return

    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line.removeprefix("export ").lstrip()

        key, separator, value = line.partition("=")

        if not separator:
            continue

        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]

        if key:
            os.environ.setdefault(key, value)


def get_required_env(*names: str) -> str:
    """Return the first configured environment variable in ``names``."""

    for name in names:
        value = os.getenv(name)

        if value and value.strip():
            return value

    expected_names = ", ".join(names)
    logging.error(
        "[ITViec] Thiếu biến môi trường bắt buộc; cần một trong: %s.",
        expected_names,
    )
    raise RuntimeError(
        f"Chua cau hinh bien moi truong. Can mot trong cac bien: "
        f"{expected_names}."
    )


def first_visible(locator: Locator, description: str) -> Locator:
    """Return the first visible desktop/mobile variant of a locator."""

    for index in range(locator.count()):
        candidate = locator.nth(index)

        if candidate.is_visible():
            return candidate

    raise RuntimeError(f"Khong tim thay phan tu dang hien thi: {description}.")


def open_sign_in_page(page: Page) -> None:
    """Open ITviec and follow its real Sign In link."""

    page.goto(HOME_URL, wait_until="domcontentloaded", timeout=TIMEOUT)

    sign_in_link = first_visible(
        page.locator('a[href^="/sign_in"]').filter(has_text="Sign In"),
        "link Sign In",
    )

    sign_in_link.click()
    page.wait_for_url("**/sign_in**", timeout=TIMEOUT)
    page.wait_for_load_state("domcontentloaded")


def wait_for_logged_in_page(page: Page) -> None:
    """Wait until ITviec has left the sign-in page and finished loading."""

    page.wait_for_url(
        lambda url: urlsplit(url).path.rstrip("/") != "/sign_in",
        timeout=TIMEOUT,
    )
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_load_state("load")

    # Analytics or notification connections can keep the network busy.  Wait
    # for networkidle when possible, but do not reject an otherwise completed
    # login only because a background request remains open.
    try:
        page.wait_for_load_state("networkidle", timeout=10_000)
    except PlaywrightTimeoutError:
        pass


def sign_in(page: Page, user_name: str, password: str) -> None:
    """Sign in with email and wait for the authenticated page to be ready."""

    open_sign_in_page(page)

    email_input = first_visible(
        page.locator('input[type="email"]'),
        "o Email",
    )
    password_input = first_visible(
        page.locator('input[type="password"]'),
        "o Password",
    )
    sign_in_button = first_visible(
        page.get_by_role("button", name="Sign In with Email", exact=True),
        "nut Sign In with Email",
    )

    email_input.fill(user_name)
    password_input.fill(password)
    sign_in_button.click()

    try:
        wait_for_logged_in_page(page)
    except PlaywrightTimeoutError as error:
        error_message = page.locator(
            '[role="alert"], .alert, .invalid-feedback, .text-danger'
        ).filter(visible=True)
        details = error_message.first.text_content() if error_message.count() else ""
        suffix = f" Chi tiet: {details.strip()}" if details else ""
        raise RuntimeError(
            "Dang nhap ITviec khong thanh cong hoac trang chuyen huong "
            f"qua thoi gian cho.{suffix}"
        ) from error


def normalize_text(element: Tag | None) -> str:
    """Return an element's text with repeated whitespace collapsed."""

    if element is None:
        return ""

    return " ".join(element.stripped_strings)


def canonical_url(url: str | None) -> str:
    """Convert an ITviec link to an absolute URL without tracking params."""

    if not url:
        return ""

    parsed_url = urlsplit(urljoin(HOME_URL, url))
    return urlunsplit(
        (parsed_url.scheme, parsed_url.netloc, parsed_url.path, "", "")
    )


def build_search_url(page_number: int) -> str:
    """Build the AJAX search URL, starting explicitly from page 1."""

    if page_number < 1:
        raise ValueError("page_number phai lon hon hoac bang 1.")

    query = urlencode(
        {
            "query": "",
            "source": "search_job",
            "page": page_number,
        }
    )
    return f"{SEARCH_URL}?{query}"


def get_ajax_headers(page: Page) -> dict[str, str]:
    """Create the useful headers from the cURL using the live CSRF token."""

    headers = {
        "accept": (
            "text/javascript, application/javascript, "
            "application/ecmascript, application/x-ecmascript, */*; q=0.01"
        ),
        "referer": SEARCH_URL,
        "x-requested-with": "XMLHttpRequest",
    }

    csrf_token = page.locator('meta[name="csrf-token"]').get_attribute("content")

    if csrf_token:
        headers["x-csrf-token"] = csrf_token

    return headers


def request_search_page(
    context: BrowserContext,
    headers: dict[str, str],
    page_number: int,
) -> dict:
    """Request one JSON page with the authenticated browser cookie jar."""

    response = None

    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        response = context.request.get(
            build_search_url(page_number),
            headers=headers,
            timeout=TIMEOUT,
        )

        if response.ok:
            break

        retryable_statuses = {429, 500, 502, 503, 504}

        if (
            response.status not in retryable_statuses
            or attempt == MAX_REQUEST_ATTEMPTS
        ):
            raise RuntimeError(
                f"Request trang {page_number} that bai, HTTP {response.status}."
            )

        retry_after = response.headers.get("retry-after", "")
        retry_delay = DEFAULT_RETRY_DELAY_SECONDS * attempt

        if retry_after.isdigit():
            retry_delay = int(retry_after)
        elif retry_after:
            try:
                retry_time = parsedate_to_datetime(retry_after)
                retry_delay = max(
                    1,
                    int((retry_time - datetime.now(timezone.utc)).total_seconds()),
                )
            except (TypeError, ValueError, OverflowError):
                pass

        retry_delay = min(retry_delay, MAX_RETRY_DELAY_SECONDS)
        logging.info(
            "[ITViec] Trang %s trả HTTP %s; sẽ thử lại lần %s/%s "
            "sau %s giây. URL=%s",
            page_number,
            response.status,
            attempt + 1,
            MAX_REQUEST_ATTEMPTS,
            retry_delay,
            build_search_url(page_number),
        )
        time.sleep(retry_delay)

    if response is None:
        raise RuntimeError(f"Khong nhan duoc response trang {page_number}.")

    try:
        response_data = response.json()
    except Exception as error:
        raise RuntimeError(
            f"Response trang {page_number} khong phai JSON hop le."
        ) from error

    if not isinstance(response_data, dict):
        raise RuntimeError(
            f"Response trang {page_number} khong phai mot dictionary."
        )

    if not isinstance(response_data.get("jobs_html"), str):
        raise RuntimeError(f"Response trang {page_number} thieu jobs_html.")

    if not isinstance(response_data.get("pagination_html"), str):
        raise RuntimeError(f"Response trang {page_number} thieu pagination_html.")

    return response_data


def get_total_pages(pagination_html: str) -> int:
    """Extract the greatest page number from pagination anchor hrefs."""

    soup = BeautifulSoup(pagination_html, "html.parser")
    page_numbers = {1}

    current_page = soup.select_one(".page.current")
    current_page_text = normalize_text(current_page)

    if current_page_text.isdigit():
        page_numbers.add(int(current_page_text))

    for link in soup.select("a[href]"):
        query = parse_qs(urlsplit(link.get("href", "")).query)
        query_page = query.get("page", [])

        if query_page and query_page[0].isdigit():
            page_numbers.add(int(query_page[0]))
            continue

        link_text = normalize_text(link)

        if link_text.isdigit():
            page_numbers.add(int(link_text))

    return max(page_numbers)


def parse_job_card(
    card: Tag,
    page_number: int,
    scraped_at: str,
) -> dict[str, object]:
    """Parse every useful feature exposed by one job-card element."""

    title_link = card.select_one("h3 a[href]")
    company_link = next(
        (
            link
            for link in card.select('a[href^="/companies/"]')
            if normalize_text(link)
        ),
        None,
    )
    logo = card.select_one("a.logo-employer-card img")
    category_link = card.select_one("a.text-decoration-dot-underline")
    location_element = card.select_one("div.text-rich-grey[title]")

    working_model_element = None

    if location_element and location_element.parent:
        working_model_element = location_element.parent.select_one(
            "div.text-rich-grey.flex-shrink-0"
        )

    posted_at = normalize_text(
        card.select_one("span.small-text.text-dark-grey")
    )

    if posted_at.lower().startswith("posted "):
        posted_at = posted_at[7:].strip()

    skills = [
        normalize_text(skill)
        for skill in card.select('[data-responsive-tag-list-target="tag"]')
        if normalize_text(skill)
    ]
    benefits = [
        normalize_text(benefit)
        for benefit in card.select("div.small-text.text-it-black.fw-500 li")
        if normalize_text(benefit)
    ]

    return {
        "job_id": card.get("data-job-key", ""),
        "slug": card.get("data-search--job-selection-job-slug-value", ""),
        "title": normalize_text(title_link),
        "job_url": canonical_url(title_link.get("href") if title_link else None),
        "company_name": normalize_text(company_link),
        "company_url": canonical_url(
            company_link.get("href") if company_link else None
        ),
        "company_logo": (
            logo.get("data-src", logo.get("src", "")) if logo else ""
        ),
        "salary": normalize_text(card.select_one(".salary span")),
        "job_category": normalize_text(category_link),
        "working_model": normalize_text(working_model_element),
        "location": (
            location_element.get("title", normalize_text(location_element))
            if location_element
            else ""
        ),
        "skills": skills,
        "benefits": benefits,
        "label": normalize_text(card.select_one(".ilabel")),
        "posted_at": posted_at,
        "source_page": page_number,
        "scraped_at": scraped_at,
    }


def parse_jobs(
    jobs_html: str,
    page_number: int,
    scraped_at: str,
) -> list[dict[str, object]]:
    """Parse all job cards in jobs_html with BeautifulSoup."""

    soup = BeautifulSoup(jobs_html, "html.parser")
    cards = soup.select("div.job-card[data-job-key]")
    return [
        parse_job_card(card, page_number, scraped_at)
        for card in cards
    ]


def career_it_hcm(page: int | None = None) -> dict[str, dict[str, object]]:
    """Collect at most ``page`` result pages and return jobs by job ID."""

    if page is not None and page < 1:
        raise ValueError("page must be greater than or equal to 1.")

    page_limit = page

    logging.info(
        "[ITViec] Bắt đầu crawl việc làm từ %s; giới hạn trang=%s.",
        SEARCH_URL,
        "toàn bộ" if page_limit is None else page_limit,
    )

    load_local_env()
    user_name = get_required_env("USER_NAME", "user_name")
    password = get_required_env("PASS_WORD", "passwword", "password")
    scraped_at = datetime.now(timezone.utc).isoformat()
    all_jobs: dict[str, dict[str, object]] = {}

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context(locale="en-US")
        page = context.new_page()

        try:
            logging.info("[ITViec] Đang đăng nhập tại %s.", HOME_URL)
            sign_in(page, user_name, password)
            logging.info(
                "[ITViec] Đăng nhập thành công; trang hiện tại=%s.",
                page.url,
            )
            headers = get_ajax_headers(page)

            logging.info(
                "[ITViec] Đang crawl trang 1/chưa xác định tổng số trang. URL=%s",
                build_search_url(1),
            )
            first_page_data = request_search_page(context, headers, 1)
            total_pages = get_total_pages(first_page_data["pagination_html"])
            last_page = (
                total_pages
                if page_limit is None
                else min(page_limit, total_pages)
            )
            logging.info(
                "[ITViec] Kế hoạch crawl: %s/%s trang của %s.",
                last_page,
                total_pages,
                SEARCH_URL,
            )

            for page_number in range(1, last_page + 1):
                if page_number > 1:
                    logging.info(
                        "[ITViec] Đang crawl trang %s/%s. URL=%s",
                        page_number,
                        last_page,
                        build_search_url(page_number),
                    )

                response_data = (
                    first_page_data
                    if page_number == 1
                    else request_search_page(context, headers, page_number)
                )
                page_jobs = parse_jobs(
                    response_data["jobs_html"],
                    page_number,
                    scraped_at,
                )

                for job in page_jobs:
                    unique_key = str(job.get("job_id") or job.get("slug") or "")

                    if unique_key:
                        all_jobs[unique_key] = job

                logging.info(
                    "[ITViec] Hoàn tất trang %s/%s: lấy được %s job; "
                    "tổng job không trùng hiện tại=%s.",
                    page_number,
                    last_page,
                    len(page_jobs),
                    len(all_jobs),
                )

                if page_number < last_page:
                    page.wait_for_timeout(REQUEST_DELAY_MS)
            logging.info(
                "[ITViec] Hoàn tất crawl %s/%s trang: %s job không trùng.",
                last_page,
                total_pages,
                len(all_jobs),
            )
        finally:
            browser.close()

    return all_jobs


def to_csv(
    jobs: dict[str, dict[str, object]],
    output_file: str | Path = OUTPUT_FILE,
) -> None:
    """Write the dictionary returned by career_it_hcm to a UTF-8 CSV."""

    logging.info(
        "[ITViec] Đang lưu %s job vào file CSV: %s.",
        len(jobs),
        output_file,
    )

    with Path(output_file).open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=CSV_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()

        for job in jobs.values():
            row = job.copy()
            row["skills"] = json.dumps(job.get("skills", []), ensure_ascii=False)
            row["benefits"] = json.dumps(
                job.get("benefits", []),
                ensure_ascii=False,
            )
            writer.writerow(row)

    logging.info(
        "[ITViec] Đã lưu %s job vào file CSV: %s.",
        len(jobs),
        output_file,
    )


if __name__ == "__main__":
    itviec_jobs = career_it_hcm()
    to_csv(itviec_jobs, OUTPUT_FILE)
    logging.info(
        "[ITViec] Chương trình độc lập hoàn tất: %s job, file=%s.",
        len(itviec_jobs),
        OUTPUT_FILE,
    )
