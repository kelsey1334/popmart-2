
import os
import io
import json
import base64
import asyncio
import logging
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("popmart-bot")

# ===== Config =====
# BASE_URL có thể là root (vd https://your-app) hoặc đã kèm /popmart (vd https://your-app/popmart)
BASE_URL = os.getenv("BASE_URL", "https://popmartstt.com").rstrip("/")
POP_PAGE_PATH = os.getenv("POP_PAGE_PATH", "/popmart").strip() or "/popmart"
AJAX_PATH = os.getenv("AJAX_PATH", "/Ajax.aspx").strip() or "/Ajax.aspx"

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMINS = [x.strip() for x in os.getenv("ADMINS", "").split(",") if x.strip()]
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_WORKERS_CAP = int(os.getenv("MAX_WORKERS", "10"))

# 2Captcha (optional)
TWO_CAPTCHA_API_KEY = os.getenv("TWO_CAPTCHA_API_KEY", "").strip()
USE_2CAPTCHA = os.getenv("USE_2CAPTCHA", "0").strip() == "1"
CAPTCHA_SOFT_TIMEOUT = int(os.getenv("CAPTCHA_SOFT_TIMEOUT", "120"))
CAPTCHA_POLL_INTERVAL = int(os.getenv("CAPTCHA_POLL_INTERVAL", "5"))
CAPTCHA_MAX_TRIES = int(os.getenv("CAPTCHA_MAX_TRIES", "4"))

# Cho phép chạy lại cùng ngày ở lần upload sau (mặc định: cho phép)
DISABLE_GLOBAL_DAY_DEDUP = os.getenv("DISABLE_GLOBAL_DAY_DEDUP", "1").strip() == "1"

# Anti-dup theo phiên đang chạy trong cùng thời điểm
ACTIVE_DAYS = set()
COMPLETED_DAYS = set()
ACTIVE_LOCK = threading.Lock()

# Pending manual captcha (nếu không dùng 2Captcha)
PENDING_CAPTCHAS: Dict[str, Dict[str, Any]] = {}
PENDING_LOCK = threading.Lock()

# ===== Env rows (optional) =====
# Cấu hình dữ liệu đầu vào qua biến môi trường:
# - POP_ROWS_JSON: JSON Array các object, ví dụ:
#   [
#     {"FullName":"A", "DOB_Day":"01","DOB_Month":"02","DOB_Year":"1999", "Phone":"090...", "Email":"a@x.com", "IDNumber":"00123"},
#     {"FullName":"B", ...}
#   ]
# - hoặc POP_ROWS_BASE64: Base64 của JSON như trên (để dễ set trên hosting)
# - hoặc POP_ROWS_CSV: nội dung CSV (header bắt buộc các cột như trên), mỗi dòng 1 record
POP_ROWS_JSON = os.getenv("POP_ROWS_JSON", "").strip()
POP_ROWS_BASE64 = os.getenv("POP_ROWS_BASE64", "").strip()
POP_ROWS_CSV = os.getenv("POP_ROWS_CSV", "").strip()


def _normalize_endpoints(base_url: str, pop_path: str, ajax_path: str):
    """
    Từ BASE_URL (root hoặc đã kèm /popmart) => tính:
      - page_url: URL trang form (…/popmart)
      - ajax_url: URL Ajax ở "root" (…/Ajax.aspx)
      - ajax_alt_url: Ajax fallback nằm cùng thư mục với page (…/popmart/Ajax.aspx)
      - root_base_url: origin + thư mục cha của page (để gọi /DangKy.aspx/GenQRImage)
    """
    sp = urlsplit(base_url)
    base_path = sp.path.rstrip("/")

    pop_path = "/" + pop_path.lstrip("/")
    ajax_path = "/" + ajax_path.lstrip("/")

    ends_with_pop = base_path.endswith(pop_path)
    page_path = base_path if ends_with_pop else (base_path + pop_path)
    if not page_path.startswith("/"):
        page_path = "/" + page_path

    # thư mục cha của page (root của app)
    root_path = base_path[:-len(pop_path)] if ends_with_pop else base_path
    if not root_path:
        root_path = "/"
    if not root_path.startswith("/"):
        root_path = "/" + root_path

    page_url = urlunsplit((sp.scheme, sp.netloc, page_path, "", ""))
    ajax_url = urlunsplit((sp.scheme, sp.netloc, (root_path.rstrip("/") + ajax_path), "", ""))
    page_dir = page_path.rsplit("/", 1)[0] or "/"
    ajax_alt_url = urlunsplit((sp.scheme, sp.netloc, (page_dir + ajax_path), "", ""))
    root_base_url = urlunsplit((sp.scheme, sp.netloc, root_path if root_path else "/", "", ""))

    return page_url, ajax_url, ajax_alt_url, root_base_url


class PopmartClient:
    def __init__(self, base_url: str, pop_path: str, ajax_path: str, timeout: int = 30):
        self.base_url = base_url
        (self.page_url,
         self.ajax_url,
         self.ajax_alt_url,
         self.root_base_url) = _normalize_endpoints(base_url, pop_path, ajax_path)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
        })
        self.timeout = timeout
        log.info(f"[ENDPOINTS] page={self.page_url} | ajax={self.ajax_url} | ajax_alt={self.ajax_alt_url} | root={self.root_base_url}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def get_main_page(self) -> str:
        r = self.session.get(self.page_url, timeout=self.timeout)
        r.raise_for_status()
        return r.text

    def _ajax_get(self, params: Dict[str, str]) -> requests.Response:
        r = self.session.get(self.ajax_url, params=params, timeout=self.timeout, allow_redirects=True)
        if r.status_code == 404:
            r2 = self.session.get(self.ajax_alt_url, params=params, timeout=self.timeout, allow_redirects=True)
            r2.raise_for_status()
            return r2
        r.raise_for_status()
        return r

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def load_sessions_for_day(self, id_ngay: str) -> List[Dict[str, str]]:
        r = self._ajax_get({"Action": "LoadPhien", "idNgayBanHang": id_ngay})
        html = r.text.split("||@@||")[0]
        soup = BeautifulSoup(html, "html.parser")
        return [{"value": (opt.get("value") or "").strip(), "label": (opt.text or "").strip()}
                for opt in soup.find_all("option")]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def fetch_captcha_image_url(self) -> Optional[str]:
        r = self._ajax_get({"Action": "LoadCaptcha"})
        soup = BeautifulSoup(r.text, "html.parser")
        img = soup.find("img")
        if img and img.get("src"):
            src = img["src"].strip()
            if src.startswith("http"):
                return src
            root = self.ajax_url.rsplit("/", 1)[0]
            return f"{root}/{src.lstrip('./')}"
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def download_image(self, url: str) -> bytes:
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.content

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def submit_registration(self, payload: Dict[str, str]) -> str:
        r = self._ajax_get(payload)
        return r.text.strip()

    # --- Extra endpoints to mirror real site ---
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def gen_qr_image(self, value: str, text: str) -> Optional[str]:
        """
        POST JSON: {"GiaTri":"<MaThamDu>", "NoiDungHienBenDuoi":"<MaThamDu>"}
        to /DangKy.aspx/GenQRImage -> returns {"d":"<url_png>"}
        """
        url = f"{self.root_base_url.rstrip('/')}/DangKy.aspx/GenQRImage"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        data = json.dumps({"GiaTri": value, "NoiDungHienBenDuoi": text})
        r = self.session.post(url, data=data, headers=headers, timeout=self.timeout)
        r.raise_for_status()
        jr = r.json()
        return jr.get("d")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), reraise=True)
    def send_email(self, id_phien: str, ma_tham_du: str) -> bool:
        r = self._ajax_get({"Action": "SendEmail", "idPhien": id_phien, "MaThamDu": ma_tham_du})
        return r.text.strip().lower() == "true"

    def map_sales_date_to_id(self, html: str, target_date: str) -> Optional[str]:
        soup = BeautifulSoup(html, "html.parser")
        sel = soup.find("select", {"id": "slNgayBanHang"})
        if not sel:
            return None
        for opt in sel.find_all("option"):
            if (opt.text or "").strip() == target_date:
                return (opt.get("value") or "").strip()
        return None


def extract_all_sales_dates(html: str) -> List[str]:
    out: List[str] = []
    soup = BeautifulSoup(html, "html.parser")
    sel = soup.find("select", {"id": "slNgayBanHang"})
    if not sel:
        return out
    for opt in sel.find_all("option"):
        txt = (opt.text or "").strip()
        val = (opt.get("value") or "").strip()
        if txt and val:  # skip placeholder
            out.append(txt)
    return out


def solve_captcha_via_2captcha(image_bytes: bytes) -> Optional[str]:
    if not TWO_CAPTCHA_API_KEY:
        return None
    try:
        import time
        b64 = base64.b64encode(image_bytes).decode("ascii")
        r = requests.post("https://2captcha.com/in.php",
                          data={"key": TWO_CAPTCHA_API_KEY, "method": "base64", "body": b64, "json": 1},
                          timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        if j.get("status") != 1 or "request" not in j:
            return None
        rid = j["request"]
        end_time = time.time() + CAPTCHA_SOFT_TIMEOUT
        while time.time() < end_time:
            time.sleep(CAPTCHA_POLL_INTERVAL)
            pr = requests.get("https://2captcha.com/res.php",
                              params={"key": TWO_CAPTCHA_API_KEY, "action": "get", "id": rid, "json": 1},
                              timeout=REQUEST_TIMEOUT)
            pr.raise_for_status()
            jr = pr.json()
            if jr.get("status") == 1:
                return str(jr.get("request", "")).strip()
        return None
    except Exception as e:
        log.warning(f"2Captcha error: {e}")
        return None


def is_session_full(text: str) -> bool:
    t = (text or "").lower()
    keys = [
        "đã hết số lượng đăng ký phiên này",
        "het so luong dang ky phien nay",
        "this session is full",
        "session is full",
    ]
    return any(k in t for k in keys)


def is_admin(uid: int) -> bool:
    return not ADMINS or str(uid) in ADMINS


def build_payload(id_ngay: str, id_phien: str, row: Dict[str, Any], captcha_text: str) -> Dict[str, str]:
    return {
        "Action": "DangKyThamDu",
        "idNgayBanHang": id_ngay,
        "idPhien": id_phien,
        "HoTen": str(row["FullName"]).strip(),
        "NgaySinh_Ngay": str(row["DOB_Day"]).strip(),
        "NgaySinh_Thang": str(row["DOB_Month"]).strip(),
        "NgaySinh_Nam": str(row["DOB_Year"]).strip(),
        "SoDienThoai": str(row["Phone"]).strip(),
        "Email": str(row["Email"]).strip(),
        "CCCD": str(row["IDNumber"]).strip(),
        "Captcha": captcha_text.strip(),
    }


# ======= Helpers: parse rows from ENV or Excel =======
REQUIRED_COLS = ["FullName", "DOB_Day", "DOB_Month", "DOB_Year", "Phone", "Email", "IDNumber"]


def parse_rows_from_env() -> List[Dict[str, Any]]:
    """Đọc dữ liệu đầu vào từ biến môi trường POP_ROWS_JSON / POP_ROWS_BASE64 / POP_ROWS_CSV."""
    # Prefer BASE64 > JSON > CSV
    if POP_ROWS_BASE64:
        try:
            raw = base64.b64decode(POP_ROWS_BASE64).decode("utf-8")
            data = json.loads(raw)
            if isinstance(data, list):
                return _normalize_rows_list(data)
        except Exception as e:
            log.error(f"POP_ROWS_BASE64 decode error: {e}")

    if POP_ROWS_JSON:
        try:
            data = json.loads(POP_ROWS_JSON)
            if isinstance(data, list):
                return _normalize_rows_list(data)
        except Exception as e:
            log.error(f"POP_ROWS_JSON parse error: {e}")

    if POP_ROWS_CSV:
        try:
            # Use pandas to parse CSV but force dtype=str to preserve leading zeros
            csv_df = pd.read_csv(io.StringIO(POP_ROWS_CSV), dtype=str)
            missing = [c for c in REQUIRED_COLS if c not in csv_df.columns]
            if missing:
                raise ValueError("CSV thiếu cột: " + ", ".join(missing))
            return csv_df.to_dict(orient="records")
        except Exception as e:
            log.error(f"POP_ROWS_CSV parse error: {e}")

    return []


def _normalize_rows_list(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Đảm bảo các khóa tồn tại, và mọi giá trị là string (giữ nguyên số 0 đầu)."""
    out: List[Dict[str, Any]] = []
    for r in rows:
        nr: Dict[str, Any] = {}
        for k in REQUIRED_COLS:
            v = r.get(k, "")
            # chuyển mọi thứ về string để không mất 0 đầu
            nr[k] = "" if v is None else str(v)
        out.append(nr)
    return out


# ======= Core run logic (shared by Excel + ENV) =======
async def run_with_rows(update: Update, context: ContextTypes.DEFAULT_TYPE, rows: List[Dict[str, Any]]):
    # Gắn chỉ số row
    for idx, r in enumerate(rows):
        r["__row_idx"] = idx

    client = PopmartClient(BASE_URL, POP_PAGE_PATH, AJAX_PATH, REQUEST_TIMEOUT)

    # Sales dates
    main_html = await asyncio.to_thread(client.get_main_page)
    all_days = extract_all_sales_dates(main_html)
    if not all_days:
        await update.message.reply_text("Không tìm thấy Sales Dates trên form.")
        return

    unique_days = list(dict.fromkeys(all_days))

    # Anti-dup scheduling
    days_to_run = []
    with ACTIVE_LOCK:
        for d in unique_days:
            if d in ACTIVE_DAYS:
                continue  # đang chạy ở 1 tác vụ khác
            if (not DISABLE_GLOBAL_DAY_DEDUP) and (d in COMPLETED_DAYS):
                continue  # chỉ chặn nếu bật dedup toàn cục
            ACTIVE_DAYS.add(d)
            days_to_run.append(d)

    if not days_to_run:
        await update.message.reply_text("Không có ngày nào mới để chạy (đã chạy trước đó).")
        return

    await update.message.reply_text(f"Tìm thấy {len(days_to_run)} ngày. Sẽ tạo {len(days_to_run)} task (mỗi ngày 1 task).")

    # Each day -> all rows
    buckets: Dict[str, List[Dict[str, Any]]] = {d: list(rows) for d in days_to_run}
    report_rows: List[Dict[str, Any]] = []
    report_lock = asyncio.Lock()

    async def add_report(**kw):
        async with report_lock:
            report_rows.append(kw)

    async def process_day(day: str, tasks: List[Dict[str, Any]]):
        try:
            html = await asyncio.to_thread(client.get_main_page)
            id_ngay = client.map_sales_date_to_id(html, day)
            if not id_ngay:
                await update.message.reply_text(f"[{day}] Không tìm thấy idNgàyBanHang.")
                for row in tasks:
                    await add_report(
                        Day=day, DayId="", SessionValue="", SessionLabel="",
                        Row=row["__row_idx"] + 1, FullName=row["FullName"],
                        DOB_Day=row["DOB_Day"], DOB_Month=row["DOB_Month"], DOB_Year=row["DOB_Year"],
                        Phone=row["Phone"], Email=row["Email"], IDNumber=row["IDNumber"],
                        Status="Failed", Attempts=0, Message="Không tìm thấy idNgàyBanHang",
                        MaThamDu="", QrUrl="", Timestamp=datetime.now().isoformat(timespec="seconds"),
                    )
                return

            sessions = await asyncio.to_thread(client.load_sessions_for_day, id_ngay)
            if not sessions:
                await update.message.reply_text(f"[{day}] Không có phiên để đăng ký. Bỏ qua.")
                for row in tasks:
                    await add_report(
                        Day=day, DayId=id_ngay, SessionValue="", SessionLabel="",
                        Row=row["__row_idx"] + 1, FullName=row["FullName"],
                        DOB_Day=row["DOB_Day"], DOB_Month=row["DOB_Month"], DOB_Year=row["DOB_Year"],
                        Phone=row["Phone"], Email=row["Email"], IDNumber=row["IDNumber"],
                        Status="Skipped", Attempts=0, Message="Không có phiên",
                        MaThamDu="", QrUrl="", Timestamp=datetime.now().isoformat(timespec="seconds"),
                    )
                return

            target_session = sessions[0]  # always pick first
            sid = target_session["value"]
            slabel = target_session["label"]

            for idx_row, row in enumerate(tasks):
                attempt = 0
                success = False
                last_msg = ""

                while attempt < CAPTCHA_MAX_TRIES and not success:
                    attempt += 1
                    try:
                        img_url = await asyncio.to_thread(client.fetch_captcha_image_url)
                        if not img_url:
                            last_msg = "Không lấy được captcha."
                            break
                        img_bytes = await asyncio.to_thread(client.download_image, img_url)

                        captcha_answer = await asyncio.to_thread(solve_captcha_via_2captcha, img_bytes) if USE_2CAPTCHA else None
                        if not captcha_answer and USE_2CAPTCHA:
                            last_msg = "2Captcha không trả lời."
                            continue

                        if USE_2CAPTCHA and captcha_answer:
                            result = await asyncio.to_thread(
                                client.submit_registration,
                                build_payload(id_ngay, sid, row, captcha_answer)
                            )
                            if "!!!True|~~|" in result:
                                # Parse MaThamDu + HTML xác nhận
                                arr = result.split("|~~|")
                                ma = arr[3].strip() if len(arr) > 3 else ""
                                # Gen QR
                                qr_url = await asyncio.to_thread(client.gen_qr_image, ma, ma)
                                qr_abs = ""
                                qr_bytes = None
                                if qr_url:
                                    qr_abs = qr_url if qr_url.startswith("http") else f"{client.root_base_url.rstrip('/')}{qr_url}"
                                    try:
                                        qr_bytes = await asyncio.to_thread(client.download_image, qr_abs)
                                    except Exception:
                                        qr_bytes = None
                                # Gửi về Telegram
                                cap = (
                                    f"✅ [{day}] Dòng {row['__row_idx'] + 1}\n"
                                    f"Mã tham dự: `{ma}`\nPhiên: {slabel} ({sid})"
                                )
                                if qr_bytes:
                                    await update.message.reply_photo(photo=qr_bytes, caption=cap, parse_mode="Markdown")
                                else:
                                    await update.message.reply_text(cap)
                                # SendEmail (best-effort)
                                try:
                                    _ = await asyncio.to_thread(client.send_email, sid, ma)
                                except Exception:
                                    pass
                                success = True
                                await add_report(
                                    Day=day, DayId=id_ngay, SessionValue=sid, SessionLabel=slabel,
                                    Row=row["__row_idx"] + 1, FullName=row["FullName"],
                                    DOB_Day=row["DOB_Day"], DOB_Month=row["DOB_Month"], DOB_Year=row["DOB_Year"],
                                    Phone=row["Phone"], Email=row["Email"], IDNumber=row["IDNumber"],
                                    Status="Success", Attempts=attempt, Message="OK",
                                    MaThamDu=ma, QrUrl=qr_abs, Timestamp=datetime.now().isoformat(timespec="seconds"),
                                )
                                break
                            elif is_session_full(result):
                                await update.message.reply_text(
                                    f"⛔ [{day}] Phiên đã hết lượt. Kết thúc xử lý ngày này."
                                )
                                # mark current row as skipped-full
                                await add_report(
                                    Day=day, DayId=id_ngay, SessionValue=sid, SessionLabel=slabel,
                                    Row=row["__row_idx"] + 1, FullName=row["FullName"],
                                    DOB_Day=row["DOB_Day"], DOB_Month=row["DOB_Month"], DOB_Year=row["DOB_Year"],
                                    Phone=row["Phone"], Email=row["Email"], IDNumber=row["IDNumber"],
                                    Status="Skipped", Attempts=attempt, Message="Session full",
                                    MaThamDu="", QrUrl="", Timestamp=datetime.now().isoformat(timespec="seconds"),
                                )
                                # mark remaining rows as skipped-full
                                for row2 in tasks[idx_row + 1:]:
                                    await add_report(
                                        Day=day, DayId=id_ngay, SessionValue=sid, SessionLabel=slabel,
                                        Row=row2["__row_idx"] + 1, FullName=row2["FullName"],
                                        DOB_Day=row2["DOB_Day"], DOB_Month=row2["DOB_Month"], DOB_Year=row2["DOB_Year"],
                                        Phone=row2["Phone"], Email=row2["Email"], IDNumber=row2["IDNumber"],
                                        Status="Skipped", Attempts=0, Message="Session full",
                                        MaThamDu="", QrUrl="", Timestamp=datetime.now().isoformat(timespec="seconds"),
                                    )
                                return  # stop processing remaining rows for this day
                            elif "captcha" in result.lower():
                                last_msg = f"Sai captcha (thử {attempt}/{CAPTCHA_MAX_TRIES})."
                                continue
                            else:
                                last_msg = f"Không thành công: {result[:200]}"
                                break
                        else:
                            # manual mode
                            key = f"{update.effective_chat.id}:{day}:{row['__row_idx']}"
                            with PENDING_LOCK:
                                PENDING_CAPTCHAS[key] = {
                                    "client": client,
                                    "id_ngay": id_ngay,
                                    "id_phien": sid,
                                    "row": row,
                                    "meta": {
                                        "Day": day, "DayId": id_ngay, "SessionValue": sid, "SessionLabel": slabel
                                    },
                                    "report_list": report_rows,
                                    "report_lock": report_lock,
                                }
                            await update.message.reply_photo(
                                photo=img_bytes,
                                caption=f"[{day}] Dòng {row['__row_idx'] + 1}: Vui lòng trả lời tin nhắn này bằng **mã captcha**.",
                                parse_mode="MarkdownV2",
                            )
                            last_msg = "Chuyển sang nhập tay."
                            break
                    except Exception as e:
                        last_msg = f"Lỗi attempt {attempt}: {e}"
                        continue

                if not success and USE_2CAPTCHA:
                    await update.message.reply_text(
                        f"⏭️ [{day}] Dòng {row['__row_idx'] + 1} — Bỏ qua sau {CAPTCHA_MAX_TRIES} lần thử. {last_msg}"
                    )
                    await add_report(
                        Day=day, DayId=id_ngay, SessionValue=sid, SessionLabel=slabel,
                        Row=row["__row_idx"] + 1, FullName=row["FullName"],
                        DOB_Day=row["DOB_Day"], DOB_Month=row["DOB_Month"], DOB_Year=row["DOB_Year"],
                        Phone=row["Phone"], Email=row["Email"], IDNumber=row["IDNumber"],
                        Status="Failed", Attempts=attempt, Message=last_msg or "Max attempts",
                        MaThamDu="", QrUrl="", Timestamp=datetime.now().isoformat(timespec="seconds"),
                    )

        except Exception as e:
            await update.message.reply_text(f"[{day}] Lỗi: {e}")
        finally:
            with ACTIVE_LOCK:
                ACTIVE_DAYS.discard(day)
                # Chỉ đánh dấu COMPLETED khi BẬT dedup toàn cục
                if not DISABLE_GLOBAL_DAY_DEDUP:
                    COMPLETED_DAYS.add(day)

    # tạo task asyncio cho mỗi ngày và đợi hoàn tất để xuất báo cáo
    tasks = [context.application.create_task(process_day(d, buckets[d])) for d in days_to_run]
    await update.message.reply_text("Đã khởi chạy các task theo ngày. Bot sẽ báo kết quả khi có.")

    _ = await asyncio.gather(*tasks, return_exceptions=True)
    # Tổng hợp & xuất báo cáo
    if not report_rows:
        await update.message.reply_text("Không có dữ liệu báo cáo (có thể tất cả bị chặn trước khi chạy).")
        return

    df_report = pd.DataFrame(report_rows)
    # sắp xếp gọn
    sort_cols = [c for c in ["Day", "Row"] if c in df_report.columns]
    if sort_cols:
        df_report = df_report.sort_values(sort_cols, kind="stable")

    total = len(df_report)
    succ = int((df_report["Status"] == "Success").sum())
    fail = int((df_report["Status"] == "Failed").sum())
    skip = int((df_report["Status"] == "Skipped").sum())
    summary = f"✅ Hoàn tất.\nTổng dòng: {total} — Thành công: {succ} • Thất bại: {fail} • Bỏ qua: {skip}"

    # tạo file Excel (fallback CSV nếu thiếu engine)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    xlsx_name = f"popmart_report_{ts}.xlsx"
    out = io.BytesIO()
    wrote = False
    try:
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df_report.to_excel(writer, index=False, sheet_name="Report")
        wrote = True
    except Exception:
        try:
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
                df_report.to_excel(writer, index=False, sheet_name="Report")
            wrote = True
        except Exception:
            wrote = False

    if wrote:
        out.seek(0)
        await update.message.reply_document(document=out, filename=xlsx_name, caption=summary)
    else:
        # fallback CSV
        csv_bytes = df_report.to_csv(index=False).encode("utf-8-sig")
        await update.message.reply_document(
            document=io.BytesIO(csv_bytes),
            filename=f"popmart_report_{ts}.csv",
            caption=summary
        )


# ===== Telegram Handlers =====
async def start(update: Update, _: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    msg = (
        "Gửi file Excel (.xlsx) cột: FullName, DOB_Day, DOB_Month, DOB_Year, Phone, Email, IDNumber.\n"
        "Hoặc đã cấu hình biến môi trường POP_ROWS_JSON/POP_ROWS_BASE64/POP_ROWS_CSV thì dùng lệnh /batdau để chạy ngay.\n"
        "Bot tự lấy mọi Sales Dates & chọn session đầu tiên. Mỗi ngày chạy 1 task và xử lý toàn bộ các dòng."
    )
    await update.message.reply_text(msg)


async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    doc = update.message.document
    if not doc or not doc.file_name.lower().endswith(".xlsx"):
        await update.message.reply_text("Vui lòng gửi file .xlsx")
        return

    file = await doc.get_file()
    # đọc Excel với dtype=str để giữ nguyên số 0 đầu
    df = pd.read_excel(io.BytesIO(await file.download_as_bytearray()), dtype=str)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        await update.message.reply_text(
            "❌ File thiếu cột bắt buộc: " + ", ".join(missing) +
            "\nCần đủ: " + ", ".join(REQUIRED_COLS)
        )
        return

    rows = df.to_dict(orient="records")
    await run_with_rows(update, context, rows)


async def cmd_batdau(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Chạy dữ liệu đã được cài sẵn qua biến môi trường."""
    if not is_admin(update.effective_user.id):
        return

    rows = parse_rows_from_env()
    if not rows:
        await update.message.reply_text(
            "❌ Không tìm thấy dữ liệu trong biến môi trường.\n"
            "Hãy set một trong các biến: POP_ROWS_BASE64 (base64 JSON), POP_ROWS_JSON (JSON) hoặc POP_ROWS_CSV (CSV)."
        )
        return

    # Validate đủ cột
    miss_any = any(any((r.get(c, "") == "" for c in REQUIRED_COLS)) for r in rows)
    if miss_any:
        await update.message.reply_text(
            "⚠️ Dữ liệu biến môi trường thiếu trường bắt buộc ở một số dòng. "
            "Cần đủ các cột: " + ", ".join(REQUIRED_COLS)
        )
        # vẫn tiếp tục chạy với các dòng đã có? Ở đây chọn chạy tất cả, vì build_payload đã str() mọi thứ
    await run_with_rows(update, context, rows)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual captcha answer when not using 2Captcha."""
    if not is_admin(update.effective_user.id):
        return
    if not update.message or not update.message.text:
        return

    chat_id = update.effective_chat.id
    text = update.message.text.strip()
    key = None
    with PENDING_LOCK:
        for k in list(PENDING_CAPTCHAS.keys()):
            if k.startswith(f"{chat_id}:"):
                key = k
                break
    if not key:
        return

    data = None
    with PENDING_LOCK:
        data = PENDING_CAPTCHAS.pop(key, None)
    if not data:
        await update.message.reply_text("Không tìm thấy tác vụ captcha tương ứng.")
        return

    client: PopmartClient = data["client"]
    id_ngay = data["id_ngay"]
    id_phien = data["id_phien"]
    row = data["row"]
    meta = data.get("meta", {})
    report_list = data.get("report_list")
    report_lock: asyncio.Lock = data.get("report_lock")  # type: ignore

    try:
        result = await asyncio.to_thread(
            client.submit_registration,
            build_payload(id_ngay, id_phien, row, text)
        )
        if "!!!True|~~|" in result:
            arr = result.split("|~~|")
            ma = arr[3].strip() if len(arr) > 3 else ""
            qr_url = await asyncio.to_thread(client.gen_qr_image, ma, ma)
            qr_abs = qr_url if (qr_url and qr_url.startswith("http")) else (f"{client.root_base_url.rstrip('/')}{qr_url}" if qr_url else "")
            qr_bytes = None
            if qr_abs:
                try:
                    qr_bytes = await asyncio.to_thread(client.download_image, qr_abs)
                except Exception:
                    qr_bytes = None
            cap = f"✅ Thành công.\nMã tham dự: `{ma}`"
            if qr_bytes:
                await update.message.reply_photo(photo=qr_bytes, caption=cap, parse_mode="Markdown")
            else:
                await update.message.reply_text(cap)
            try:
                _ = await asyncio.to_thread(client.send_email, id_phien, ma)
            except Exception:
                pass

            if report_list is not None and report_lock is not None:
                async with report_lock:
                    report_list.append({
                        "Day": meta.get("Day", ""),
                        "DayId": meta.get("DayId", ""),
                        "SessionValue": meta.get("SessionValue", ""),
                        "SessionLabel": meta.get("SessionLabel", ""),
                        "Row": row["__row_idx"] + 1,
                        "FullName": row["FullName"],
                        "DOB_Day": row["DOB_Day"],
                        "DOB_Month": row["DOB_Month"],
                        "DOB_Year": row["DOB_Year"],
                        "Phone": row["Phone"],
                        "Email": row["Email"],
                        "IDNumber": row["IDNumber"],
                        "Status": "Success",
                        "Attempts": 1,
                        "Message": "OK (manual captcha)",
                        "MaThamDu": ma,
                        "QrUrl": qr_abs,
                        "Timestamp": datetime.now().isoformat(timespec="seconds"),
                    })
        elif is_session_full(result):
            await update.message.reply_text("⛔ Phiên đã hết lượt.")
            if report_list is not None and report_lock is not None:
                async with report_lock:
                    report_list.append({
                        "Day": meta.get("Day", ""),
                        "DayId": meta.get("DayId", ""),
                        "SessionValue": meta.get("SessionValue", ""),
                        "SessionLabel": meta.get("SessionLabel", ""),
                        "Row": row["__row_idx"] + 1,
                        "FullName": row["FullName"],
                        "DOB_Day": row["DOB_Day"],
                        "DOB_Month": row["DOB_Month"],
                        "DOB_Year": row["DOB_Year"],
                        "Phone": row["Phone"],
                        "Email": row["Email"],
                        "IDNumber": row["IDNumber"],
                        "Status": "Skipped",
                        "Attempts": 1,
                        "Message": "Session full (manual)",
                        "MaThamDu": "",
                        "QrUrl": "",
                        "Timestamp": datetime.now().isoformat(timespec="seconds"),
                    })
        elif "captcha" in result.lower():
            await update.message.reply_text("❌ Sai captcha. Gửi lại mã hoặc /start để gửi file mới.")
            if report_list is not None and report_lock is not None:
                async with report_lock:
                    report_list.append({
                        "Day": meta.get("Day", ""),
                        "DayId": meta.get("DayId", ""),
                        "SessionValue": meta.get("SessionValue", ""),
                        "SessionLabel": meta.get("SessionLabel", ""),
                        "Row": row["__row_idx"] + 1,
                        "FullName": row["FullName"],
                        "DOB_Day": row["DOB_Day"],
                        "DOB_Month": row["DOB_Month"],
                        "DOB_Year": row["DOB_Year"],
                        "Phone": row["Phone"],
                        "Email": row["Email"],
                        "IDNumber": row["IDNumber"],
                        "Status": "Failed",
                        "Attempts": 1,
                        "Message": "Sai captcha (manual)",
                        "MaThamDu": "",
                        "QrUrl": "",
                        "Timestamp": datetime.now().isoformat(timespec="seconds"),
                    })
        else:
            await update.message.reply_text(f"⚠️ Không thành công: {result[:200]}")
            if report_list is not None and report_lock is not None:
                async with report_lock:
                    report_list.append({
                        "Day": meta.get("Day", ""),
                        "DayId": meta.get("DayId", ""),
                        "SessionValue": meta.get("SessionValue", ""),
                        "SessionLabel": meta.get("SessionLabel", ""),
                        "Row": row["__row_idx"] + 1,
                        "FullName": row["FullName"],
                        "DOB_Day": row["DOB_Day"],
                        "DOB_Month": row["DOB_Month"],
                        "DOB_Year": row["DOB_Year"],
                        "Phone": row["Phone"],
                        "Email": row["Email"],
                        "IDNumber": row["IDNumber"],
                        "Status": "Failed",
                        "Attempts": 1,
                        "Message": (result or "")[:200],
                        "MaThamDu": "",
                        "QrUrl": "",
                        "Timestamp": datetime.now().isoformat(timespec="seconds"),
                    })
    except Exception as e:
        await update.message.reply_text(f"❌ Lỗi: {e}")


async def on_error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled exception", exc_info=context.error)
    try:
        if update and update.effective_message:
            await update.effective_message.reply_text(f"Lỗi nội bộ: {context.error}")
    except Exception:
        pass


def main():
    if not BOT_TOKEN:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("batdau", cmd_batdau))  # <-- new command
    app.add_handler(MessageHandler(filters.Document.ALL, handle_excel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(on_error)
    log.info("Bot started.")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
