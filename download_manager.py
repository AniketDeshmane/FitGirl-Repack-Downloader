#!/usr/bin/env python3
"""
Download Manager - A minimal, dark-themed IDM-like download manager.
Supports fuckingfast.co links and FitGirl repack page URL extraction.
Features: pause/resume, concurrent downloads, auto-retry, scheduler, file categories.
"""

import sys
import os
import re
import time
import logging
import logging.handlers
from datetime import datetime, timedelta
from threading import Event
from urllib.parse import urljoin, urlparse, unquote
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List

import requests
from bs4 import BeautifulSoup

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTextEdit, QLineEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QProgressBar, QLabel, QSpinBox, QFileDialog, QHeaderView,
    QAbstractItemView, QMenu, QAction, QToolBar, QSplitter,
    QListWidget, QListWidgetItem, QDialog, QDateTimeEdit, QDialogButtonBox,
    QMessageBox, QFrame, QStatusBar, QSizePolicy
)
from PyQt5.QtCore import (
    Qt, QThread, pyqtSignal, pyqtSlot, QTimer, QDateTime, QSize
)
from PyQt5.QtGui import QFont, QIcon, QColor


# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------

def setup_logging():
    """Configure per-day log files in logs/ directory."""
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
        os.makedirs(log_dir, exist_ok=True)

        logger = logging.getLogger("DM")
        logger.setLevel(logging.DEBUG)

        # Per-day rotating file handler
        log_file = os.path.join(log_dir, "dm.log")
        file_handler = logging.handlers.TimedRotatingFileHandler(
            log_file, when="midnight", interval=1, backupCount=30,
            encoding="utf-8"
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.namer = lambda name: name.replace("dm.log.", "dm_") + ".log"
        file_handler.setLevel(logging.DEBUG)
        file_fmt = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_fmt)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(file_fmt)
        logger.addHandler(console_handler)

        return logger
    except Exception as e:
        print(f"Failed to setup logging: {e}")
        return logging.getLogger("DM")


log = setup_logging()


# ---------------------------------------------------------------------------
# Global Exception Hook
# ---------------------------------------------------------------------------

def global_exception_hook(exc_type, exc_value, exc_tb):
    """Catch any unhandled exception, log it, and show a crash dialog."""
    try:
        import traceback
        tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        log.error(f"Unhandled exception:\n{tb_str}")
    except Exception:
        pass
    # Try to show a dialog if QApplication exists
    try:
        app = QApplication.instance()
        if app:
            QMessageBox.critical(
                None, "Unexpected Error",
                f"An unexpected error occurred:\n\n{exc_value}\n\nSee logs for details."
            )
    except Exception:
        pass


sys.excepthook = global_exception_hook


# ---------------------------------------------------------------------------
# Constants & Enums
# ---------------------------------------------------------------------------

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
CHUNK_SIZE = 8192  # 8KB


class Status(Enum):
    WAITING = "Waiting"
    ANALYZING = "Analyzing"
    READY = "Ready"
    DOWNLOADING = "Downloading"
    PAUSED = "Paused"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    RETRYING = "Retrying"


class Category(Enum):
    ALL = "All Downloads"
    ARCHIVES = "Archives (.rar)"
    BINARIES = "Binaries (.bin)"
    VIDEOS = "Videos (.mp4)"
    OTHER = "Other"
    COMPLETED = "Completed"
    FAILED = "Failed"


def categorize_file(filename: str) -> Category:
    """Auto-categorize a file based on its extension."""
    try:
        ext = os.path.splitext(filename)[1].lower()
        if ext in (".rar", ".zip", ".7z", ".tar", ".gz"):
            return Category.ARCHIVES
        elif ext in (".bin",):
            return Category.BINARIES
        elif ext in (".mp4", ".mkv", ".avi"):
            return Category.VIDEOS
        else:
            return Category.OTHER
    except Exception:
        return Category.OTHER


# ---------------------------------------------------------------------------
# Data Model
# ---------------------------------------------------------------------------

@dataclass
class DownloadItem:
    id: int
    page_url: str  # fuckingfast.co URL
    download_url: str = ""  # resolved direct download URL
    filename: str = ""
    file_size: int = 0  # bytes
    file_size_str: str = ""
    downloaded: int = 0  # bytes downloaded so far
    speed: float = 0.0  # bytes/sec
    status: Status = Status.WAITING
    category: Category = Category.OTHER
    retries: int = 0
    max_retries: int = 3
    eta_str: str = ""
    error_msg: str = ""


# ---------------------------------------------------------------------------
# Dark Theme Stylesheet
# ---------------------------------------------------------------------------

DARK_STYLESHEET = """
QMainWindow, QWidget {
    background-color: #1e1e2e;
    color: #e0e0e0;
    font-family: 'Segoe UI', sans-serif;
    font-size: 13px;
}
QToolBar {
    background-color: #2b2b3d;
    border: none;
    padding: 4px;
    spacing: 6px;
}
QToolBar QPushButton {
    background-color: #3b3b52;
    color: #e0e0e0;
    border: none;
    border-radius: 6px;
    padding: 6px 14px;
    font-weight: bold;
    min-width: 80px;
}
QToolBar QPushButton:hover {
    background-color: #7c3aed;
}
QToolBar QPushButton:pressed {
    background-color: #6d28d9;
}
QPushButton {
    background-color: #7c3aed;
    color: #ffffff;
    border: none;
    border-radius: 6px;
    padding: 8px 18px;
    font-weight: bold;
}
QPushButton:hover {
    background-color: #8b5cf6;
}
QPushButton:pressed {
    background-color: #6d28d9;
}
QPushButton:disabled {
    background-color: #3b3b52;
    color: #666680;
}
QPushButton#dangerBtn {
    background-color: #ef4444;
}
QPushButton#dangerBtn:hover {
    background-color: #f87171;
}
QPushButton#successBtn {
    background-color: #22c55e;
}
QPushButton#successBtn:hover {
    background-color: #4ade80;
}
QTextEdit, QLineEdit {
    background-color: #2b2b3d;
    color: #e0e0e0;
    border: 1px solid #3b3b52;
    border-radius: 6px;
    padding: 6px;
    selection-background-color: #7c3aed;
}
QTextEdit:focus, QLineEdit:focus {
    border: 1px solid #7c3aed;
}
QTableWidget {
    background-color: #1e1e2e;
    alternate-background-color: #252538;
    color: #e0e0e0;
    gridline-color: #2b2b3d;
    border: none;
    selection-background-color: #3b3b52;
}
QTableWidget::item {
    padding: 4px;
}
QHeaderView::section {
    background-color: #2b2b3d;
    color: #a0a0b8;
    border: none;
    padding: 6px;
    font-weight: bold;
}
QProgressBar {
    background-color: #2b2b3d;
    border: none;
    border-radius: 4px;
    text-align: center;
    color: #e0e0e0;
    font-size: 11px;
}
QProgressBar::chunk {
    background-color: #7c3aed;
    border-radius: 4px;
}
QListWidget {
    background-color: #1e1e2e;
    color: #e0e0e0;
    border: none;
    outline: none;
    font-size: 13px;
}
QListWidget::item {
    padding: 10px 14px;
    border-radius: 6px;
    margin: 2px 4px;
}
QListWidget::item:selected {
    background-color: #7c3aed;
    color: #ffffff;
}
QListWidget::item:hover {
    background-color: #2b2b3d;
}
QSpinBox {
    background-color: #2b2b3d;
    color: #e0e0e0;
    border: 1px solid #3b3b52;
    border-radius: 6px;
    padding: 4px;
}
QStatusBar {
    background-color: #2b2b3d;
    color: #a0a0b8;
    border-top: 1px solid #3b3b52;
}
QLabel {
    color: #e0e0e0;
}
QLabel#dimLabel {
    color: #666680;
}
QSplitter::handle {
    background-color: #2b2b3d;
    width: 2px;
}
QMenu {
    background-color: #2b2b3d;
    color: #e0e0e0;
    border: 1px solid #3b3b52;
    border-radius: 6px;
    padding: 4px;
}
QMenu::item:selected {
    background-color: #7c3aed;
    border-radius: 4px;
}
QDialog {
    background-color: #1e1e2e;
    color: #e0e0e0;
}
QDateTimeEdit {
    background-color: #2b2b3d;
    color: #e0e0e0;
    border: 1px solid #3b3b52;
    border-radius: 6px;
    padding: 6px;
}
QScrollBar:vertical {
    background: #1e1e2e;
    width: 10px;
    border: none;
}
QScrollBar::handle:vertical {
    background: #3b3b52;
    border-radius: 5px;
    min-height: 30px;
}
QScrollBar::handle:vertical:hover {
    background: #7c3aed;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0;
}
QScrollBar:horizontal {
    background: #1e1e2e;
    height: 10px;
    border: none;
}
QScrollBar::handle:horizontal {
    background: #3b3b52;
    border-radius: 5px;
    min-width: 30px;
}
QScrollBar::handle:horizontal:hover {
    background: #7c3aed;
}
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
    width: 0;
}
"""


# ---------------------------------------------------------------------------
# Qt Log Handler  -- routes log messages to a QTextEdit widget
# ---------------------------------------------------------------------------

class QtLogHandler(logging.Handler):
    """Custom logging handler that emits log records to a Qt signal."""

    def __init__(self, signal):
        super().__init__()
        self.signal = signal

    def emit(self, record):
        try:
            msg = self.format(record)
            self.signal.emit(msg)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Scraper Workers
# ---------------------------------------------------------------------------

class FitGirlScraperWorker(QThread):
    """Scrapes a FitGirl repack page and extracts fuckingfast.co links."""
    finished = pyqtSignal(list)
    error = pyqtSignal(str)
    log_msg = pyqtSignal(str)

    def __init__(self, url: str):
        super().__init__()
        self.url = url

    def run(self):
        try:
            log.info(f"Scraping FitGirl page: {self.url}")
            self.log_msg.emit(f"Fetching FitGirl page: {self.url}")
            response = requests.get(self.url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            links = []
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if "fuckingfast.co" in href:
                    links.append(href)
            links = list(dict.fromkeys(links))  # deduplicate, preserve order
            log.info(f"Extracted {len(links)} fuckingfast.co links from FitGirl page")
            self.log_msg.emit(f"Found {len(links)} fuckingfast.co links")
            self.finished.emit(links)
        except requests.RequestException as e:
            log.error(f"FitGirl scrape failed: {e}")
            self.error.emit(f"Failed to fetch FitGirl page: {e}")
        except Exception as e:
            log.error(f"FitGirl scrape unexpected error: {e}")
            self.error.emit(f"Unexpected error: {e}")


def _sanitize_url(url: str) -> str:
    """Strip the #fragment from a URL (fragments are client-side only, not sent to server)."""
    try:
        parsed = urlparse(url)
        # Rebuild without fragment
        return parsed._replace(fragment="").geturl()
    except Exception:
        # Fallback: just split on #
        return url.split("#")[0]


def _create_session() -> requests.Session:
    """Create a requests Session with retry adapter for transient errors."""
    try:
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter

        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=2,  # 2s, 4s, 8s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(HEADERS)
        return session
    except Exception as e:
        log.error(f"Failed to create session with retry: {e}")
        session = requests.Session()
        session.headers.update(HEADERS)
        return session


class LinkResolverWorker(QThread):
    """Resolves a single fuckingfast.co page URL to the actual download link."""
    resolved = pyqtSignal(int, str, str, str)  # id, download_url, filename, size_str
    failed = pyqtSignal(int, str)  # id, error_msg
    log_msg = pyqtSignal(str)

    MAX_ATTEMPTS = 3
    BACKOFF_BASE = 3  # seconds

    def __init__(self, item_id: int, page_url: str):
        super().__init__()
        self.item_id = item_id
        self.page_url = page_url

    def run(self):
        try:
            # Extract filename from fragment BEFORE sanitizing the URL
            parsed = urlparse(self.page_url)
            fragment = unquote(parsed.fragment) if parsed.fragment else ""
            filename = fragment if fragment else f"file_{self.item_id}"

            # Sanitize: strip the fragment so we only send the base URL to the server
            clean_url = _sanitize_url(self.page_url)
            log.info(f"Analyzing link {self.item_id}: {clean_url} (filename from fragment: {filename})")

            # Retry loop
            last_error = ""
            for attempt in range(1, self.MAX_ATTEMPTS + 1):
                try:
                    result = self._try_resolve(clean_url, filename, fragment, attempt)
                    if result is not None:
                        return  # success or permanent failure already emitted
                    # result is None means "download button not found, retry"
                    last_error = "Download button not found on page"
                except requests.RequestException as e:
                    last_error = str(e)
                    log.warning(
                        f"Resolve attempt {attempt}/{self.MAX_ATTEMPTS} failed for "
                        f"{clean_url}: {e}"
                    )
                except Exception as e:
                    last_error = str(e)
                    log.warning(
                        f"Resolve attempt {attempt}/{self.MAX_ATTEMPTS} unexpected error for "
                        f"{clean_url}: {e}"
                    )

                if attempt < self.MAX_ATTEMPTS:
                    wait = self.BACKOFF_BASE * attempt  # 3s, 6s
                    log.info(f"Waiting {wait}s before retry for {filename}...")
                    time.sleep(wait)

            log.error(f"Failed all {self.MAX_ATTEMPTS} attempts for {clean_url}: {last_error}")
            self.failed.emit(self.item_id, last_error)

        except Exception as e:
            log.error(f"Resolve worker fatal error for {self.page_url}: {e}")
            self.failed.emit(self.item_id, str(e))

    def _try_resolve(self, clean_url: str, filename: str, fragment: str, attempt: int):
        """
        Try to resolve the download link once.
        Returns True if resolved (signal emitted), None if DOWNLOAD button not found (retryable).
        Raises on network errors.
        """
        session = _create_session()
        try:
            response = session.get(clean_url, timeout=30)
            response.raise_for_status()
        finally:
            session.close()

        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        download_url = ""

        # --- Strategy 1: Extract URL from JavaScript download() function ---
        # The DOWNLOAD button on fuckingfast.co is a <button @click="download()">
        # and the actual URL is inside a <script> block like:
        #   window.open("https://fuckingfast.co/dl/...")
        try:
            for script_tag in soup.find_all("script"):
                script_text = script_tag.string or ""
                if "function download()" in script_text:
                    # Find all window.open("...") URLs in the download function
                    urls = re.findall(
                        r'window\.open\(\s*["\']([^"\']+)["\']',
                        script_text
                    )
                    # The actual download URL contains "/dl/" in its path
                    for url in urls:
                        if "/dl/" in url:
                            download_url = url
                            break
                    # If no /dl/ URL, take the last window.open URL as fallback
                    if not download_url and urls:
                        download_url = urls[-1]
                    break
        except Exception as e:
            log.warning(f"Script parsing error for {clean_url}: {e}")

        # --- Strategy 2: Look for <a> tag with text "DOWNLOAD" (fallback) ---
        if not download_url:
            for a_tag in soup.find_all("a"):
                try:
                    text = a_tag.get_text(strip=True).upper()
                    if text == "DOWNLOAD":
                        download_url = urljoin(clean_url, a_tag.get("href", ""))
                        break
                except Exception:
                    continue

        # --- Strategy 3: Look for <a> with "download" or "/dl/" in href ---
        if not download_url:
            for a_tag in soup.find_all("a", href=True):
                try:
                    href = a_tag["href"]
                    if "/dl/" in href.lower():
                        download_url = urljoin(clean_url, href)
                        break
                except Exception:
                    continue

        # --- Strategy 4: Regex scan entire HTML for /dl/ URLs ---
        if not download_url:
            try:
                dl_matches = re.findall(
                    r'https?://fuckingfast\.co/dl/[A-Za-z0-9_\-]+',
                    html
                )
                if dl_matches:
                    download_url = dl_matches[0]
            except Exception:
                pass

        if not download_url:
            log.warning(f"Attempt {attempt}: No download URL found for {clean_url}")
            return None  # signal to retry

        # Extract file size from page
        size_str = ""
        try:
            page_text = soup.get_text()
            size_match = re.search(r"Size:\s*([\d.]+\s*[KMGT]?B)", page_text, re.IGNORECASE)
            if size_match:
                size_str = size_match.group(1).strip()
        except Exception:
            pass

        # Use filename from page if we got a generic one
        if not fragment:
            try:
                for tag in soup.find_all(["h1", "h2", "h3", "p"]):
                    text = tag.get_text(strip=True)
                    if "." in text and len(text) < 200:
                        filename = text
                        break
            except Exception:
                pass

        log.info(f"Resolved: {filename} ({size_str}) -> {download_url}")
        self.log_msg.emit(f"Resolved: {filename} ({size_str})")
        self.resolved.emit(self.item_id, download_url, filename, size_str)
        return True


# ---------------------------------------------------------------------------
# Download Worker
# ---------------------------------------------------------------------------

class DownloadWorker(QThread):
    """Downloads a single file with streaming, pause/resume support."""
    progress = pyqtSignal(int, int, int)  # id, downloaded, total
    speed_update = pyqtSignal(int, float)  # id, bytes_per_sec
    status_changed = pyqtSignal(int, str)  # id, status string
    finished = pyqtSignal(int)  # id
    error = pyqtSignal(int, str)  # id, error_msg

    def __init__(self, item: DownloadItem, save_dir: str):
        super().__init__()
        self.item = item
        self.save_dir = save_dir
        self._pause_event = Event()
        self._pause_event.set()  # not paused by default
        self._cancel = False

    def pause(self):
        """Pause the download."""
        try:
            self._pause_event.clear()
            log.info(f"Paused: {self.item.filename}")
        except Exception as e:
            log.error(f"Error pausing {self.item.filename}: {e}")

    def resume(self):
        """Resume the download."""
        try:
            self._pause_event.set()
            log.info(f"Resumed: {self.item.filename}")
        except Exception as e:
            log.error(f"Error resuming {self.item.filename}: {e}")

    def cancel(self):
        """Cancel the download."""
        try:
            self._cancel = True
            self._pause_event.set()  # unblock if paused
            log.info(f"Cancelled: {self.item.filename}")
        except Exception as e:
            log.error(f"Error cancelling {self.item.filename}: {e}")

    def run(self):
        try:
            filepath = os.path.join(self.save_dir, self.item.filename)
            downloaded = 0
            mode = "wb"
            req_headers = dict(HEADERS)

            # Resume support: if partial file exists, use Range header
            try:
                if os.path.exists(filepath):
                    downloaded = os.path.getsize(filepath)
                    if downloaded > 0:
                        req_headers["Range"] = f"bytes={downloaded}-"
                        mode = "ab"
                        log.info(f"Resuming {self.item.filename} from {downloaded} bytes")
            except OSError as e:
                log.warning(f"Could not check existing file {filepath}: {e}")
                downloaded = 0
                mode = "wb"

            self.status_changed.emit(self.item.id, Status.DOWNLOADING.value)

            try:
                response = requests.get(
                    self.item.download_url, headers=req_headers,
                    stream=True, timeout=60
                )
                response.raise_for_status()
            except requests.RequestException as e:
                log.error(f"Download request failed for {self.item.filename}: {e}")
                self.error.emit(self.item.id, str(e))
                return

            # Determine total size
            total = 0
            try:
                content_length = response.headers.get("Content-Length")
                if content_length:
                    total = int(content_length) + downloaded
                content_range = response.headers.get("Content-Range")
                if content_range:
                    try:
                        total = int(content_range.split("/")[-1])
                    except (ValueError, IndexError):
                        pass
            except Exception:
                pass

            self.progress.emit(self.item.id, downloaded, total)

            start_time = time.time()
            last_speed_time = start_time
            last_speed_bytes = downloaded

            try:
                with open(filepath, mode) as f:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        # Check cancel
                        if self._cancel:
                            self.status_changed.emit(self.item.id, Status.CANCELLED.value)
                            log.info(f"Download cancelled: {self.item.filename}")
                            return

                        # Check pause
                        self._pause_event.wait()

                        if self._cancel:
                            self.status_changed.emit(self.item.id, Status.CANCELLED.value)
                            return

                        if chunk:
                            try:
                                f.write(chunk)
                                downloaded += len(chunk)
                            except (IOError, OSError) as e:
                                log.error(f"Write error for {self.item.filename}: {e}")
                                self.error.emit(self.item.id, f"Write error: {e}")
                                return

                            self.progress.emit(self.item.id, downloaded, total)

                            # Speed calculation every 0.5 seconds
                            try:
                                now = time.time()
                                elapsed = now - last_speed_time
                                if elapsed >= 0.5:
                                    speed = (downloaded - last_speed_bytes) / elapsed
                                    self.speed_update.emit(self.item.id, speed)
                                    last_speed_time = now
                                    last_speed_bytes = downloaded
                            except Exception:
                                pass

            except (IOError, OSError) as e:
                log.error(f"File I/O error for {self.item.filename}: {e}")
                self.error.emit(self.item.id, f"File error: {e}")
                return

            elapsed_total = time.time() - start_time
            log.info(
                f"Download complete: {self.item.filename} "
                f"({self._format_size(downloaded)} in {self._format_time(elapsed_total)})"
            )
            self.status_changed.emit(self.item.id, Status.COMPLETED.value)
            self.finished.emit(self.item.id)

        except Exception as e:
            log.error(f"Download worker unexpected error for {self.item.filename}: {e}")
            self.error.emit(self.item.id, str(e))

    @staticmethod
    def _format_size(b):
        try:
            for unit in ("B", "KB", "MB", "GB"):
                if b < 1024:
                    return f"{b:.1f}{unit}"
                b /= 1024
            return f"{b:.1f}TB"
        except Exception:
            return "?"

    @staticmethod
    def _format_time(seconds):
        try:
            m, s = divmod(int(seconds), 60)
            h, m = divmod(m, 60)
            if h > 0:
                return f"{h}h{m:02d}m{s:02d}s"
            return f"{m}m{s:02d}s"
        except Exception:
            return "?"


# ---------------------------------------------------------------------------
# Download Manager (queue + concurrency control)
# ---------------------------------------------------------------------------

class DownloadManager:
    """Manages download queue and concurrent workers."""

    def __init__(self, max_concurrent: int = 3, save_dir: str = ""):
        self.max_concurrent = max_concurrent
        self.save_dir = save_dir or os.path.join(os.path.expanduser("~"), "Downloads")
        self.items: dict[int, DownloadItem] = {}
        self.workers: dict[int, DownloadWorker] = {}
        self.queue: list[int] = []  # IDs waiting to start

    def set_max_concurrent(self, n: int):
        try:
            self.max_concurrent = max(1, min(10, n))
        except Exception:
            pass

    def active_count(self) -> int:
        try:
            return sum(
                1 for w in self.workers.values()
                if w.isRunning() and self.items.get(w.item.id, None)
                and self.items[w.item.id].status == Status.DOWNLOADING
            )
        except Exception:
            return 0

    def can_start_more(self) -> bool:
        try:
            return self.active_count() < self.max_concurrent
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Scheduler Dialog
# ---------------------------------------------------------------------------

class SchedulerDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        try:
            self.setWindowTitle("Schedule Downloads")
            self.setFixedSize(350, 180)
            layout = QVBoxLayout(self)

            lbl = QLabel("Start downloads at:")
            lbl.setStyleSheet("font-size: 14px; font-weight: bold;")
            layout.addWidget(lbl)

            self.datetime_edit = QDateTimeEdit(QDateTime.currentDateTime().addSecs(3600))
            self.datetime_edit.setCalendarPopup(True)
            self.datetime_edit.setDisplayFormat("yyyy-MM-dd hh:mm:ss")
            layout.addWidget(self.datetime_edit)

            btn_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
            btn_box.accepted.connect(self.accept)
            btn_box.rejected.connect(self.reject)
            layout.addWidget(btn_box)
        except Exception as e:
            log.error(f"SchedulerDialog init error: {e}")

    def get_datetime(self) -> QDateTime:
        try:
            return self.datetime_edit.dateTime()
        except Exception:
            return QDateTime.currentDateTime()


# ---------------------------------------------------------------------------
# Main Window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):
    log_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        try:
            self.setWindowTitle("Download Manager")
            self.setMinimumSize(1100, 700)
            self.resize(1280, 800)

            self.manager = DownloadManager()
            self.next_id = 1
            self.resolver_workers: list[LinkResolverWorker] = []
            self.resolve_total = 0
            self.resolve_done = 0
            self.scheduled_timer: Optional[QTimer] = None
            self.scheduled_time: Optional[QDateTime] = None
            self.countdown_timer: Optional[QTimer] = None
            self.input_panel_visible = True
            self._auto_download_after_analyze = False
            self._analyze_batch_queue: list = []  # URLs waiting to be analyzed
            self._active_resolvers: list[LinkResolverWorker] = []
            self._max_concurrent_resolvers = 3

            self._setup_qt_log_handler()
            self._build_ui()
            self._connect_signals()

            log.info("App started")
        except Exception as e:
            log.error(f"MainWindow init error: {e}")

    # ---- Qt log handler ----

    def _setup_qt_log_handler(self):
        try:
            qt_handler = QtLogHandler(self.log_signal)
            qt_handler.setLevel(logging.DEBUG)
            qt_fmt = logging.Formatter(
                "[%(asctime)s] [%(levelname)s] %(message)s",
                datefmt="%H:%M:%S"
            )
            qt_handler.setFormatter(qt_fmt)
            logging.getLogger("DM").addHandler(qt_handler)
        except Exception as e:
            print(f"Failed to setup Qt log handler: {e}")

    # ---- Build UI ----

    def _build_ui(self):
        try:
            central = QWidget()
            self.setCentralWidget(central)
            main_layout = QVBoxLayout(central)
            main_layout.setContentsMargins(0, 0, 0, 0)
            main_layout.setSpacing(0)

            # Toolbar
            self._build_toolbar()

            # Body: sidebar + content
            body_splitter = QSplitter(Qt.Horizontal)
            body_splitter.setHandleWidth(2)

            # Left sidebar
            self._build_sidebar(body_splitter)

            # Right content area
            content_widget = QWidget()
            content_layout = QVBoxLayout(content_widget)
            content_layout.setContentsMargins(8, 8, 8, 0)
            content_layout.setSpacing(6)

            # Input panel
            self._build_input_panel(content_layout)

            # Download table
            self._build_table(content_layout)

            # Log panel (collapsible)
            self._build_log_panel(content_layout)

            body_splitter.addWidget(content_widget)
            body_splitter.setStretchFactor(0, 0)
            body_splitter.setStretchFactor(1, 1)
            body_splitter.setSizes([180, 1100])

            main_layout.addWidget(body_splitter)

            # Status bar
            self._build_status_bar()

        except Exception as e:
            log.error(f"Build UI error: {e}")

    def _build_toolbar(self):
        try:
            toolbar = QToolBar("Main Toolbar")
            toolbar.setMovable(False)
            toolbar.setIconSize(QSize(20, 20))
            self.addToolBar(toolbar)

            self.btn_toggle_input = QPushButton("Add Links")
            self.btn_toggle_input.clicked.connect(self._toggle_input_panel)
            toolbar.addWidget(self.btn_toggle_input)

            self.btn_analyze = QPushButton("Analyze")
            toolbar.addWidget(self.btn_analyze)

            toolbar.addSeparator()

            self.btn_download_all = QPushButton("Download All")
            self.btn_download_all.setObjectName("successBtn")
            toolbar.addWidget(self.btn_download_all)

            self.btn_pause_all = QPushButton("Pause All")
            toolbar.addWidget(self.btn_pause_all)

            self.btn_resume_all = QPushButton("Resume All")
            toolbar.addWidget(self.btn_resume_all)

            self.btn_cancel_all = QPushButton("Cancel All")
            self.btn_cancel_all.setObjectName("dangerBtn")
            toolbar.addWidget(self.btn_cancel_all)

            toolbar.addSeparator()

            self.btn_scheduler = QPushButton("Scheduler")
            toolbar.addWidget(self.btn_scheduler)

            # Spacer
            spacer = QWidget()
            spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
            toolbar.addWidget(spacer)

            # Concurrent downloads
            lbl = QLabel("  Max:")
            lbl.setObjectName("dimLabel")
            toolbar.addWidget(lbl)

            self.spin_concurrent = QSpinBox()
            self.spin_concurrent.setRange(1, 10)
            self.spin_concurrent.setValue(3)
            self.spin_concurrent.setFixedWidth(60)
            toolbar.addWidget(self.spin_concurrent)

            # Folder picker
            self.btn_folder = QPushButton("Folder")
            toolbar.addWidget(self.btn_folder)

        except Exception as e:
            log.error(f"Build toolbar error: {e}")

    def _build_sidebar(self, parent_splitter: QSplitter):
        try:
            self.sidebar = QListWidget()
            self.sidebar.setFixedWidth(180)

            categories = [
                (Category.ALL, "0"),
                (Category.ARCHIVES, "0"),
                (Category.BINARIES, "0"),
                (Category.VIDEOS, "0"),
                (Category.OTHER, "0"),
                (Category.COMPLETED, "0"),
                (Category.FAILED, "0"),
            ]
            for cat, count in categories:
                item = QListWidgetItem(f"{cat.value}  ({count})")
                item.setData(Qt.UserRole, cat)
                self.sidebar.addItem(item)

            self.sidebar.setCurrentRow(0)
            parent_splitter.addWidget(self.sidebar)
        except Exception as e:
            log.error(f"Build sidebar error: {e}")

    def _build_input_panel(self, parent_layout: QVBoxLayout):
        try:
            self.input_frame = QFrame()
            input_layout = QVBoxLayout(self.input_frame)
            input_layout.setContentsMargins(0, 0, 0, 0)
            input_layout.setSpacing(6)

            # FitGirl URL row
            fitgirl_row = QHBoxLayout()
            fitgirl_lbl = QLabel("FitGirl URL:")
            fitgirl_lbl.setFixedWidth(80)
            fitgirl_row.addWidget(fitgirl_lbl)

            self.fitgirl_input = QLineEdit()
            self.fitgirl_input.setPlaceholderText("Paste a FitGirl repack page URL to auto-extract links...")
            fitgirl_row.addWidget(self.fitgirl_input)

            self.btn_extract = QPushButton("Extract")
            self.btn_extract.setFixedWidth(100)
            fitgirl_row.addWidget(self.btn_extract)
            input_layout.addLayout(fitgirl_row)

            # Links text area
            self.links_input = QTextEdit()
            self.links_input.setPlaceholderText(
                "Paste fuckingfast.co links here (one per line)...\n\n"
                "Example:\nhttps://fuckingfast.co/p1y261eoep14#filename.rar"
            )
            self.links_input.setMaximumHeight(150)
            input_layout.addWidget(self.links_input)

            parent_layout.addWidget(self.input_frame)
        except Exception as e:
            log.error(f"Build input panel error: {e}")

    def _build_table(self, parent_layout: QVBoxLayout):
        try:
            self.table = QTableWidget()
            self.table.setColumnCount(8)
            self.table.setHorizontalHeaderLabels([
                "#", "Filename", "Size", "Progress", "Speed", "Status", "ETA", "Category"
            ])
            self.table.setAlternatingRowColors(True)
            self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
            self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
            self.table.setContextMenuPolicy(Qt.CustomContextMenu)
            self.table.setSortingEnabled(True)
            self.table.verticalHeader().setVisible(False)

            header = self.table.horizontalHeader()
            header.setSectionResizeMode(0, QHeaderView.Fixed)
            header.setSectionResizeMode(1, QHeaderView.Stretch)
            header.setSectionResizeMode(2, QHeaderView.Fixed)
            header.setSectionResizeMode(3, QHeaderView.Fixed)
            header.setSectionResizeMode(4, QHeaderView.Fixed)
            header.setSectionResizeMode(5, QHeaderView.Fixed)
            header.setSectionResizeMode(6, QHeaderView.Fixed)
            header.setSectionResizeMode(7, QHeaderView.Fixed)

            self.table.setColumnWidth(0, 40)
            self.table.setColumnWidth(2, 90)
            self.table.setColumnWidth(3, 180)
            self.table.setColumnWidth(4, 90)
            self.table.setColumnWidth(5, 100)
            self.table.setColumnWidth(6, 80)
            self.table.setColumnWidth(7, 110)

            parent_layout.addWidget(self.table, stretch=1)
        except Exception as e:
            log.error(f"Build table error: {e}")

    def _build_log_panel(self, parent_layout: QVBoxLayout):
        try:
            self.log_toggle_btn = QPushButton("Logs")
            self.log_toggle_btn.setFixedHeight(24)
            self.log_toggle_btn.setStyleSheet(
                "QPushButton { background-color: #2b2b3d; color: #666680; "
                "border-radius: 4px; font-size: 11px; padding: 2px; }"
                "QPushButton:hover { background-color: #3b3b52; }"
            )
            parent_layout.addWidget(self.log_toggle_btn)

            self.log_text = QTextEdit()
            self.log_text.setReadOnly(True)
            self.log_text.setMaximumHeight(120)
            self.log_text.setVisible(False)
            self.log_text.setStyleSheet(
                "QTextEdit { font-family: 'Consolas', monospace; font-size: 11px; "
                "color: #888; background-color: #16161e; }"
            )
            parent_layout.addWidget(self.log_text)
        except Exception as e:
            log.error(f"Build log panel error: {e}")

    def _build_status_bar(self):
        try:
            self.status_bar = QStatusBar()
            self.setStatusBar(self.status_bar)
            self.status_label = QLabel("Ready")
            self.status_bar.addPermanentWidget(self.status_label)
        except Exception as e:
            log.error(f"Build status bar error: {e}")

    # ---- Connect Signals ----

    def _connect_signals(self):
        try:
            self.btn_extract.clicked.connect(self._on_extract_fitgirl)
            self.btn_analyze.clicked.connect(self._on_analyze)
            self.btn_download_all.clicked.connect(self._on_download_all)
            self.btn_pause_all.clicked.connect(self._on_pause_all)
            self.btn_resume_all.clicked.connect(self._on_resume_all)
            self.btn_cancel_all.clicked.connect(self._on_cancel_all)
            self.btn_scheduler.clicked.connect(self._on_scheduler)
            self.btn_folder.clicked.connect(self._on_pick_folder)
            self.spin_concurrent.valueChanged.connect(self._on_concurrent_changed)
            self.sidebar.currentRowChanged.connect(self._on_category_changed)
            self.table.customContextMenuRequested.connect(self._on_context_menu)
            self.log_toggle_btn.clicked.connect(self._toggle_log_panel)
            self.log_signal.connect(self._append_log)
        except Exception as e:
            log.error(f"Connect signals error: {e}")

    # ---- Slots ----

    @pyqtSlot()
    def _toggle_input_panel(self):
        try:
            self.input_panel_visible = not self.input_panel_visible
            self.input_frame.setVisible(self.input_panel_visible)
            self.btn_toggle_input.setText(
                "Hide Links" if self.input_panel_visible else "Add Links"
            )
        except Exception as e:
            log.error(f"Toggle input panel error: {e}")

    @pyqtSlot()
    def _toggle_log_panel(self):
        try:
            visible = not self.log_text.isVisible()
            self.log_text.setVisible(visible)
        except Exception as e:
            log.error(f"Toggle log panel error: {e}")

    @pyqtSlot(str)
    def _append_log(self, msg: str):
        try:
            self.log_text.append(msg)
            # Auto-scroll
            sb = self.log_text.verticalScrollBar()
            sb.setValue(sb.maximum())
        except Exception:
            pass

    @pyqtSlot()
    def _on_extract_fitgirl(self):
        try:
            url = self.fitgirl_input.text().strip()
            if not url:
                self.status_label.setText("Enter a FitGirl repack URL first")
                return

            self.btn_extract.setEnabled(False)
            self.status_label.setText("Extracting links from FitGirl page...")

            self._fitgirl_worker = FitGirlScraperWorker(url)
            self._fitgirl_worker.finished.connect(self._on_fitgirl_done)
            self._fitgirl_worker.error.connect(self._on_fitgirl_error)
            self._fitgirl_worker.start()
        except Exception as e:
            log.error(f"Extract FitGirl error: {e}")
            self.btn_extract.setEnabled(True)

    @pyqtSlot(list)
    def _on_fitgirl_done(self, links: list):
        try:
            self.btn_extract.setEnabled(True)
            if links:
                existing = self.links_input.toPlainText().strip()
                new_text = "\n".join(links)
                if existing:
                    new_text = existing + "\n" + new_text
                self.links_input.setPlainText(new_text)
                self.status_label.setText(f"Extracted {len(links)} links")
            else:
                self.status_label.setText("No fuckingfast.co links found on that page")
        except Exception as e:
            log.error(f"FitGirl done handler error: {e}")
            self.btn_extract.setEnabled(True)

    @pyqtSlot(str)
    def _on_fitgirl_error(self, msg: str):
        try:
            self.btn_extract.setEnabled(True)
            self.status_label.setText(f"Error: {msg}")
            log.error(f"FitGirl extraction error: {msg}")
        except Exception:
            self.btn_extract.setEnabled(True)

    @pyqtSlot()
    def _on_analyze(self):
        try:
            text = self.links_input.toPlainText().strip()
            if not text:
                self.status_label.setText("No links to analyze")
                return

            urls = [line.strip() for line in text.splitlines() if line.strip()]
            urls = list(dict.fromkeys(urls))  # deduplicate

            if not urls:
                self.status_label.setText("No valid links found")
                return

            self.btn_analyze.setEnabled(False)
            self.resolve_total = len(urls)
            self.resolve_done = 0
            self.resolver_workers.clear()
            self._active_resolvers.clear()
            self._analyze_batch_queue.clear()
            self.status_label.setText(f"Analyzing 0/{self.resolve_total} links...")

            # Clear table, manager items, and add fresh items
            self.table.setSortingEnabled(False)
            self.table.setRowCount(0)
            self.manager.items.clear()
            self.manager.queue.clear()

            for url in urls:
                item = DownloadItem(
                    id=self.next_id,
                    page_url=url,
                    status=Status.ANALYZING
                )

                # Pre-extract filename from fragment
                try:
                    parsed = urlparse(url)
                    if parsed.fragment:
                        item.filename = unquote(parsed.fragment)
                        item.category = categorize_file(item.filename)
                except Exception:
                    pass

                self.manager.items[item.id] = item
                self._add_table_row(item)
                self._analyze_batch_queue.append((item.id, url))
                self.next_id += 1

            self.table.setSortingEnabled(True)
            log.info(f"Started analyzing {self.resolve_total} links (batched, max {self._max_concurrent_resolvers} concurrent)")

            # Start first batch of resolvers
            self._start_next_resolvers()

        except Exception as e:
            log.error(f"Analyze error: {e}")
            self.btn_analyze.setEnabled(True)

    def _start_next_resolvers(self):
        """Start resolver workers from the batch queue, up to the concurrency limit."""
        try:
            started = 0
            while (len(self._active_resolvers) < self._max_concurrent_resolvers
                   and self._analyze_batch_queue):
                item_id, url = self._analyze_batch_queue.pop(0)
                worker = LinkResolverWorker(item_id, url)
                worker.resolved.connect(self._on_link_resolved)
                worker.failed.connect(self._on_link_resolve_failed)
                worker.finished.connect(lambda w=worker: self._on_resolver_finished(w))
                self.resolver_workers.append(worker)
                self._active_resolvers.append(worker)
                worker.start()
                started += 1

            # If there are more in queue, schedule next batch with a small delay
            # to avoid hammering the server
            if self._analyze_batch_queue and started == 0:
                QTimer.singleShot(500, self._start_next_resolvers)
        except Exception as e:
            log.error(f"Start next resolvers error: {e}")

    def _on_resolver_finished(self, worker):
        """Called when a resolver thread finishes; starts the next one in queue."""
        try:
            if worker in self._active_resolvers:
                self._active_resolvers.remove(worker)
            self._start_next_resolvers()
        except Exception as e:
            log.error(f"Resolver finished handler error: {e}")

    def _add_table_row(self, item: DownloadItem):
        try:
            row = self.table.rowCount()
            self.table.insertRow(row)

            # Store item ID in first column
            id_item = QTableWidgetItem(str(item.id))
            id_item.setData(Qt.UserRole, item.id)
            id_item.setTextAlignment(Qt.AlignCenter)
            self.table.setItem(row, 0, id_item)

            self.table.setItem(row, 1, QTableWidgetItem(item.filename or "..."))
            self.table.setItem(row, 2, QTableWidgetItem(item.file_size_str or "..."))

            # Progress bar
            progress_bar = QProgressBar()
            progress_bar.setRange(0, 100)
            progress_bar.setValue(0)
            progress_bar.setFormat("%p%")
            self.table.setCellWidget(row, 3, progress_bar)

            self.table.setItem(row, 4, QTableWidgetItem("--"))  # Speed
            self.table.setItem(row, 5, QTableWidgetItem(item.status.value))
            self.table.setItem(row, 6, QTableWidgetItem("--"))  # ETA
            self.table.setItem(row, 7, QTableWidgetItem(item.category.value))

        except Exception as e:
            log.error(f"Add table row error: {e}")

    def _find_row_by_id(self, item_id: int) -> int:
        try:
            for row in range(self.table.rowCount()):
                cell = self.table.item(row, 0)
                if cell and cell.data(Qt.UserRole) == item_id:
                    return row
        except Exception:
            pass
        return -1

    @pyqtSlot(int, str, str, str)
    def _on_link_resolved(self, item_id: int, download_url: str, filename: str, size_str: str):
        try:
            item = self.manager.items.get(item_id)
            if not item:
                return

            item.download_url = download_url
            item.filename = filename
            item.file_size_str = size_str
            item.category = categorize_file(filename)
            item.status = Status.READY

            # Parse size string to bytes for sorting
            try:
                if size_str:
                    size_match = re.match(r"([\d.]+)\s*([KMGT]?)B?", size_str, re.IGNORECASE)
                    if size_match:
                        val = float(size_match.group(1))
                        unit = size_match.group(2).upper()
                        multipliers = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
                        item.file_size = int(val * multipliers.get(unit, 1))
            except Exception:
                pass

            # Update table row
            row = self._find_row_by_id(item_id)
            if row >= 0:
                self.table.setSortingEnabled(False)
                self.table.item(row, 1).setText(filename)
                self.table.item(row, 2).setText(size_str or "?")
                self.table.item(row, 5).setText(Status.READY.value)
                self.table.item(row, 7).setText(item.category.value)
                self.table.setSortingEnabled(True)

            self.resolve_done += 1
            self._update_analyze_progress()
            self._update_sidebar_counts()

        except Exception as e:
            log.error(f"Link resolved handler error: {e}")

    @pyqtSlot(int, str)
    def _on_link_resolve_failed(self, item_id: int, error_msg: str):
        try:
            item = self.manager.items.get(item_id)
            if item:
                item.status = Status.FAILED
                item.error_msg = error_msg

            row = self._find_row_by_id(item_id)
            if row >= 0:
                self.table.item(row, 5).setText(Status.FAILED.value)
                self.table.item(row, 5).setForeground(QColor("#ef4444"))

            self.resolve_done += 1
            self._update_analyze_progress()
            self._update_sidebar_counts()

        except Exception as e:
            log.error(f"Link resolve failed handler error: {e}")

    def _update_analyze_progress(self):
        try:
            self.status_label.setText(
                f"Analyzed {self.resolve_done}/{self.resolve_total} links"
            )
            if self.resolve_done >= self.resolve_total:
                self.btn_analyze.setEnabled(True)
                ready = sum(1 for i in self.manager.items.values() if i.status == Status.READY)
                failed = sum(1 for i in self.manager.items.values() if i.status == Status.FAILED)
                self.status_label.setText(
                    f"Analysis complete: {ready} ready, {failed} failed"
                )
                # Collapse input panel
                if ready > 0:
                    self.input_frame.setVisible(False)
                    self.input_panel_visible = False
                    self.btn_toggle_input.setText("Add Links")
                log.info(f"Analysis complete: {ready} ready, {failed} failed")

                # Auto-start downloads if flag was set
                if self._auto_download_after_analyze and ready > 0:
                    self._auto_download_after_analyze = False
                    log.info("Auto-starting downloads after analysis completed")
                    self.status_label.setText(f"Analysis done ({ready} ready) -- starting downloads...")
                    # Use a short delay so the UI updates before downloads begin
                    QTimer.singleShot(500, self._on_download_all)
        except Exception as e:
            log.error(f"Update analyze progress error: {e}")

    @pyqtSlot()
    def _on_download_all(self):
        try:
            # Check if there are READY items to download
            ready_count = sum(1 for i in self.manager.items.values() if i.status == Status.READY)

            # If nothing is ready, check if we need to analyze first
            if ready_count == 0:
                text = self.links_input.toPlainText().strip()
                if text and not self.manager.items:
                    # Links exist but haven't been analyzed yet -- auto-analyze first
                    log.info("No analyzed links yet -- auto-triggering analysis before download")
                    self.status_label.setText("Analyzing links first, downloads will start automatically...")
                    self._auto_download_after_analyze = True
                    self._on_analyze()
                    return
                elif any(i.status == Status.ANALYZING for i in self.manager.items.values()):
                    # Analysis is still running -- set flag to auto-start when done
                    log.info("Analysis still in progress -- downloads will start automatically when done")
                    self.status_label.setText("Analysis in progress... downloads will start automatically when done")
                    self._auto_download_after_analyze = True
                    return
                else:
                    self.status_label.setText("No links ready to download. Paste links and click Analyze first.")
                    log.info("Download All clicked but no READY items found")
                    return

            save_dir = self.manager.save_dir
            try:
                os.makedirs(save_dir, exist_ok=True)
            except OSError as e:
                log.error(f"Cannot create download folder: {e}")
                self.status_label.setText(f"Error: Cannot create folder {save_dir}")
                return

            # Queue all READY items
            queued = 0
            for item_id, item in self.manager.items.items():
                if item.status == Status.READY:
                    item.status = Status.WAITING
                    self.manager.queue.append(item_id)
                    queued += 1
                    row = self._find_row_by_id(item_id)
                    if row >= 0:
                        self.table.item(row, 5).setText(Status.WAITING.value)

            self._start_next_downloads()
            self._update_status_bar()
            log.info(f"Queued {queued} downloads ({len(self.manager.queue)} in queue)")
        except Exception as e:
            log.error(f"Download all error: {e}")

    def _start_next_downloads(self):
        """Start downloads from queue up to max concurrent limit."""
        try:
            while self.manager.can_start_more() and self.manager.queue:
                item_id = self.manager.queue.pop(0)
                item = self.manager.items.get(item_id)
                if not item or item.status not in (Status.WAITING, Status.RETRYING):
                    continue

                self._start_download(item)
        except Exception as e:
            log.error(f"Start next downloads error: {e}")

    def _start_download(self, item: DownloadItem):
        """Start a single download worker."""
        try:
            item.status = Status.DOWNLOADING
            item.downloaded = 0
            item.speed = 0

            worker = DownloadWorker(item, self.manager.save_dir)
            worker.progress.connect(self._on_download_progress)
            worker.speed_update.connect(self._on_download_speed)
            worker.status_changed.connect(self._on_download_status)
            worker.finished.connect(self._on_download_finished)
            worker.error.connect(self._on_download_error)
            worker.finished.connect(lambda: self._start_next_downloads())
            worker.error.connect(lambda: self._start_next_downloads())

            self.manager.workers[item.id] = worker
            worker.start()

            row = self._find_row_by_id(item.id)
            if row >= 0:
                self.table.item(row, 5).setText(Status.DOWNLOADING.value)
                self.table.item(row, 5).setForeground(QColor("#7c3aed"))

            log.info(f"Download started: {item.filename}")
        except Exception as e:
            log.error(f"Start download error for {item.filename}: {e}")

    @pyqtSlot(int, int, int)
    def _on_download_progress(self, item_id: int, downloaded: int, total: int):
        try:
            item = self.manager.items.get(item_id)
            if item:
                item.downloaded = downloaded
                if total > 0:
                    item.file_size = total

            row = self._find_row_by_id(item_id)
            if row >= 0:
                progress_bar = self.table.cellWidget(row, 3)
                if progress_bar and total > 0:
                    pct = int((downloaded / total) * 100)
                    progress_bar.setValue(pct)
                    progress_bar.setFormat(
                        f"{self._format_size(downloaded)} / {self._format_size(total)} ({pct}%)"
                    )
            self._update_status_bar()
        except Exception:
            pass

    @pyqtSlot(int, float)
    def _on_download_speed(self, item_id: int, speed: float):
        try:
            item = self.manager.items.get(item_id)
            if item:
                item.speed = speed

            row = self._find_row_by_id(item_id)
            if row >= 0:
                self.table.item(row, 4).setText(self._format_speed(speed))

                # Calculate ETA
                if item and item.file_size > 0 and speed > 0:
                    remaining = item.file_size - item.downloaded
                    eta_secs = remaining / speed
                    self.table.item(row, 6).setText(self._format_eta(eta_secs))
                else:
                    self.table.item(row, 6).setText("--")
        except Exception:
            pass

    @pyqtSlot(int, str)
    def _on_download_status(self, item_id: int, status_str: str):
        try:
            item = self.manager.items.get(item_id)
            if item:
                item.status = Status(status_str)

            row = self._find_row_by_id(item_id)
            if row >= 0:
                status_item = self.table.item(row, 5)
                status_item.setText(status_str)
                if status_str == Status.COMPLETED.value:
                    status_item.setForeground(QColor("#22c55e"))
                elif status_str == Status.FAILED.value:
                    status_item.setForeground(QColor("#ef4444"))
                elif status_str == Status.PAUSED.value:
                    status_item.setForeground(QColor("#f59e0b"))
                elif status_str == Status.DOWNLOADING.value:
                    status_item.setForeground(QColor("#7c3aed"))
                else:
                    status_item.setForeground(QColor("#e0e0e0"))

            self._update_sidebar_counts()
            self._update_status_bar()
        except Exception:
            pass

    @pyqtSlot(int)
    def _on_download_finished(self, item_id: int):
        try:
            item = self.manager.items.get(item_id)
            if item:
                item.status = Status.COMPLETED
                item.speed = 0
                row = self._find_row_by_id(item_id)
                if row >= 0:
                    self.table.item(row, 4).setText("--")
                    self.table.item(row, 6).setText("Done")

            self._update_sidebar_counts()
            self._update_status_bar()
        except Exception as e:
            log.error(f"Download finished handler error: {e}")

    @pyqtSlot(int, str)
    def _on_download_error(self, item_id: int, error_msg: str):
        try:
            item = self.manager.items.get(item_id)
            if not item:
                return

            item.speed = 0
            item.retries += 1

            row = self._find_row_by_id(item_id)

            if item.retries <= item.max_retries:
                # Auto-retry with backoff
                delay = 2 ** item.retries  # 2s, 4s, 8s
                log.warning(
                    f"Retry {item.retries}/{item.max_retries} for {item.filename}: "
                    f"{error_msg} (waiting {delay}s)"
                )
                item.status = Status.RETRYING

                if row >= 0:
                    self.table.item(row, 5).setText(f"Retry {item.retries}/{item.max_retries}")
                    self.table.item(row, 5).setForeground(QColor("#f59e0b"))
                    self.table.item(row, 4).setText("--")

                # Schedule retry
                QTimer.singleShot(delay * 1000, lambda: self._retry_download(item_id))
            else:
                log.error(f"Failed after {item.max_retries} retries: {item.filename} - {error_msg}")
                item.status = Status.FAILED
                item.error_msg = error_msg

                if row >= 0:
                    self.table.item(row, 5).setText(Status.FAILED.value)
                    self.table.item(row, 5).setForeground(QColor("#ef4444"))
                    self.table.item(row, 4).setText("--")
                    self.table.item(row, 6).setText("--")

            self._update_sidebar_counts()
            self._update_status_bar()
        except Exception as e:
            log.error(f"Download error handler error: {e}")

    def _retry_download(self, item_id: int):
        try:
            item = self.manager.items.get(item_id)
            if item and item.status == Status.RETRYING:
                self._start_download(item)
        except Exception as e:
            log.error(f"Retry download error: {e}")

    @pyqtSlot()
    def _on_pause_all(self):
        try:
            for item_id, worker in self.manager.workers.items():
                try:
                    item = self.manager.items.get(item_id)
                    if item and item.status == Status.DOWNLOADING:
                        worker.pause()
                        item.status = Status.PAUSED
                        row = self._find_row_by_id(item_id)
                        if row >= 0:
                            self.table.item(row, 5).setText(Status.PAUSED.value)
                            self.table.item(row, 5).setForeground(QColor("#f59e0b"))
                except Exception as e:
                    log.error(f"Pause error for item {item_id}: {e}")

            self._update_sidebar_counts()
            self._update_status_bar()
            log.info("Paused all downloads")
        except Exception as e:
            log.error(f"Pause all error: {e}")

    @pyqtSlot()
    def _on_resume_all(self):
        try:
            for item_id, worker in self.manager.workers.items():
                try:
                    item = self.manager.items.get(item_id)
                    if item and item.status == Status.PAUSED:
                        worker.resume()
                        item.status = Status.DOWNLOADING
                        row = self._find_row_by_id(item_id)
                        if row >= 0:
                            self.table.item(row, 5).setText(Status.DOWNLOADING.value)
                            self.table.item(row, 5).setForeground(QColor("#7c3aed"))
                except Exception as e:
                    log.error(f"Resume error for item {item_id}: {e}")

            self._update_sidebar_counts()
            self._update_status_bar()
            log.info("Resumed all downloads")
        except Exception as e:
            log.error(f"Resume all error: {e}")

    @pyqtSlot()
    def _on_cancel_all(self):
        try:
            # Clear the queue
            self.manager.queue.clear()

            for item_id, worker in self.manager.workers.items():
                try:
                    item = self.manager.items.get(item_id)
                    if item and item.status in (Status.DOWNLOADING, Status.PAUSED, Status.WAITING):
                        worker.cancel()
                        item.status = Status.CANCELLED
                        row = self._find_row_by_id(item_id)
                        if row >= 0:
                            self.table.item(row, 5).setText(Status.CANCELLED.value)
                            self.table.item(row, 5).setForeground(QColor("#ef4444"))
                            self.table.item(row, 4).setText("--")
                except Exception as e:
                    log.error(f"Cancel error for item {item_id}: {e}")

            # Also cancel waiting items
            for item_id, item in self.manager.items.items():
                try:
                    if item.status == Status.WAITING:
                        item.status = Status.CANCELLED
                        row = self._find_row_by_id(item_id)
                        if row >= 0:
                            self.table.item(row, 5).setText(Status.CANCELLED.value)
                except Exception:
                    pass

            self._update_sidebar_counts()
            self._update_status_bar()
            log.info("Cancelled all downloads")
        except Exception as e:
            log.error(f"Cancel all error: {e}")

    @pyqtSlot()
    def _on_scheduler(self):
        try:
            dialog = SchedulerDialog(self)
            if dialog.exec_() == QDialog.Accepted:
                scheduled = dialog.get_datetime()
                now = QDateTime.currentDateTime()

                if scheduled <= now:
                    self.status_label.setText("Scheduled time must be in the future")
                    return

                self.scheduled_time = scheduled
                delay_ms = now.msecsTo(scheduled)

                # Set one-shot timer
                if self.scheduled_timer:
                    self.scheduled_timer.stop()
                self.scheduled_timer = QTimer(self)
                self.scheduled_timer.setSingleShot(True)
                self.scheduled_timer.timeout.connect(self._on_scheduled_start)
                self.scheduled_timer.start(delay_ms)

                # Countdown display timer
                if self.countdown_timer:
                    self.countdown_timer.stop()
                self.countdown_timer = QTimer(self)
                self.countdown_timer.timeout.connect(self._update_countdown)
                self.countdown_timer.start(1000)

                self.status_label.setText(
                    f"Downloads scheduled for {scheduled.toString('yyyy-MM-dd hh:mm:ss')}"
                )
                log.info(f"Downloads scheduled for {scheduled.toString('yyyy-MM-dd hh:mm:ss')}")
        except Exception as e:
            log.error(f"Scheduler error: {e}")

    def _on_scheduled_start(self):
        try:
            if self.countdown_timer:
                self.countdown_timer.stop()
            self.scheduled_time = None
            self.status_label.setText("Scheduled time reached - starting downloads...")
            log.info("Scheduled time reached - starting downloads")
            self._on_download_all()
        except Exception as e:
            log.error(f"Scheduled start error: {e}")

    def _update_countdown(self):
        try:
            if self.scheduled_time:
                now = QDateTime.currentDateTime()
                remaining_secs = now.secsTo(self.scheduled_time)
                if remaining_secs <= 0:
                    return
                h = remaining_secs // 3600
                m = (remaining_secs % 3600) // 60
                s = remaining_secs % 60
                self.status_label.setText(
                    f"Downloads start in {h:02d}:{m:02d}:{s:02d}"
                )
        except Exception:
            pass

    @pyqtSlot()
    def _on_pick_folder(self):
        try:
            folder = QFileDialog.getExistingDirectory(
                self, "Select Download Folder", self.manager.save_dir
            )
            if folder:
                self.manager.save_dir = folder
                self.status_label.setText(f"Download folder: {folder}")
                log.info(f"Download folder changed to: {folder}")
        except Exception as e:
            log.error(f"Pick folder error: {e}")

    @pyqtSlot(int)
    def _on_concurrent_changed(self, value: int):
        try:
            self.manager.set_max_concurrent(value)
            log.info(f"Max concurrent downloads set to {value}")
            self._start_next_downloads()
        except Exception as e:
            log.error(f"Concurrent changed error: {e}")

    @pyqtSlot(int)
    def _on_category_changed(self, row: int):
        try:
            item = self.sidebar.item(row)
            if not item:
                return
            cat = item.data(Qt.UserRole)
            self._filter_table(cat)
        except Exception as e:
            log.error(f"Category changed error: {e}")

    def _filter_table(self, category: Category):
        try:
            for row in range(self.table.rowCount()):
                cell = self.table.item(row, 0)
                if not cell:
                    continue
                item_id = cell.data(Qt.UserRole)
                item = self.manager.items.get(item_id)
                if not item:
                    self.table.setRowHidden(row, True)
                    continue

                show = False
                if category == Category.ALL:
                    show = True
                elif category == Category.COMPLETED:
                    show = item.status == Status.COMPLETED
                elif category == Category.FAILED:
                    show = item.status in (Status.FAILED, Status.CANCELLED)
                else:
                    show = item.category == category

                self.table.setRowHidden(row, not show)
        except Exception as e:
            log.error(f"Filter table error: {e}")

    def _update_sidebar_counts(self):
        try:
            counts = {cat: 0 for cat in Category}
            for item in self.manager.items.values():
                counts[Category.ALL] += 1
                counts[item.category] += 1
                if item.status == Status.COMPLETED:
                    counts[Category.COMPLETED] += 1
                elif item.status in (Status.FAILED, Status.CANCELLED):
                    counts[Category.FAILED] += 1

            for i in range(self.sidebar.count()):
                list_item = self.sidebar.item(i)
                cat = list_item.data(Qt.UserRole)
                count = counts.get(cat, 0)
                list_item.setText(f"{cat.value}  ({count})")
        except Exception as e:
            log.error(f"Update sidebar counts error: {e}")

    def _on_context_menu(self, pos):
        try:
            row = self.table.rowAt(pos.y())
            if row < 0:
                return

            cell = self.table.item(row, 0)
            if not cell:
                return
            item_id = cell.data(Qt.UserRole)
            item = self.manager.items.get(item_id)
            if not item:
                return

            menu = QMenu(self)

            if item.status == Status.DOWNLOADING:
                pause_action = menu.addAction("Pause")
                pause_action.triggered.connect(lambda: self._pause_single(item_id))
            elif item.status == Status.PAUSED:
                resume_action = menu.addAction("Resume")
                resume_action.triggered.connect(lambda: self._resume_single(item_id))
            elif item.status in (Status.FAILED, Status.CANCELLED):
                retry_action = menu.addAction("Retry")
                retry_action.triggered.connect(lambda: self._retry_single(item_id))

            if item.status in (Status.DOWNLOADING, Status.PAUSED, Status.WAITING):
                cancel_action = menu.addAction("Cancel")
                cancel_action.triggered.connect(lambda: self._cancel_single(item_id))

            menu.addSeparator()

            if item.status == Status.COMPLETED:
                open_action = menu.addAction("Open Folder")
                open_action.triggered.connect(lambda: self._open_folder(item))

            copy_action = menu.addAction("Copy URL")
            copy_action.triggered.connect(
                lambda: QApplication.clipboard().setText(item.download_url or item.page_url)
            )

            menu.exec_(self.table.viewport().mapToGlobal(pos))
        except Exception as e:
            log.error(f"Context menu error: {e}")

    def _pause_single(self, item_id: int):
        try:
            worker = self.manager.workers.get(item_id)
            item = self.manager.items.get(item_id)
            if worker and item:
                worker.pause()
                item.status = Status.PAUSED
                row = self._find_row_by_id(item_id)
                if row >= 0:
                    self.table.item(row, 5).setText(Status.PAUSED.value)
                    self.table.item(row, 5).setForeground(QColor("#f59e0b"))
            self._update_sidebar_counts()
        except Exception as e:
            log.error(f"Pause single error: {e}")

    def _resume_single(self, item_id: int):
        try:
            worker = self.manager.workers.get(item_id)
            item = self.manager.items.get(item_id)
            if worker and item:
                worker.resume()
                item.status = Status.DOWNLOADING
                row = self._find_row_by_id(item_id)
                if row >= 0:
                    self.table.item(row, 5).setText(Status.DOWNLOADING.value)
                    self.table.item(row, 5).setForeground(QColor("#7c3aed"))
            self._update_sidebar_counts()
        except Exception as e:
            log.error(f"Resume single error: {e}")

    def _cancel_single(self, item_id: int):
        try:
            worker = self.manager.workers.get(item_id)
            item = self.manager.items.get(item_id)
            if item:
                if worker:
                    worker.cancel()
                item.status = Status.CANCELLED
                row = self._find_row_by_id(item_id)
                if row >= 0:
                    self.table.item(row, 5).setText(Status.CANCELLED.value)
                    self.table.item(row, 5).setForeground(QColor("#ef4444"))
                    self.table.item(row, 4).setText("--")

                # Remove from queue if waiting
                if item_id in self.manager.queue:
                    self.manager.queue.remove(item_id)
            self._update_sidebar_counts()
        except Exception as e:
            log.error(f"Cancel single error: {e}")

    def _retry_single(self, item_id: int):
        try:
            item = self.manager.items.get(item_id)
            if item and item.download_url:
                item.retries = 0
                item.status = Status.WAITING
                item.downloaded = 0
                item.speed = 0
                self.manager.queue.append(item_id)

                row = self._find_row_by_id(item_id)
                if row >= 0:
                    self.table.item(row, 5).setText(Status.WAITING.value)
                    self.table.item(row, 5).setForeground(QColor("#e0e0e0"))
                    progress_bar = self.table.cellWidget(row, 3)
                    if progress_bar:
                        progress_bar.setValue(0)
                        progress_bar.setFormat("%p%")

                self._start_next_downloads()
                self._update_sidebar_counts()
                log.info(f"Manual retry queued: {item.filename}")
        except Exception as e:
            log.error(f"Retry single error: {e}")

    def _open_folder(self, item: DownloadItem):
        try:
            filepath = os.path.join(self.manager.save_dir, item.filename)
            if os.path.exists(filepath):
                os.startfile(os.path.dirname(filepath))
            else:
                os.startfile(self.manager.save_dir)
        except Exception as e:
            log.error(f"Open folder error: {e}")

    def _update_status_bar(self):
        try:
            total_items = len(self.manager.items)
            downloading = sum(1 for i in self.manager.items.values() if i.status == Status.DOWNLOADING)
            paused = sum(1 for i in self.manager.items.values() if i.status == Status.PAUSED)
            completed = sum(1 for i in self.manager.items.values() if i.status == Status.COMPLETED)
            failed = sum(1 for i in self.manager.items.values() if i.status in (Status.FAILED, Status.CANCELLED))
            waiting = sum(1 for i in self.manager.items.values() if i.status == Status.WAITING)

            total_downloaded = sum(i.downloaded for i in self.manager.items.values())
            total_size = sum(i.file_size for i in self.manager.items.values() if i.file_size > 0)
            total_speed = sum(
                i.speed for i in self.manager.items.values()
                if i.status == Status.DOWNLOADING
            )

            parts = []
            if downloading > 0 or waiting > 0:
                parts.append(f"Downloading {downloading}/{total_items}")
            if waiting > 0:
                parts.append(f"Queued: {waiting}")
            if paused > 0:
                parts.append(f"Paused: {paused}")
            parts.append(f"Done: {completed}")
            if failed > 0:
                parts.append(f"Failed: {failed}")

            if total_size > 0:
                parts.append(
                    f"{self._format_size(total_downloaded)} / {self._format_size(total_size)}"
                )
            if total_speed > 0:
                parts.append(f"Speed: {self._format_speed(total_speed)}")

            # Don't overwrite scheduler countdown
            if not self.scheduled_time:
                self.status_label.setText(" | ".join(parts) if parts else "Ready")
        except Exception:
            pass

    # ---- Formatting helpers ----

    @staticmethod
    def _format_size(b: int) -> str:
        try:
            if b <= 0:
                return "0 B"
            for unit in ("B", "KB", "MB", "GB"):
                if b < 1024:
                    return f"{b:.1f} {unit}"
                b /= 1024
            return f"{b:.1f} TB"
        except Exception:
            return "?"

    @staticmethod
    def _format_speed(bps: float) -> str:
        try:
            if bps <= 0:
                return "--"
            if bps < 1024:
                return f"{bps:.0f} B/s"
            elif bps < 1024 ** 2:
                return f"{bps/1024:.1f} KB/s"
            else:
                return f"{bps/(1024**2):.1f} MB/s"
        except Exception:
            return "?"

    @staticmethod
    def _format_eta(seconds: float) -> str:
        try:
            if seconds <= 0 or seconds > 86400 * 7:
                return "--"
            s = int(seconds)
            if s < 60:
                return f"{s}s"
            m, s = divmod(s, 60)
            if m < 60:
                return f"{m}m {s:02d}s"
            h, m = divmod(m, 60)
            return f"{h}h {m:02d}m"
        except Exception:
            return "?"


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

def main():
    try:
        app = QApplication(sys.argv)
        app.setStyleSheet(DARK_STYLESHEET)
        app.setStyle("Fusion")

        window = MainWindow()
        window.show()

        log.info("Application window shown")
        sys.exit(app.exec_())
    except Exception as e:
        log.error(f"Application startup error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
