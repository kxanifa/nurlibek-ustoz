import logging
import sqlite3
import json
import io
import os
import re
import time
import asyncio
import contextlib
from datetime import datetime

import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher.filters import Text
from aiogram.utils import executor
from aiogram.utils.exceptions import (
    RetryAfter,
    BotBlocked,
    ChatNotFound,
    UserDeactivated,
    TelegramAPIError,
)
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputFile,
    MediaGroup,
    WebAppInfo,
)

# PDF (ReportLab)
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import (
    SimpleDocTemplate,
    Table,
    TableStyle,
    Paragraph,
    Spacer,
    Image as ReportLabImage,
    PageBreak,
)
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

# Serverda GUI oynasiz ishlashi uchun Agg backend
matplotlib.use("Agg")


# ================= .ENV FAYLNI AVTOMATIK YUKLASH =================
def load_env_file(filepath: str = ".env"):
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filepath)
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip("'").strip('"')
            if key and key not in os.environ:
                os.environ[key] = val


load_env_file()

# ================= SOZLAMALAR =================
API_TOKEN = (
    os.environ.get("BOT_TOKEN")
    or os.environ.get("TELEGRAM_BOT_TOKEN")
    or "7058354398:AAFmuKIJ2N3gvpqm6EYeyQVxukYk6UTDBiM"
)

ADMINS_ENV = os.environ.get("ADMINS")
if ADMINS_ENV:
    try:
        ADMINS = [int(x.strip()) for x in ADMINS_ENV.split(",") if x.strip()]
    except Exception:
        ADMINS = [1486347042, 1033652624]
else:
    ADMINS = [1486347042, 1033652624]

# Railway / Docker Volume tekshiruvi
if os.path.exists("/app/data"):
    DB_FILE = "/app/data/bot_database.db"
elif os.environ.get("DATA_DIR") and os.path.exists(os.environ.get("DATA_DIR")):
    DB_FILE = os.path.join(os.environ.get("DATA_DIR"), "bot_database.db")
else:
    DB_FILE = "bot_database.db"

WEB_APP_URL = os.environ.get(
    "WEB_APP_URL", "https://kxanifa.github.io/lochin-math"
).rstrip("/")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ================= RAM KESHLAR (100K HIGH-LOAD UCHUN) =================
_channel_cache = None
_known_users = set()
_test_cache = {}  # {code: (timestamp, test_dict)}
_sub_cache = {}  # {user_id: (expire_ts, is_subscribed)}

TEST_CACHE_TTL = 30.0  # 30 soniya
SUB_CACHE_TTL = 120.0  # 2 daqiqa (Telegram API FloodWait oldini oladi)
MAX_PDF_ROWS = 300  # PDF hisobotda OOM bo'lmasligi uchun maksimal qatorlar (Excelda 100% hammasi chiqadi)


# ================= 1. DATABASE (HIGH-CONCURRENCY WAL & AUTO-MIGRATION) =================
@contextlib.contextmanager
def get_db(immediate: bool = False):
    """
    100k foydalanuvchi uchun optimallashtirilgan SQLite ulanishi:
    - WAL (Write-Ahead Logging) rejimi bir vaqtda o'qish va yozish imkonini beradi
    - 30 soniyalik timeout 'database is locked' xatosini bartaraf qiladi
    - immediate=True atomik o'qish-yozish tranzaksiyalari (race condition oldini olish) uchun
    """
    conn = sqlite3.connect(DB_FILE, timeout=30.0)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute("PRAGMA cache_size = -64000;")  # 64 MB RAM cache
    conn.execute("PRAGMA busy_timeout = 30000;")
    conn.row_factory = sqlite3.Row
    try:
        if immediate:
            conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def db_start():
    with get_db() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                full_name TEXT,
                username TEXT,
                joined_at DATETIME
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS tests (
                code TEXT PRIMARY KEY,
                pdf_id TEXT,
                keys TEXT,
                stats TEXT,
                deadline TEXT,
                test_type TEXT DEFAULT 'rash'
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                test_code TEXT,
                user_id INTEGER,
                full_name TEXT,
                correct_count INTEGER,
                percent REAL,
                grade TEXT,
                score_75 REAL,
                final_score REAL,
                user_answers TEXT,
                date DATETIME
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )"""
        )

        # --- AVTOMATIK MIGRATSIYA (Eski bazalar bilan 100% moslik) ---
        test_cols = {
            r["name"] for r in conn.execute("PRAGMA table_info(tests)").fetchall()
        }
        if "test_type" not in test_cols:
            conn.execute("ALTER TABLE tests ADD COLUMN test_type TEXT DEFAULT 'rash'")
        if "category" in test_cols and "test_type" in test_cols:
            conn.execute(
                "UPDATE tests SET test_type = category WHERE test_type IS NULL AND category IS NOT NULL"
            )

        res_cols = {
            r["name"] for r in conn.execute("PRAGMA table_info(results)").fetchall()
        }
        if "final_score" not in res_cols:
            conn.execute("ALTER TABLE results ADD COLUMN final_score REAL DEFAULT 0")
            conn.execute(
                "UPDATE results SET final_score = score_75 WHERE final_score = 0 OR final_score IS NULL"
            )
        if "user_answers" not in res_cols:
            conn.execute("ALTER TABLE results ADD COLUMN user_answers TEXT")
        if "percent" not in res_cols:
            conn.execute("ALTER TABLE results ADD COLUMN percent REAL DEFAULT 0")
        if "score_75" not in res_cols and "rash_score_75" in res_cols:
            conn.execute("ALTER TABLE results ADD COLUMN score_75 REAL DEFAULT 0")
            conn.execute("UPDATE results SET score_75 = rash_score_75")

        # --- 100K FOYDALANUVCHI UCHUN TEZKOR INDEKSLAR ---
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_test_code ON results (test_code)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_user_id ON results (user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_test_user ON results (test_code, user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_date ON results (date DESC)"
        )

        # Mavjud userlarni RAM keshga yuklab olish (har /start da DB ni qiynamaslik uchun)
        cur = conn.execute("SELECT user_id FROM users")
        for r in cur.fetchall():
            _known_users.add(r["user_id"])


# --- DB Yordamchi Funksiyalari ---
def set_channel(username):
    global _channel_cache
    clean = username.strip() if username else None
    if clean and not clean.startswith("@") and not clean.startswith("-100"):
        clean = "@" + clean
    _channel_cache = clean if clean else ""
    _sub_cache.clear()
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO config (key, value) VALUES ('channel', ?)",
            (clean,),
        )


def get_channel():
    global _channel_cache
    if _channel_cache is not None:
        return _channel_cache if _channel_cache else None
    with get_db() as conn:
        cur = conn.execute("SELECT value FROM config WHERE key='channel'")
        row = cur.fetchone()
        _channel_cache = row["value"] if row and row["value"] else ""
    return _channel_cache if _channel_cache else None


def add_user(user_id: int, full_name: str, username: str):
    if user_id in _known_users:
        return
    _known_users.add(user_id)
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (user_id, full_name, username, joined_at) VALUES (?, ?, ?, ?)",
            (user_id, full_name, username, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )


def iter_all_users(batch_size: int = 2000):
    """100k userni xotirani to'ldirmasdan (cursor batch orqali) o'qish"""
    with get_db() as conn:
        cur = conn.execute("SELECT user_id FROM users")
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for r in rows:
                yield r["user_id"]


def get_system_stats():
    with get_db() as conn:
        u_cnt = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
        t_cnt = conn.execute("SELECT COUNT(*) as c FROM tests").fetchone()["c"]
        r_cnt = conn.execute("SELECT COUNT(*) as c FROM results").fetchone()["c"]
    return u_cnt, t_cnt, r_cnt


def save_test(code: str, pdf_id: str, keys_list: list, deadline: str, test_type: str):
    norm_keys = [normalize_answer(k) for k in keys_list]
    stats = [{"total": 0, "correct": 0} for _ in range(len(norm_keys))]
    with get_db() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO tests (code, pdf_id, keys, stats, deadline, test_type)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                code,
                pdf_id,
                json.dumps(norm_keys),
                json.dumps(stats),
                deadline,
                test_type,
            ),
        )
    _test_cache.pop(code, None)


def delete_test(code: str):
    with get_db() as conn:
        conn.execute("DELETE FROM tests WHERE code=?", (code,))
        conn.execute("DELETE FROM results WHERE test_code=?", (code,))
    _test_cache.pop(code, None)


def get_test(code: str, use_cache: bool = True):
    now = time.monotonic()
    if use_cache and code in _test_cache:
        ts, cached = _test_cache[code]
        if now - ts < TEST_CACHE_TTL:
            return cached

    with get_db() as conn:
        cur = conn.execute("SELECT * FROM tests WHERE code=?", (code,))
        row = cur.fetchone()
        if not row:
            return None

        try:
            keys = json.loads(row["keys"])
        except Exception:
            keys = list(row["keys"])

        try:
            stats = json.loads(row["stats"]) if row["stats"] else []
        except Exception:
            stats = [{"total": 0, "correct": 0} for _ in range(len(keys))]

        t_type = row["test_type"] if "test_type" in row.keys() and row["test_type"] else "rash"

        test_data = {
            "code": row["code"],
            "pdf": row["pdf_id"],
            "keys": keys,
            "stats": stats,
            "deadline": row["deadline"],
            "type": t_type,
        }
        _test_cache[code] = (now, test_data)
        return test_data


def get_user_test_attempt(test_code: str, user_id: int):
    with get_db() as conn:
        cur = conn.execute(
            "SELECT * FROM results WHERE test_code=? AND user_id=? LIMIT 1",
            (test_code, user_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def record_test_attempt_atomic(
    test_code: str,
    user_id: int,
    full_name: str,
    user_answers: list,
):
    """
    100k o'quvchi bir vaqtda javob yuborganda Race Condition bo'lmasligi uchun:
    - BEGIN IMMEDIATE orqali bazadan eng so'nggi stats o'qiladi
    - Agar foydalanuvchi bu testni avval topshirgan bo'lsa, statistika buzilmaydi (is_first_attempt=False)
    - Birinchi marta topshirayotgan bo'lsa, stats yangilanadi va natija saqlanadi
    """
    with get_db(immediate=True) as conn:
        cur = conn.execute("SELECT * FROM tests WHERE code=?", (test_code,))
        t_row = cur.fetchone()
        if not t_row:
            return None

        try:
            keys = json.loads(t_row["keys"])
        except Exception:
            keys = list(t_row["keys"])

        try:
            stats = json.loads(t_row["stats"]) if t_row["stats"] else []
        except Exception:
            stats = []

        t_type = t_row["test_type"] if "test_type" in t_row.keys() and t_row["test_type"] else "rash"

        while len(stats) < len(keys):
            stats.append({"total": 0, "correct": 0})

        # Avval yechgan-yechmaganini tekshirish
        existing = conn.execute(
            "SELECT id, correct_count, percent, grade, score_75, final_score, date FROM results WHERE test_code=? AND user_id=? LIMIT 1",
            (test_code, user_id),
        ).fetchone()

        # Shu urinishni baholash (joriy yangilangan stats bilan)
        temp_stats = [dict(s) for s in stats]
        _, vec, _, _, _, _ = evaluate_vector(user_answers, keys)
        for i, is_corr in enumerate(vec):
            if i < len(temp_stats):
                temp_stats[i]["total"] = temp_stats[i].get("total", 0) + 1
                if is_corr:
                    temp_stats[i]["correct"] = temp_stats[i].get("correct", 0) + 1

        corr, vec, perc, grade, score_75, final_score = calculate_score(
            user_answers, keys, t_type, temp_stats if not existing else stats
        )

        if existing:
            return {
                "is_first_attempt": False,
                "existing": dict(existing),
                "correct": corr,
                "total": len(keys),
                "vector": vec,
                "percent": perc,
                "grade": grade,
                "score_75": score_75,
                "final_score": final_score,
                "type": t_type,
            }

        # Yangi urinish: stats ni yangilash va natijani yozish
        conn.execute(
            "UPDATE tests SET stats=? WHERE code=?",
            (json.dumps(temp_stats), test_code),
        )
        conn.execute(
            """INSERT INTO results
               (test_code, user_id, full_name, correct_count, percent, grade, score_75, final_score, user_answers, date)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                test_code,
                user_id,
                full_name,
                corr,
                perc,
                grade,
                score_75,
                final_score,
                json.dumps(user_answers),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    _test_cache.pop(test_code, None)
    return {
        "is_first_attempt": True,
        "correct": corr,
        "total": len(keys),
        "vector": vec,
        "percent": perc,
        "grade": grade,
        "score_75": score_75,
        "final_score": final_score,
        "type": t_type,
    }


def get_results_list(test_code: str):
    with get_db() as conn:
        cur = conn.execute("SELECT * FROM results WHERE test_code=?", (test_code,))
        return [dict(r) for r in cur.fetchall()]


def get_user_history(user_id: int, limit: int = 25):
    with get_db() as conn:
        cur = conn.execute(
            """SELECT test_code, correct_count, percent, grade, score_75, final_score, date
               FROM results WHERE user_id=? ORDER BY id DESC LIMIT ?""",
            (user_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]


def get_all_test_codes():
    with get_db() as conn:
        cur = conn.execute("SELECT code FROM tests ORDER BY rowid DESC")
        return [r["code"] for r in cur.fetchall()]


# ================= 2. MATEMATIK VA MANTIQIY HISOBLASH (RASH + DTM) =================
def normalize_answer(val) -> str:
    """
    Javob va kalitlarni yagona standart formatga keltiradi:
    - 'a)5b)10', 'a) 5 b) 10', 'a:5;b:10', '5|10' -> '5|10'
    - '2,5' -> '2.5' (o'nli kasrlar)
    - Bo'shliqlar va kichik harflarni tozalaydi
    """
    if val is None:
        return "X"
    s = str(val).strip().upper()
    if not s or s == "X":
        return "X"

    # O'nli kasrlarda vergulni nuqtaga almashtirish (masalan: 2,5 -> 2.5)
    s = re.sub(r"(\d),(\d)", r"\1.\2", s)

    # 'A)...B)...' formatini '...|...' formatiga o'girish (admin matn orqali kiritganda)
    ab_match = re.match(
        r"^A[\)\.\:\-]?\s*(.+?)\s*[,;]?\s*B[\)\.\:\-]?\s*(.+)$",
        s,
        flags=re.IGNORECASE,
    )
    if ab_match:
        part_a = ab_match.group(1).replace(" ", "").strip()
        part_b = ab_match.group(2).replace(" ", "").strip()
        return f"{part_a}|{part_b}"

    s = s.replace(" ", "")
    return s if s else "X"


def parse_raw_keys(keys_raw: str) -> list:
    """
    Admin matn yoki TXT fayl yuborganda kalitlarni aqlli ajratib oladi.
    Masalan:
    - 'A B C D ... a)5b)10'
    - '1.A 2.B 3.C ... 40.a)5b)10'
    - Har qatorda bittadan yozilgan kalitlar
    """
    # Agar 'a) 5 b) 10' bo'shliq bilan yozilgan bo'lsa, ularni birlashtiramiz
    cleaned = re.sub(
        r"([aA][\)\.\:])\s*([^\s]+)\s+([bB][\)\.\:])\s*([^\s]+)",
        r"\1\2\3\4",
        keys_raw,
    )
    tokens = [tok.strip() for tok in cleaned.replace("\r", "\n").split() if tok.strip()]
    is_numbered_list = len(tokens) >= 2 and bool(
        re.match(r"^1[\.\)\-\:]", tokens[0]) and re.match(r"^2[\.\)\-\:]", tokens[1])
    )
    parsed = []
    for idx, t in enumerate(tokens, 1):
        # 1) Agar ro'yxat ketma-ket raqamlangan bo'lsa (1.A 2.B ... 36.12,5), mos savol raqamini olib tashlaymiz
        if is_numbered_list:
            m_seq = re.match(rf"^{idx}[\.\)\-\:](.+)$", t)
            if m_seq:
                t = m_seq.group(1)
        # 2) Harfli javob oldidagi raqam ('1.A', '12)B') yoki ')' / ':' bilan ajratilgan raqam ('36)12.5')
        m_gen = re.match(r"^\d+(?:[\)\:](.+)|[\.\-]([A-Za-z].*))$", t)
        if m_gen:
            t = m_gen.group(1) or m_gen.group(2)
        parsed.append(normalize_answer(t))
    return parsed


def check_single_answer(user_val, key_val) -> bool:
    u = normalize_answer(user_val)
    k = normalize_answer(key_val)
    if u == "X" or k == "X":
        return False

    if "|" in k:
        if "|" not in u:
            return False
        u_parts = [p.strip() for p in u.split("|")]
        k_parts = [p.strip() for p in k.split("|")]
        if len(u_parts) != len(k_parts):
            return False
        # a) va b) o'z joyida mos kelishi kerak
        return u_parts == k_parts

    # Sonli javoblarni aniq solishtirish (masalan 5.0 == 5)
    try:
        if float(u) == float(k):
            return True
    except ValueError:
        pass

    return u == k


def evaluate_vector(user_ans_list: list, correct_keys_list: list):
    total = len(correct_keys_list)
    if total == 0:
        return 0, [], 0.0, "NC", 0.0, 0.0

    score_vector = []
    correct_count = 0
    limit = min(len(user_ans_list), total)

    for i in range(limit):
        if check_single_answer(user_ans_list[i], correct_keys_list[i]):
            score_vector.append(1)
            correct_count += 1
        else:
            score_vector.append(0)

    while len(score_vector) < total:
        score_vector.append(0)

    percent = (correct_count / total) * 100.0
    return correct_count, score_vector, percent, "NC", 0.0, 0.0


def get_rash_weights(total_questions: int, stats: list = None) -> list:
    """
    Rash modeli bo'yicha har bir savolning qiyinlik vaznini hisoblaydi:
    - Ko'pchilik topgan oson savol -> 1.1 ballga yaqin
    - Kamchilik topgan qiyin savol -> 1.9 ballga yaqin
    - Hali hech kim ishlamagan savol -> 1.5 ball (o'rtacha)
    """
    weights = []
    for i in range(total_questions):
        t_att = 0
        c_att = 0
        if stats and i < len(stats):
            t_att = stats[i].get("total", 0)
            c_att = stats[i].get("correct", 0)
        if t_att <= 0:
            w = 1.5
        else:
            p = max(0.0, min(1.0, c_att / t_att))
            w = 1.1 + (1.9 - 1.1) * (1.0 - p)
        weights.append(w)
    return weights


def grade_from_rash(rash_75: float) -> str:
    if rash_75 >= 70.0:
        return "A+"
    elif rash_75 >= 65.0:
        return "A"
    elif rash_75 >= 60.0:
        return "B+"
    elif rash_75 >= 55.0:
        return "B"
    elif rash_75 >= 50.0:
        return "C+"
    elif rash_75 >= 46.0:
        return "C"
    return "NC"


def calculate_score(
    user_ans_list: list,
    correct_keys_list: list,
    test_type: str,
    stats: list = None,
):
    total = len(correct_keys_list)
    if total == 0:
        return 0, [], 0.0, "NC", 0.0, 0.0

    correct_count, vector, percent, _, _, _ = evaluate_vector(
        user_ans_list, correct_keys_list
    )

    if test_type == "dtm":
        score_75 = round(percent, 2)
        final_score = round(correct_count * 3.1, 2) if total == 30 else round(percent, 2)
        grade = f"{correct_count}/{total}"
        return correct_count, vector, round(percent, 2), grade, score_75, final_score

    # Rash modeli (Milliy Sertifikat / Fan 1 / Fan 2)
    weights = get_rash_weights(total, stats)
    max_weighted = sum(weights)
    weighted_score = sum(weights[i] for i in range(total) if vector[i] == 1)

    rash_75 = (weighted_score / max_weighted) * 75.0 if max_weighted > 0 else 0.0
    grade = grade_from_rash(rash_75)

    final_score = rash_75
    if test_type == "fan1":
        final_score = 93.0 if rash_75 >= 65.0 else rash_75 * (93.0 / 65.0)
    elif test_type == "fan2":
        final_score = 63.0 if rash_75 >= 65.0 else rash_75 * (63.0 / 65.0)

    return (
        correct_count,
        vector,
        round(percent, 2),
        grade,
        round(rash_75, 2),
        round(final_score, 2),
    )


async def check_subscription(user_id: int) -> bool:
    if user_id in ADMINS:
        return True
    channel = get_channel()
    if not channel:
        return True

    now = time.monotonic()
    if user_id in _sub_cache:
        exp_ts, is_sub = _sub_cache[user_id]
        if now < exp_ts:
            return is_sub

    try:
        member = await bot.get_chat_member(chat_id=channel, user_id=user_id)
        is_sub = member.status in ["creator", "administrator", "member"]
        _sub_cache[user_id] = (now + SUB_CACHE_TTL, is_sub)
        return is_sub
    except Exception:
        return False


# ================= 3. PDF VA EXCEL HISOBOT (IN-MEMORY & THREAD-SAFE) =================
def create_charts(stats: list):
    q_nums = list(range(1, len(stats) + 1))
    corrects = [s.get("correct", 0) for s in stats]

    fig = plt.figure(figsize=(11, 4.2))
    ax = fig.add_subplot(111)
    ax.bar(q_nums, corrects, color="#28a745", edgecolor="#1e7e34")
    ax.set_title("Savollar tahlili (To'g'ri javoblar soni)", fontweight="bold")
    ax.set_xlabel("Savol raqami")
    ax.set_ylabel("To'g'ri topganlar soni")
    ax.set_xticks(q_nums)
    ax.tick_params(axis="x", labelsize=8, rotation=90 if len(q_nums) > 25 else 0)

    max_v = max(corrects) if corrects else 1
    for i, v in enumerate(corrects):
        ax.text(
            i + 1,
            v + max_v * 0.015,
            str(v),
            ha="center",
            fontsize=7,
        )

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=130)
    buf.seek(0)
    plt.close(fig)
    return [buf]


def generate_reports(test_code: str):
    """
    Sinxron hisobot generatori (run_in_executor orqali chaqiriladi):
    1. Barcha qatnashchilarning ballarini yakuniy Rash qiyinlik darajasi (stats) bo'yicha adolatli qayta hisoblaydi
    2. PDF va Excel fayllarni diskni ifloslantirmasdan RAM (io.BytesIO) da yaratadi
    """
    test = get_test(test_code, use_cache=False)
    if not test:
        raise ValueError("Test topilmadi")

    results = get_results_list(test_code)
    keys = test["keys"]
    stats = test["stats"]
    t_type = test["type"]

    # Agar Rash testi bo'lsa va user_answers saqlangan bo'lsa, yakuniy stats asosida ballarni sinxron yangilaymiz
    if t_type != "dtm" and results:
        updated_rows = []
        for r in results:
            if r.get("user_answers"):
                try:
                    u_ans = json.loads(r["user_answers"])
                    corr, _, perc, grade, rash_75, final_sc = calculate_score(
                        u_ans, keys, t_type, stats
                    )
                    r["correct_count"] = corr
                    r["percent"] = perc
                    r["grade"] = grade
                    r["score_75"] = rash_75
                    r["final_score"] = final_sc
                    updated_rows.append((corr, perc, grade, rash_75, final_sc, r["id"]))
                except Exception:
                    pass
        if updated_rows:
            with get_db() as conn:
                conn.executemany(
                    "UPDATE results SET correct_count=?, percent=?, grade=?, score_75=?, final_score=? WHERE id=?",
                    updated_rows,
                )

    sorted_results = sorted(
        results,
        key=lambda x: (
            float(x.get("final_score") or x.get("score_75") or 0),
            int(x.get("correct_count") or 0),
        ),
        reverse=True,
    )

    # 1. PDF Yaratish
    pdf_buf = io.BytesIO()
    doc = SimpleDocTemplate(pdf_buf, pagesize=landscape(A4))
    styles = getSampleStyleSheet()
    elements = [
        Paragraph(
            f"LOCHIN MATH — HISOBOT: {test_code} ({t_type.upper()}) | Jami: {len(sorted_results)} ta",
            styles["Title"],
        ),
        Spacer(1, 15),
    ]

    if stats:
        charts = create_charts(stats)
        elements.append(Paragraph("Savollar kesimida statistika", styles["Heading2"]))
        elements.append(ReportLabImage(charts[0], width=720, height=250))
        elements.append(PageBreak())

    headers = ["№", "F.I.O", "To'g'ri", "Rash (75)", "Yakuniy Ball", "Daraja", "%"]
    table_data = [headers]

    pdf_slice = sorted_results[:MAX_PDF_ROWS]
    for idx, res in enumerate(pdf_slice, 1):
        sc75 = float(res.get("score_75") or 0)
        f_sc = float(res.get("final_score") or sc75)
        pct = float(res.get("percent") or 0)
        table_data.append(
            [
                str(idx),
                str(res.get("full_name", ""))[:45],
                f"{res.get('correct_count', 0)}/{len(keys)}",
                f"{sc75:.2f}",
                f"{f_sc:.2f}",
                str(res.get("grade", "")),
                f"{pct:.1f}%",
            ]
        )

    t = Table(table_data, colWidths=[40, 250, 75, 80, 90, 75, 65])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#003366")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.8, colors.grey),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("ALIGN", (1, 1), (1, -1), "LEFT"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
            ]
        )
    )
    elements.append(t)
    if len(sorted_results) > MAX_PDF_ROWS:
        elements.append(Spacer(1, 12))
        elements.append(
            Paragraph(
                f"Eslatma: PDF hisobotda eng yuqori {MAX_PDF_ROWS} ta natija ko'rsatildi. Barcha {len(sorted_results)} ta qatnashchi ro'yxati Excel faylda mavjud.",
                styles["Normal"],
            )
        )
    doc.build(elements)
    pdf_buf.seek(0)

    # 2. Excel Yaratish (100% barcha qatnashchilar, In-Memory)
    excel_buf = io.BytesIO()
    excel_rows = []
    for idx, res in enumerate(sorted_results, 1):
        sc75 = float(res.get("score_75") or 0)
        f_sc = float(res.get("final_score") or sc75)
        excel_rows.append(
            {
                "O'rin": idx,
                "F.I.O": res.get("full_name", ""),
                "Telegram ID": res.get("user_id", ""),
                "To'g'ri javoblar": res.get("correct_count", 0),
                "Jami savollar": len(keys),
                "Foiz (%)": round(float(res.get("percent") or 0), 2),
                "Rash Ball (75)": round(sc75, 2),
                "Yakuniy Ball": round(f_sc, 2),
                "Daraja / Baho": res.get("grade", ""),
                "Topshirilgan vaqt": res.get("date", ""),
            }
        )
    df = pd.DataFrame(excel_rows)
    with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
        if not df.empty:
            df.to_excel(writer, index=False, sheet_name="Natijalar")
        else:
            pd.DataFrame(
                columns=[
                    "O'rin",
                    "F.I.O",
                    "Telegram ID",
                    "To'g'ri javoblar",
                    "Rash Ball (75)",
                    "Yakuniy Ball",
                    "Daraja / Baho",
                ]
            ).to_excel(writer, index=False, sheet_name="Natijalar")
    excel_buf.seek(0)

    return pdf_buf, excel_buf


# ================= 4. STATES & KEYBOARDS =================
def kb_admin():
    return (
        ReplyKeyboardMarkup(resize_keyboard=True)
        .row("➕ Yangi Test", "📊 Hisobotlar")
        .row("📢 Reklama", "🔗 Majburiy Obuna")
        .row("📈 Statistika")
    )


def kb_user():
    return ReplyKeyboardMarkup(resize_keyboard=True).row(
        "📝 Test ishlash", "👤 Mening natijalarim"
    )


def kb_cancel():
    return ReplyKeyboardMarkup(resize_keyboard=True).add("🔙 Bekor qilish")


def kb_test_type():
    return (
        ReplyKeyboardMarkup(resize_keyboard=True)
        .row("RASH (Milliy - 75 ball)", "DTM (Standart)")
        .row("Fan 1 (Max 93 ball)", "Fan 2 (Max 63 ball)")
        .add("🔙 Bekor qilish")
    )


class AdminStates(StatesGroup):
    waiting_code = State()
    waiting_type = State()
    waiting_pdf = State()
    waiting_keys = State()
    waiting_deadline = State()
    waiting_ad = State()
    waiting_channel = State()


class UserStates(StatesGroup):
    waiting_code = State()
    waiting_name = State()
    solving = State()


# ================= 5. HANDLERS =================
@dp.message_handler(commands=["start"], state="*")
async def cmd_start(message: types.Message, state: FSMContext):
    await state.finish()
    add_user(
        message.from_user.id,
        message.from_user.full_name,
        message.from_user.username,
    )

    if message.from_user.id in ADMINS:
        await message.answer(
            f"👨‍💻 **Lochin Math — Admin panel** (ID: `{message.from_user.id}`):",
            parse_mode="Markdown",
            reply_markup=kb_admin(),
        )
    else:
        if not await check_subscription(message.from_user.id):
            chan = get_channel()
            if chan:
                kb = (
                    InlineKeyboardMarkup()
                    .add(
                        InlineKeyboardButton(
                            "➕ A'zo bo'lish",
                            url=f"https://t.me/{chan.replace('@', '')}",
                        )
                    )
                    .add(
                        InlineKeyboardButton(
                            "✅ Tekshirish", callback_data="check_sub"
                        )
                    )
                )
                await message.answer(
                    f"Botdan foydalanish uchun {chan} kanaliga a'zo bo'ling:",
                    reply_markup=kb,
                )
                return
        await message.answer(
            "🦅 **Lochin Math** test botiga xush kelibsiz!\nTest ishlash uchun quyidagi tugmani bosing:",
            parse_mode="Markdown",
            reply_markup=kb_user(),
        )


@dp.callback_query_handler(text="check_sub", state="*")
async def check_sub_call(call: types.CallbackQuery):
    _sub_cache.pop(call.from_user.id, None)  # Yangi tekshiruv uchun keshni tozalash
    if await check_subscription(call.from_user.id):
        with contextlib.suppress(Exception):
            await call.message.delete()
        await call.message.answer(
            "✅ Obuna tasdiqlandi! Botdan foydalanishingiz mumkin.",
            reply_markup=kb_user(),
        )
    else:
        await call.answer("❌ Hali kanalga a'zo bo'lmadingiz!", show_alert=True)


@dp.message_handler(Text(equals="🔙 Bekor qilish"), state="*")
async def cancel(message: types.Message, state: FSMContext):
    await state.finish()
    mk = kb_admin() if message.from_user.id in ADMINS else kb_user()
    await message.answer("Amal bekor qilindi.", reply_markup=mk)


# --- ADMIN: STATISTIKA ---
@dp.message_handler(Text(equals="📈 Statistika"), user_id=ADMINS, state="*")
async def admin_stats_handler(message: types.Message):
    u_cnt, t_cnt, r_cnt = get_system_stats()
    chan = get_channel() or "Yo'q"
    txt = (
        f"📈 **Lochin Math — Tizim Statistikasi:**\n\n"
        f"👥 Jami foydalanuvchilar: **{u_cnt}** ta\n"
        f"📝 Jami yaratilgan testlar: **{t_cnt}** ta\n"
        f"✅ Jami yechilgan testlar: **{r_cnt}** ta\n"
        f"🔗 Majburiy obuna kanali: **{chan}**"
    )
    await message.answer(txt, parse_mode="Markdown", reply_markup=kb_admin())


# --- ADMIN: MAJBURIY OBUNA ---
@dp.message_handler(Text(equals="🔗 Majburiy Obuna"), user_id=ADMINS, state="*")
async def ch_menu(message: types.Message):
    curr = get_channel() or "O'rnatilmagan"
    await message.answer(
        f"Hozirgi kanal: **{curr}**\n\nYangi kanal username'ini yuboring (`@kanal`) yoki o'chirish uchun `del` deb yozing:",
        parse_mode="Markdown",
        reply_markup=kb_cancel(),
    )
    await AdminStates.waiting_channel.set()


@dp.message_handler(state=AdminStates.waiting_channel, user_id=ADMINS)
async def ch_save(message: types.Message, state: FSMContext):
    if message.text.strip().lower() == "del":
        set_channel(None)
        await message.answer("✅ Majburiy obuna o'chirildi.", reply_markup=kb_admin())
    else:
        set_channel(message.text.strip())
        await message.answer(
            f"✅ Majburiy obuna kanali saqlandi: {get_channel()}",
            reply_markup=kb_admin(),
        )
    await state.finish()


# --- ADMIN: TEST YARATISH ---
@dp.message_handler(Text(equals="➕ Yangi Test"), user_id=ADMINS, state="*")
async def new_test(message: types.Message):
    await message.answer(
        "Yangi test kodini kiriting (masalan: `101`):",
        parse_mode="Markdown",
        reply_markup=kb_cancel(),
    )
    await AdminStates.waiting_code.set()


@dp.message_handler(state=AdminStates.waiting_code, user_id=ADMINS)
async def code_step(message: types.Message, state: FSMContext):
    code = message.text.strip()
    if get_test(code, use_cache=False):
        await message.answer("❌ Bu test kodi band! Boshqa kod kiriting:")
        return
    await state.update_data(code=code)
    await message.answer("Test turini tanlang:", reply_markup=kb_test_type())
    await AdminStates.waiting_type.set()


@dp.message_handler(state=AdminStates.waiting_type, user_id=ADMINS)
async def type_step(message: types.Message, state: FSMContext):
    txt = message.text.upper()
    if "FAN 1" in txt:
        t_type = "fan1"
    elif "FAN 2" in txt:
        t_type = "fan2"
    elif "RASH" in txt or "MILLIY" in txt:
        t_type = "rash"
    else:
        t_type = "dtm"

    await state.update_data(test_type=t_type)
    await message.answer(
        "📄 Test savollari PDF faylini yuboring (yoki PDF siz yaratish uchun `yoq` deb yozing):",
        parse_mode="Markdown",
        reply_markup=kb_cancel(),
    )
    await AdminStates.waiting_pdf.set()


@dp.message_handler(
    content_types=["document", "text"], state=AdminStates.waiting_pdf, user_id=ADMINS
)
async def pdf_step(message: types.Message, state: FSMContext):
    pdf_id = message.document.file_id if message.document else None
    await state.update_data(pdf=pdf_id)
    data = await state.get_data()

    web_type = "dtm" if data["test_type"] == "dtm" else "rash"
    admin_webapp_url = f"{WEB_APP_URL}/?mode=admin&type={web_type}&code={data['code']}"

    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(
        KeyboardButton(
            text="🔑 WebApp orqali kalit kiritish",
            web_app=WebAppInfo(url=admin_webapp_url),
        )
    )
    kb.add(KeyboardButton("🔙 Bekor qilish"))

    await message.answer(
        "🔑 **Kalitlarni yuboring:**\n\n"
        "1️⃣ Pastdagi **🔑 WebApp orqali kalit kiritish** tugmasini bosib belgilashingiz mumkin;\n"
        "2️⃣ Yoki oddiy matn / TXT fayl ko'rinishida yuborishingiz mumkin:\n"
        "• DTM uchun: `A B C D ...`\n"
        "• Rash uchun: `A B ... a)5b)10` yoki `5|10`",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    await AdminStates.waiting_keys.set()


@dp.message_handler(
    content_types=["document", "text", "web_app_data"],
    state=AdminStates.waiting_keys,
    user_id=ADMINS,
)
async def keys_step(message: types.Message, state: FSMContext):
    keys_list = []

    if message.web_app_data:
        try:
            raw_list = json.loads(message.web_app_data.data)
            keys_list = [normalize_answer(k) for k in raw_list]
        except Exception:
            await message.answer("❌ WebApp ma'lumotini o'qishda xatolik yuz berdi.")
            return
    elif message.document:
        f_info = await bot.get_file(message.document.file_id)
        content = await bot.download_file(f_info.file_path)
        keys_raw = content.read().decode("utf-8", errors="ignore")
        keys_list = parse_raw_keys(keys_raw)
    elif message.text:
        keys_list = parse_raw_keys(message.text)

    if not keys_list:
        await message.answer("❌ Kalitlar topilmadi! Qaytadan yuboring:")
        return

    await state.update_data(keys=keys_list)
    await message.answer(
        f"✅ **{len(keys_list)} ta kalit qabul qilindi!**\n\n"
        f"⏳ Test tugash vaqtini kiriting (Format: `KK.OO.YYYY SS:MM`, masalan `25.11.2026 21:00`) yoki cheklovsiz qilish uchun `yoq` deb yozing:",
        parse_mode="Markdown",
        reply_markup=kb_cancel(),
    )
    await AdminStates.waiting_deadline.set()


@dp.message_handler(state=AdminStates.waiting_deadline, user_id=ADMINS)
async def deadline_step(message: types.Message, state: FSMContext):
    dl = message.text.strip()
    if dl.lower() != "yoq":
        try:
            datetime.strptime(dl, "%d.%m.%Y %H:%M")
        except ValueError:
            await message.answer(
                "❌ Vaqt formati xato! `KK.OO.YYYY SS:MM` ko'rinishida yozing (masalan: `25.11.2026 21:00`) yoki `yoq` deb yozing:",
                parse_mode="Markdown",
            )
            return
    else:
        dl = None

    data = await state.get_data()
    save_test(
        data["code"],
        data["pdf"],
        data["keys"],
        dl,
        data["test_type"],
    )
    await message.answer(
        f"✅ **Test muvaffaqiyatli saqlandi!**\n\n"
        f"🔢 Kod: `{data['code']}`\n"
        f"📚 Tur: `{data['test_type'].upper()}`\n"
        f"🔑 Savollar soni: `{len(data['keys'])}` ta\n"
        f"⏰ Muddat: `{dl or 'Cheklanmagan'}`",
        parse_mode="Markdown",
        reply_markup=kb_admin(),
    )
    await state.finish()


# --- ADMIN: HISOBOTLAR (SAHIFALASH VA O'CHIRISH BILAN) ---
REPORTS_PER_PAGE = 12


def build_reports_keyboard(codes: list, page: int = 0) -> InlineKeyboardMarkup:
    mk = InlineKeyboardMarkup(row_width=2)
    start = page * REPORTS_PER_PAGE
    end = start + REPORTS_PER_PAGE
    page_codes = codes[start:end]

    for c in page_codes:
        mk.insert(InlineKeyboardButton(f"📂 {c}", callback_data=f"rep:{c}"))

    nav_btns = []
    if page > 0:
        nav_btns.append(
            InlineKeyboardButton("⬅️ Oldingi", callback_data=f"reppage:{page - 1}")
        )
    if end < len(codes):
        nav_btns.append(
            InlineKeyboardButton("Keyingi ➡️", callback_data=f"reppage:{page + 1}")
        )
    if nav_btns:
        mk.row(*nav_btns)
    return mk


@dp.message_handler(Text(equals="📊 Hisobotlar"), user_id=ADMINS, state="*")
async def report_menu(message: types.Message):
    codes = get_all_test_codes()
    if not codes:
        await message.answer("Hozircha testlar mavjud emas.")
        return
    await message.answer(
        f"📊 **Hisobotlar bo'limi** (Jami testlar: {len(codes)} ta)\nKerakli testni tanlang:",
        parse_mode="Markdown",
        reply_markup=build_reports_keyboard(codes, 0),
    )


@dp.callback_query_handler(
    lambda c: c.data.startswith("reppage:"), user_id=ADMINS, state="*"
)
async def report_page_call(call: types.CallbackQuery):
    page = int(call.data.split(":")[1])
    codes = get_all_test_codes()
    await call.answer()
    with contextlib.suppress(Exception):
        await call.message.edit_reply_markup(
            reply_markup=build_reports_keyboard(codes, page)
        )


@dp.callback_query_handler(
    lambda c: c.data.startswith("rep:"), user_id=ADMINS, state="*"
)
async def send_rep(call: types.CallbackQuery):
    code = call.data.split(":", 1)[1]
    await call.answer("⏳ Hisobot tayyorlanmoqda...")
    try:
        loop = asyncio.get_running_loop()
        pdf_buf, xls_buf = await loop.run_in_executor(None, generate_reports, code)
        media = MediaGroup()
        media.attach_document(
            InputFile(pdf_buf, filename=f"Hisobot_{code}.pdf"),
            caption=f"📊 **{code}** testi bo'yicha to'liq PDF va Excel hisobot",
        )
        media.attach_document(
            InputFile(xls_buf, filename=f"Hisobot_{code}.xlsx")
        )
        await bot.send_media_group(call.from_user.id, media=media)

        del_kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton(
                f"🗑 {code} testini o'chirish", callback_data=f"deltest:{code}"
            )
        )
        await call.message.answer(
            f"Test `{code}` boshqaruvi:",
            parse_mode="Markdown",
            reply_markup=del_kb,
        )
    except Exception as e:
        logging.exception("Report generation failed")
        await call.message.answer(f"❌ Hisobot tayyorlashda xatolik: {e}")


@dp.callback_query_handler(
    lambda c: c.data.startswith("deltest:"), user_id=ADMINS, state="*"
)
async def delete_test_call(call: types.CallbackQuery):
    code = call.data.split(":", 1)[1]
    delete_test(code)
    await call.answer("Test o'chirildi!", show_alert=True)
    with contextlib.suppress(Exception):
        await call.message.edit_text(f"🗑 `{code}` testi va uning natijalari o'chirildi.", parse_mode="Markdown")


# --- FOYDALANUVCHI: TEST ISHLASH ---
@dp.message_handler(Text(equals="📝 Test ishlash"), state="*")
async def start_solve(message: types.Message, state: FSMContext):
    await state.finish()
    add_user(
        message.from_user.id,
        message.from_user.full_name,
        message.from_user.username,
    )
    if not await check_subscription(message.from_user.id):
        chan = get_channel()
        if chan:
            url = f"https://t.me/{chan.replace('@', '')}"
            kb = (
                InlineKeyboardMarkup()
                .add(InlineKeyboardButton("➕ A'zo bo'lish", url=url))
                .add(InlineKeyboardButton("✅ Tekshirish", callback_data="check_sub"))
            )
            await message.answer(
                "❗️ Test ishlash uchun avval kanalimizga a'zo bo'ling!",
                reply_markup=kb,
            )
            return
    await message.answer("🔢 Test kodini kiriting:", reply_markup=kb_cancel())
    await UserStates.waiting_code.set()


@dp.message_handler(state=UserStates.waiting_code)
async def check_test_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    test = get_test(code)
    if not test:
        await message.answer("❌ Bunday kodli test topilmadi. Qaytadan kiriting:")
        return

    if test["deadline"]:
        try:
            if datetime.now() > datetime.strptime(test["deadline"], "%d.%m.%Y %H:%M"):
                await message.answer(
                    "⏳ Ushbu testning topshirish vaqti tugagan!",
                    reply_markup=kb_user(),
                )
                await state.finish()
                return
        except ValueError:
            pass

    await state.update_data(test_code=code)
    await message.answer("👤 Ism va Familiyangizni to'liq kiriting:")
    await UserStates.waiting_name.set()


@dp.message_handler(state=UserStates.waiting_name)
async def enter_name(message: types.Message, state: FSMContext):
    full_name = message.text.strip()
    if len(full_name) < 3:
        await message.answer("Iltimos, Ism va Familiyangizni to'liq yozing:")
        return

    await state.update_data(name=full_name)
    data = await state.get_data()
    test = get_test(data["test_code"])
    if not test:
        await message.answer("❌ Test topilmadi.", reply_markup=kb_user())
        await state.finish()
        return

    if test["pdf"]:
        await message.answer_document(
            test["pdf"],
            caption=f"📄 **Test kodi:** `{test['code']}`\nSavollar fayli (Himoyalangan).",
            parse_mode="Markdown",
            protect_content=True,
        )

    q_count = len(test["keys"])
    web_type = "dtm" if test["type"] == "dtm" and q_count not in (44, 45) else "rash"
    web_app_url = f"{WEB_APP_URL}/?mode=user&count={q_count}&type={web_type}&code={test['code']}"

    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(
        KeyboardButton(
            text="🚀 JAVOBLARNI BELGILASH",
            web_app=WebAppInfo(url=web_app_url),
        )
    )
    kb.add(KeyboardButton("🔙 Bekor qilish"))

    await message.answer(
        f"✅ Tayyor! Savollar soni: **{q_count} ta**.\nJavoblarni belgilash uchun quyidagi tugmani bosing:",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    await UserStates.solving.set()


@dp.message_handler(content_types=["web_app_data"], state=UserStates.solving)
async def web_app_data_handler(message: types.Message, state: FSMContext):
    try:
        user_answers = json.loads(message.web_app_data.data)
    except Exception:
        await message.answer("❌ Javoblarni o'qishda xatolik yuz berdi.")
        return

    data = await state.get_data()
    test_code = data.get("test_code")
    student_name = data.get("name", message.from_user.full_name)

    res = record_test_attempt_atomic(
        test_code=test_code,
        user_id=message.from_user.id,
        full_name=student_name,
        user_answers=user_answers,
    )
    if not res:
        await message.answer("❌ Test topilmadi.", reply_markup=kb_user())
        await state.finish()
        return

    corr = res["correct"]
    total = res["total"]
    perc = res["percent"]
    grade = res["grade"]
    score_75 = res["score_75"]
    final_score = res["final_score"]
    t_type = res["type"]
    vec = res["vector"]

    # Savollar bo'yicha qisqacha xatolar ko'rsatkichi
    wrong_nums = [str(i + 1) for i, v in enumerate(vec) if v == 0]
    if not wrong_nums:
        detail_line = "🔥 **Barcha savollarga 100% to'g'ri javob berdingiz!**"
    elif len(wrong_nums) <= 20:
        detail_line = f"❌ **Xato qilingan savollar:** {', '.join(wrong_nums)}"
    else:
        detail_line = f"❌ **Xato qilingan savollar ({len(wrong_nums)} ta):** {', '.join(wrong_nums[:20])}..."

    header = (
        "✅ **Natijangiz qabul qilindi!**"
        if res["is_first_attempt"]
        else "ℹ️ **Qayta urinish natijasi** _(Reytingda 1-urinishingiz saqlanadi)_"
    )

    res_text = (
        f"{header}\n\n"
        f"👤 **O'quvchi:** {student_name}\n"
        f"🔢 **Test kodi:** `{test_code}`\n"
        f"🎯 **To'g'ri javoblar:** {corr}/{total} ({perc:.1f}%)\n"
    )
    if t_type == "dtm":
        res_text += f"📊 **Natija:** {perc:.1f}%\n"
    elif t_type in ("fan1", "fan2"):
        res_text += (
            f"📊 **Rash Ball (75):** {score_75:.2f}\n"
            f"🏆 **Yakuniy Ball:** {final_score:.2f}\n"
            f"⭐️ **Daraja:** {grade}\n"
        )
    else:
        res_text += f"📊 **Ball:** {score_75:.2f} (max 75)\n🏆 **Daraja:** {grade}\n"

    res_text += f"\n{detail_line}"

    await message.answer(
        res_text,
        parse_mode="Markdown",
        reply_markup=kb_user(),
        protect_content=True,
    )
    await state.finish()


# --- ADMIN: 100K FOYDALANUVCHIGA REKLAMA (BACKGROUND BROADCAST) ---
_broadcast_running = False


async def _run_broadcast(admin_id: int, src_message: types.Message):
    global _broadcast_running
    _broadcast_running = True
    sent = 0
    blocked = 0
    failed = 0
    start_ts = time.monotonic()

    status_msg = await bot.send_message(
        admin_id, "⏳ **Reklama tarqatish boshlandi...**", parse_mode="Markdown"
    )

    try:
        for idx, u_id in enumerate(iter_all_users(batch_size=2000), 1):
            if u_id in ADMINS:
                continue
            try:
                await src_message.copy_to(u_id, protect_content=True)
                sent += 1
                await asyncio.sleep(0.035)  # ~28 msg/sec (Telegram global limit 30 msg/sec)
            except RetryAfter as e:
                await asyncio.sleep(e.timeout + 1)
                try:
                    await src_message.copy_to(u_id, protect_content=True)
                    sent += 1
                except Exception:
                    failed += 1
            except (BotBlocked, ChatNotFound, UserDeactivated):
                blocked += 1
            except TelegramAPIError:
                failed += 1
            except Exception:
                failed += 1

            # Har 500 ta xabarda progressni yangilash
            if idx % 500 == 0:
                with contextlib.suppress(Exception):
                    await status_msg.edit_text(
                        f"⏳ **Reklama yuborilmoqda...**\n\n"
                        f"✅ Yuborildi: **{sent}**\n"
                        f"🚫 Bloklaganlar: **{blocked}**\n"
                        f"⚠️ Xatolik: **{failed}**",
                        parse_mode="Markdown",
                    )
    finally:
        _broadcast_running = False
        elapsed = int(time.monotonic() - start_ts)
        with contextlib.suppress(Exception):
            await status_msg.edit_text(
                f"✅ **Reklama tarqatish yakunlandi!** ({elapsed} s)\n\n"
                f"📬 Yetkazildi: **{sent}** ta\n"
                f"🚫 Botni bloklagan: **{blocked}** ta\n"
                f"⚠️ Boshqa xatoliklar: **{failed}** ta",
                parse_mode="Markdown",
            )


@dp.message_handler(Text(equals="📢 Reklama"), user_id=ADMINS, state="*")
async def ad_menu(message: types.Message):
    if _broadcast_running:
        await message.answer(
            "⏳ Hozirda reklama tarqatish jarayoni ketmoqda. Tugashini kuting."
        )
        return
    await message.answer(
        "📢 Barcha foydalanuvchilarga yuboriladigan reklama postini yuboring:",
        reply_markup=kb_cancel(),
    )
    await AdminStates.waiting_ad.set()


@dp.message_handler(
    content_types=types.ContentTypes.ANY, state=AdminStates.waiting_ad, user_id=ADMINS
)
async def ad_send(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer(
        "🚀 Reklama orqa fonda yuborishga qo'yildi. Bot odatdagidek ishlashda davom etadi!",
        reply_markup=kb_admin(),
    )
    asyncio.create_task(_run_broadcast(message.from_user.id, message))


# --- FOYDALANUVCHI: MENING NATIJALARIM ---
@dp.message_handler(Text(equals="👤 Mening natijalarim"), state="*")
async def history(message: types.Message):
    rows = get_user_history(message.from_user.id, limit=25)
    if not rows:
        await message.answer("📂 Siz hali hech qanday test ishlamagansiz.")
    else:
        txt = "📂 **Sizning oxirgi natijalaringiz:**\n\n"
        for r in rows:
            sc = r.get("final_score") or r.get("score_75") or 0
            txt += f"🔹 Kod: `{r['test_code']}` | **{sc:.1f} ball** | Daraja: **{r['grade']}**\n"
        await message.answer(txt, parse_mode="Markdown", protect_content=True)


# ================= 6. SPAM BLOCKER =================
@dp.message_handler(content_types=types.ContentTypes.ANY, state=None)
async def block_spam(message: types.Message):
    if message.from_user.id in ADMINS:
        return
    if message.text in [
        "📝 Test ishlash",
        "👤 Mening natijalarim",
        "🔙 Bekor qilish",
    ]:
        return
    with contextlib.suppress(Exception):
        await message.delete()


# ================= 7. WEBHOOK VA POLLING SERVER =================
WEBHOOK_PATH = f"/webhook/{API_TOKEN}"


def get_webhook_host():
    host = (
        os.environ.get("WEBHOOK_HOST")
        or os.environ.get("RAILWAY_PUBLIC_DOMAIN")
        or os.environ.get("RAILWAY_STATIC_URL")
        or os.environ.get("RENDER_EXTERNAL_URL")
    )
    if not host:
        return None
    host = host.strip().rstrip("/")
    if not host.startswith("http://") and not host.startswith("https://"):
        host = f"https://{host}"
    return host


async def handle_health_check(request):
    return web.Response(text="Lochin Math Bot is running healthy!")


async def handle_webapp(request):
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    if os.path.exists(html_path):
        return web.FileResponse(html_path)
    return web.Response(text="WebApp index.html not found", status=404)


async def on_startup_webhook(dispatcher):
    db_start()
    webhook_host = get_webhook_host()
    if not webhook_host:
        logging.warning("WEBHOOK_HOST topilmadi! Serverda RAILWAY_PUBLIC_DOMAIN yoki WEBHOOK_HOST o'rnating.")
        return
    webhook_url = f"{webhook_host}{WEBHOOK_PATH}"
    logging.info(f"Setting webhook to: {webhook_url}")
    try:
        current_info = await bot.get_webhook_info()
        if current_info.url != webhook_url:
            await bot.set_webhook(webhook_url, drop_pending_updates=False)
            logging.info(f"Webhook successfully set to: {webhook_url}")
        else:
            logging.info(f"Webhook is already set to: {webhook_url}")
    except Exception as e:
        logging.error(f"Error setting webhook: {e}")


async def on_startup_polling(dispatcher):
    db_start()
    with contextlib.suppress(Exception):
        await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Lochin Math Bot started in POLLING mode.")


async def on_shutdown(dispatcher):
    logging.warning("Shutting down Lochin Math Bot...")
    await dp.storage.close()
    await dp.storage.wait_closed()


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    use_polling = os.environ.get("USE_POLLING", "false").lower() in ("true", "1", "yes")

    if use_polling:
        logging.info("Starting bot in POLLING mode...")
        executor.start_polling(
            dp,
            on_startup=on_startup_polling,
            on_shutdown=on_shutdown,
            skip_updates=True,
            loop=loop,
        )
    else:
        logging.info("Starting bot in WEBHOOK mode (High-Load)...")
        app = web.Application()
        app.router.add_get("/", handle_health_check)
        app.router.add_get("/health", handle_health_check)
        app.router.add_get("/webapp", handle_webapp)

        from aiogram.utils.executor import set_webhook

        executor_instance = set_webhook(
            dispatcher=dp,
            webhook_path=WEBHOOK_PATH,
            on_startup=on_startup_webhook,
            on_shutdown=on_shutdown,
            skip_updates=False,
            web_app=app,
            loop=loop,
        )
        port = int(os.environ.get("PORT", 8080))
        executor_instance.run_app(host="0.0.0.0", port=port)
