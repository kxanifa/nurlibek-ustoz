import logging
import sqlite3
import json
import io
import os
import asyncio
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from datetime import datetime

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher.filters import Text
from aiogram.utils import executor
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, InputFile, MediaGroup, WebAppInfo

# PDF
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as ReportLabImage, PageBreak
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

matplotlib.use('Agg')

# ================= SOZLAMALAR =================
API_TOKEN = '7058354398:AAFmuKIJ2N3gvpqm6EYeyQVxukYk6UTDBiM'

# ADMINLAR (Ikkita ID)
ADMINS = [1486347042, 1033652624]

# Railway Volume tekshiruvi
if os.path.exists("/app/data"):
    DB_FILE = "/app/data/bot_database.db"
else:
    DB_FILE = "bot_database.db"

WEB_APP_URL = "https://kxanifa.github.io/test-bot-webapp/" 

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ================= 1. DATABASE (AUTO-FIX BILAN) =================
def db_start():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    # Jadvallarni yaratish
    cur.execute("""CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, full_name TEXT, username TEXT, joined_at DATETIME)""")
    # Dastlabki yaratishda 'test_type' ni qo'shamiz
    cur.execute("""CREATE TABLE IF NOT EXISTS tests (code TEXT PRIMARY KEY, pdf_id TEXT, keys TEXT, stats TEXT, deadline TEXT, test_type TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS results (id INTEGER PRIMARY KEY AUTOINCREMENT, test_code TEXT, user_id INTEGER, full_name TEXT, correct_count INTEGER, percent REAL, grade TEXT, score_75 REAL, date DATETIME)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)""")
    
    # --- MUAMMONI TO'G'IRLASH QISMI ---
    # Agar eski bazada 'test_type' ustuni yo'q bo'lsa, uni qo'shamiz
    try:
        cur.execute("SELECT test_type FROM tests LIMIT 1")
    except sqlite3.OperationalError:
        print("DIQQAT: Bazaga 'test_type' ustuni qo'shilmoqda (Migratsiya)...")
        try:
            cur.execute("ALTER TABLE tests ADD COLUMN test_type TEXT DEFAULT 'rash'")
            conn.commit()
            print("MUVAFFAQIYATLI QO'SHILDI!")
        except Exception as e:
            print(f"Xatolik: {e}")

    conn.commit()
    conn.close()

# --- DB Helper ---
def set_channel(username):
    conn = sqlite3.connect(DB_FILE)
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('channel', ?)", (username,))
    conn.commit()
    conn.close()

def get_channel():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.execute("SELECT value FROM config WHERE key='channel'")
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def add_user(user_id, full_name, username):
    conn = sqlite3.connect(DB_FILE)
    conn.execute("INSERT OR IGNORE INTO users (user_id, full_name, username, joined_at) VALUES (?, ?, ?, ?)", (user_id, full_name, username, datetime.now()))
    conn.commit()
    conn.close()

def get_all_users():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.execute("SELECT user_id FROM users")
    users = [r[0] for r in cur.fetchall()]
    conn.close()
    return users

def save_test(code, pdf_id, keys_list, deadline, test_type):
    conn = sqlite3.connect(DB_FILE)
    keys_json = json.dumps(keys_list)
    stats = [{"total": 0, "correct": 0} for _ in range(len(keys_list))]
    conn.execute("INSERT OR REPLACE INTO tests VALUES (?, ?, ?, ?, ?, ?)", 
                 (code, pdf_id, keys_json, json.dumps(stats), deadline, test_type))
    conn.commit()
    conn.close()

def get_test(code):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.execute("SELECT * FROM tests WHERE code=?", (code,))
    row = cur.fetchone()
    conn.close()
    if row:
        try: keys = json.loads(row[2])
        except: keys = list(row[2])
        
        # Agar ustun bo'lmasa yoki None bo'lsa 'rash' olamiz
        try: t_type = row[5] if row[5] else 'rash'
        except: t_type = 'rash'

        return {
            "code": row[0], "pdf": row[1], "keys": keys,
            "stats": json.loads(row[3]), "deadline": row[4],
            "type": t_type
        }
    return None

def update_test_stats(code, new_stats):
    conn = sqlite3.connect(DB_FILE)
    conn.execute("UPDATE tests SET stats=? WHERE code=?", (json.dumps(new_stats), code))
    conn.commit()
    conn.close()

def save_result(test_code, user_id, full_name, correct, percent, grade, score_75):
    conn = sqlite3.connect(DB_FILE)
    conn.execute("INSERT INTO results (test_code, user_id, full_name, correct_count, percent, grade, score_75, date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                 (test_code, user_id, full_name, correct, percent, grade, score_75, datetime.now()))
    conn.commit()
    conn.close()

def get_results_list(test_code):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT * FROM results WHERE test_code=?", (test_code,))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_user_history(user_id):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT test_code, correct_count, grade, score_75, date FROM results WHERE user_id=? ORDER BY date DESC", (user_id,))
    rows = cur.fetchall()
    conn.close()
    return rows

def get_all_test_codes():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.execute("SELECT code FROM tests")
    codes = [r[0] for r in cur.fetchall()]
    conn.close()
    return codes

# ================= 2. LOGIKA =================
def calculate_score(user_ans_list, correct_keys_list, test_type):
    clean_user = [str(x).upper().replace(" ", "") if x else "X" for x in user_ans_list]
    clean_keys = [str(k).upper().replace(" ", "") for k in correct_keys_list]

    score_vector = []
    correct_count = 0
    limit = min(len(clean_user), len(clean_keys))
    
    for i in range(limit):
        if clean_user[i] == clean_keys[i]:
            score_vector.append(1)
            correct_count += 1
        else:
            score_vector.append(0)

    total = len(clean_keys)
    if total == 0: return 0, [], 0, "NC", 0

    percent = (correct_count / total) * 100
    
    if test_type == 'dtm':
        score_75 = percent 
        grade = f"{correct_count}/{total}"
    else:
        score_75 = (correct_count / total) * 75
        grade = "NC"
        if score_75 >= 70: grade = "A+"
        elif score_75 >= 65: grade = "A"
        elif score_75 >= 60: grade = "B+"
        elif score_75 >= 55: grade = "B"
        elif score_75 >= 50: grade = "C+"
        elif score_75 >= 45: grade = "C"

    return correct_count, score_vector, percent, grade, round(score_75, 2)

async def check_subscription(user_id):
    channel = get_channel()
    if not channel: return True
    try:
        member = await bot.get_chat_member(chat_id=channel, user_id=user_id)
        return member.status in ['creator', 'administrator', 'member']
    except: return False

# ================= 3. PDF/CHART =================
def create_charts(stats):
    q_nums = list(range(1, len(stats) + 1))
    corrects = [s['correct'] for s in stats]
    images = []
    plt.figure(figsize=(10, 4))
    plt.bar(q_nums, corrects, color='#90EE90', edgecolor='green')
    plt.title("Tahlil", fontweight='bold')
    plt.xticks(q_nums, fontsize=8, rotation=90)
    for i, v in enumerate(corrects): plt.text(i+1, v+0.1, str(v), ha='center', fontsize=8)
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    images.append(buf)
    plt.close()
    return images

def generate_reports(test_code):
    test = get_test(test_code)
    results = get_results_list(test_code)
    
    pdf_buf = io.BytesIO()
    doc = SimpleDocTemplate(pdf_buf, pagesize=landscape(A4))
    elements = [Paragraph(f"HISOBOT: {test_code}", getSampleStyleSheet()['Title']), Spacer(1, 20)]
    
    charts = create_charts(test['stats'])
    elements.append(Paragraph("Statistika", getSampleStyleSheet()['Heading2']))
    elements.append(ReportLabImage(charts[0], width=700, height=250))
    elements.append(PageBreak())
    
    headers = ['№', 'F.I.O', "To'g'ri", 'Ball', 'Baho/Natija', '%']
    table_data = [headers]
    for idx, res in enumerate(sorted(results, key=lambda x: x['score_75'], reverse=True), 1):
        table_data.append([str(idx), res['full_name'], str(res['correct_count']), f"{res['score_75']:.1f}", res['grade'], f"{res['percent']:.0f}%"])
    
    t = Table(table_data, colWidths=[40, 250, 60, 80, 80, 60])
    t.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,0), colors.lightblue), ('GRID', (0,0), (-1,-1), 1, colors.grey), ('ALIGN', (0,0), (-1,-1), 'CENTER'), ('ALIGN', (1,0), (1,-1), 'LEFT')]))
    elements.append(t)
    doc.build(elements)
    pdf_buf.seek(0)
    
    df = pd.DataFrame(results)
    excel_path = f"Excel_{test_code}.xlsx"
    if not df.empty: df[['full_name', 'correct_count', 'score_75', 'grade', 'percent', 'date']].to_excel(excel_path, index=False)
    else: pd.DataFrame().to_excel(excel_path)
    return pdf_buf, excel_path

# ================= 4. STATES & KEYBOARDS =================
def kb_admin(): return ReplyKeyboardMarkup(resize_keyboard=True).row("➕ Yangi Test", "📊 Hisobotlar").row("📢 Reklama", "🔗 Majburiy Obuna")
def kb_user(): return ReplyKeyboardMarkup(resize_keyboard=True).row("📝 Test ishlash", "👤 Mening natijalarim")
def kb_cancel(): return ReplyKeyboardMarkup(resize_keyboard=True).add("🔙 Bekor qilish")
def kb_test_type(): return ReplyKeyboardMarkup(resize_keyboard=True).row("DTM (Standart)", "RASH (Milliy)").add("🔙 Bekor qilish")

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
@dp.message_handler(commands=['start'], state="*")
async def cmd_start(message: types.Message, state: FSMContext):
    await state.finish()
    add_user(message.from_user.id, message.from_user.full_name, message.from_user.username)
    
    if message.from_user.id in ADMINS: 
        await message.answer(f"Admin panel (ID: {message.from_user.id}):", reply_markup=kb_admin())
    else:
        if not await check_subscription(message.from_user.id):
            chan = get_channel()
            if chan:
                kb = InlineKeyboardMarkup().add(InlineKeyboardButton("A'zo bo'lish", url=f"https://t.me/{chan.replace('@', '')}")).add(InlineKeyboardButton("✅ Tekshirish", callback_data="check_sub"))
                await message.answer(f"Botdan foydalanish uchun {chan} ga a'zo bo'ling:", reply_markup=kb)
                return
        await message.answer("Xush kelibsiz!", reply_markup=kb_user())

@dp.callback_query_handler(text="check_sub", state="*")
async def check_sub_call(call: types.CallbackQuery):
    if await check_subscription(call.from_user.id):
        await call.message.delete()
        await call.message.answer("Rahmat!", reply_markup=kb_user())
    else: await call.answer("❌ A'zo bo'lmadingiz!", show_alert=True)

@dp.message_handler(Text(equals="🔙 Bekor qilish"), state="*")
async def cancel(message: types.Message, state: FSMContext):
    await state.finish()
    if message.from_user.id in ADMINS:
        await message.answer("Bekor qilindi.", reply_markup=kb_admin())
    else:
        await message.answer("Bekor qilindi.", reply_markup=kb_user())

# --- ADMIN ACTIONS ---
@dp.message_handler(Text(equals="🔗 Majburiy Obuna"), user_id=ADMINS)
async def ch_menu(message: types.Message):
    await message.answer("Yangi username (@kanal) yoki 'del':", reply_markup=kb_cancel())
    await AdminStates.waiting_channel.set()

@dp.message_handler(state=AdminStates.waiting_channel, user_id=ADMINS)
async def ch_save(message: types.Message, state: FSMContext):
    if message.text == 'del': set_channel(None)
    else: set_channel(message.text)
    await message.answer("Saqlandi.", reply_markup=kb_admin())
    await state.finish()

# TEST YARATISH
@dp.message_handler(Text(equals="➕ Yangi Test"), user_id=ADMINS)
async def new_test(message: types.Message):
    await message.answer("Test kodini kiriting:", reply_markup=kb_cancel())
    await AdminStates.waiting_code.set()

@dp.message_handler(state=AdminStates.waiting_code, user_id=ADMINS)
async def code_step(message: types.Message, state: FSMContext):
    if get_test(message.text.strip()):
        await message.answer("Bu kod band.")
        return
    await state.update_data(code=message.text.strip())
    await message.answer("Test turini tanlang:", reply_markup=kb_test_type())
    await AdminStates.waiting_type.set()

@dp.message_handler(state=AdminStates.waiting_type, user_id=ADMINS)
async def type_step(message: types.Message, state: FSMContext):
    t_type = "dtm"
    if "RASH" in message.text: t_type = "rash"
    
    await state.update_data(test_type=t_type)
    await message.answer("PDF faylni yuklang (yoki 'yoq' deb yozing):", reply_markup=kb_cancel())
    await AdminStates.waiting_pdf.set()

@dp.message_handler(content_types=['document', 'text'], state=AdminStates.waiting_pdf, user_id=ADMINS)
async def pdf_step(message: types.Message, state: FSMContext):
    if message.document: pdf_id = message.document.file_id
    else: pdf_id = None
    await state.update_data(pdf=pdf_id)
    await message.answer("Kalitlarni yuboring (TXT yoki matn):\n\nDTM uchun: A B C D...\nRash uchun: A B ... a)5b)10")
    await AdminStates.waiting_keys.set()

@dp.message_handler(content_types=['document', 'text'], state=AdminStates.waiting_keys, user_id=ADMINS)
async def keys_step(message: types.Message, state: FSMContext):
    keys_raw = ""
    if message.document:
        f_info = await bot.get_file(message.document.file_id)
        content = await bot.download_file(f_info.file_path)
        keys_raw = content.read().decode('utf-8')
    else: keys_raw = message.text
    
    keys_list = keys_raw.replace("\n", " ").split()
    keys_list = [k.strip().upper() for k in keys_list if k.strip()]
    
    if not keys_list:
        await message.answer("Kalitlar topilmadi.")
        return
    
    await state.update_data(keys=keys_list)
    await message.answer(f"✅ {len(keys_list)} ta kalit.\nTugash vaqti (KK.OO.YYYY SS:MM) yoki 'yoq':")
    await AdminStates.next()

@dp.message_handler(state=AdminStates.waiting_deadline, user_id=ADMINS)
async def deadline_step(message: types.Message, state: FSMContext):
    dl = message.text.strip()
    if dl.lower() != 'yoq':
        try: datetime.strptime(dl, "%d.%m.%Y %H:%M")
        except: 
            await message.answer("Format xato!")
            return
    else: dl = None
    
    data = await state.get_data()
    save_test(data['code'], data['pdf'], data['keys'], dl, data['test_type'])
    await message.answer(f"✅ Test saqlandi: {data['code']} ({data['test_type']})", reply_markup=kb_admin())
    await state.finish()

# REPORT
@dp.message_handler(Text(equals="📊 Hisobotlar"), user_id=ADMINS)
async def report_menu(message: types.Message):
    codes = get_all_test_codes()
    mk = InlineKeyboardMarkup(row_width=2)
    for c in codes: mk.insert(InlineKeyboardButton(f"📂 {c}", callback_data=f"rep:{c}"))
    await message.answer("Tanlang:", reply_markup=mk)

@dp.callback_query_handler(lambda c: c.data.startswith('rep:'), user_id=ADMINS)
async def send_rep(call: types.CallbackQuery):
    code = call.data.split(":")[1]
    await call.answer("Yuklanmoqda...")
    try:
        pdf, xls = generate_reports(code)
        media = MediaGroup()
        media.attach_document(InputFile(pdf, filename=f"{code}.pdf"), caption=f"{code} hisoboti")
        media.attach_document(InputFile(open(xls, 'rb'), filename=xls))
        await bot.send_media_group(call.from_user.id, media=media)
    except Exception as e: await call.message.answer(f"Xato: {e}")

# USER TEST SOLVING
@dp.message_handler(Text(equals="📝 Test ishlash"))
async def start_solve(message: types.Message):
    if not await check_subscription(message.from_user.id):
        chan = get_channel()
        if chan:
            url = f"https://t.me/{chan.replace('@', '')}"
            kb = InlineKeyboardMarkup().add(InlineKeyboardButton("A'zo bo'lish", url=url)).add(InlineKeyboardButton("✅ Tekshirish", callback_data="check_sub"))
            await message.answer("Kanalga a'zo bo'ling!", reply_markup=kb)
            return
    await message.answer("Test kodini yozing:", reply_markup=kb_cancel())
    await UserStates.waiting_code.set()

@dp.message_handler(state=UserStates.waiting_code)
async def check_test_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    test = get_test(code)
    if not test:
        await message.answer("Test topilmadi.")
        return
    if test['deadline'] and datetime.now() > datetime.strptime(test['deadline'], "%d.%m.%Y %H:%M"):
        await message.answer("Vaqt tugagan!")
        return

    await state.update_data(test_code=code)
    await message.answer("Ism-Familiyangiz:")
    await UserStates.waiting_name.set()

@dp.message_handler(state=UserStates.waiting_name)
async def enter_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    data = await state.get_data()
    test = get_test(data['test_code'])
    
    if test['pdf']:
        # Protect content
        await message.answer_document(test['pdf'], caption="Test savollari (Himoyalangan).", protect_content=True)
    
    q_count = len(test['keys'])
    t_type = test['type']
    web_app_url = f"{WEB_APP_URL}?count={q_count}&type={t_type}"
    
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(text="🚀 JAVOBLARNI BELGILASH", web_app=WebAppInfo(url=web_app_url)))
    kb.add(KeyboardButton("🔙 Bekor qilish"))
    
    await message.answer("Boshlash uchun tugmani bosing:", reply_markup=kb)
    await UserStates.solving.set()

@dp.message_handler(content_types=['web_app_data'], state=UserStates.solving)
async def web_app_data_handler(message: types.Message, state: FSMContext):
    raw_data = message.web_app_data.data
    user_answers = json.loads(raw_data)
    
    data = await state.get_data()
    test = get_test(data['test_code'])
    
    corr, vec, perc, grade, score_75 = calculate_score(user_answers, test['keys'], test['type'])
    
    stats = test['stats']
    for i, is_corr in enumerate(vec):
        if i < len(stats):
            stats[i]['total'] += 1
            if is_corr: stats[i]['correct'] += 1
    update_test_stats(data['test_code'], stats)
    
    save_result(data['test_code'], message.from_user.id, data['name'], corr, perc, grade, score_75)
    
    res_text = f"✅ Natija qabul qilindi!\n\n👤 {data['name']}\n🎯 To'g'ri: {corr}/{len(test['keys'])}\n"
    if test['type'] == 'dtm':
        res_text += f"📊 Natija: {perc:.1f}%"
    else:
        res_text += f"📊 Ball: {score_75:.1f} (max 75)\n🏆 Baho: {grade}"
        
    await message.answer(res_text, reply_markup=kb_user(), protect_content=True)
    await state.finish()

# REKLAMA
@dp.message_handler(Text(equals="📢 Reklama"), user_id=ADMINS)
async def ad_menu(message: types.Message):
    await message.answer("Postni yuboring:", reply_markup=kb_cancel())
    await AdminStates.waiting_ad.set()

@dp.message_handler(content_types=types.ContentTypes.ANY, state=AdminStates.waiting_ad, user_id=ADMINS)
async def ad_send(message: types.Message, state: FSMContext):
    users = get_all_users()
    cnt = 0
    for u in users:
        if u in ADMINS: continue
        try: 
            await message.copy_to(u, protect_content=True)
            cnt += 1
            await asyncio.sleep(0.05)
        except: pass
    await message.answer(f"Yuborildi: {cnt}", reply_markup=kb_admin())
    await state.finish()

# HISTORY
@dp.message_handler(Text(equals="👤 Mening natijalarim"))
async def history(message: types.Message):
    rows = get_user_history(message.from_user.id)
    if not rows: await message.answer("Tarix bo'sh.")
    else:
        txt = "📂 Tarix:\n"
        for r in rows: txt += f"🔹 {r['test_code']} | {r['score_75']:.1f} ball | {r['grade']}\n"
        await message.answer(txt, protect_content=True)

# ================= 6. SPAM BLOCKER =================
@dp.message_handler(content_types=types.ContentTypes.ANY, state=None)
async def block_spam(message: types.Message):
    if message.from_user.id in ADMINS:
        return
    if message.text in ["📝 Test ishlash", "👤 Mening natijalarim", "🔙 Bekor qilish"]:
        return
    try: await message.delete()
    except: pass

if __name__ == '__main__':
    db_start()
    executor.start_polling(dp, skip_updates=True)
