# -*- coding: utf-8 -*-
import logging
import io
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # للبيئات بدون شاشة
import matplotlib.pyplot as plt
import os

from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters



# =============== إعدادات عامة ===============
BOT_TOKEN = "8428782467:AAEtssvyqFtv8fuuvj2EuJ5qKBLEoyLFOoQ"
TARGET_MONTHLY = 50_000  # الهدف الشهري (ر.س)

# =============== لوجات ===============
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

application = Application.builder().token(BOT_TOKEN).build()
# =============== دعم العربية في الرسوم ===============
# الخطوط: نحاول اختيار خط يدعم العربية
matplotlib.rcParams["font.family"] = "sans-serif"
matplotlib.rcParams["font.sans-serif"] = ["Arial", "Segoe UI", "Tahoma", "DejaVu Sans"]
matplotlib.rcParams["axes.unicode_minus"] = False

# تشكيل النص العربي (كي لا تكون الحروف منفصلة)
try:
    import arabic_reshaper
    from bidi.algorithm import get_display

    def ar(text) -> str:
        if text is None:
            return ""
        s = str(text)
        return get_display(arabic_reshaper.reshape(s))
except Exception:
    # إن لم تتوفر الحزم، نُظهر النص كما هو
    def ar(text) -> str:
        return "" if text is None else str(text)

# =============== قراءة وتنظيف الإكسل ===============
def _try_read(file_bytes: bytes, header_row: int) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(file_bytes), header=header_row)
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    return df

def read_and_clean_excel(file_bytes: bytes) -> pd.DataFrame:
    """
    Clean read:
    - pick the correct header row
    - drop duplicate header lines inside data
    - drop grand-total rows & any rows with empty/NaN client
    - dedupe clients using a normalized key (keep newest / highest balance)
    Returns: columns [client, balance, last_sale, last_payment]
    """
    def _try_read(header_row: int):
        df0 = pd.read_excel(io.BytesIO(file_bytes), header=header_row)
        df0.columns = [c.strip() if isinstance(c, str) else c for c in df0.columns]
        return df0

    df = None
    for h in (2, 1, 0):
        try:
            tmp = _try_read(h)
            if "اسم الحساب" in tmp.columns:
                df = tmp
                break
        except Exception:
            continue
    if df is None:
        tmp = _try_read(0)
        raise ValueError(f"تعذر تحديد رؤوس الأعمدة. الأعمدة الحالية: {list(tmp.columns)}")

    # remove header-like rows duplicated inside the sheet
    header_like = pd.Series(False, index=df.index)
    if "اسم الحساب" in df.columns:
        header_like |= df["اسم الحساب"].astype(str).str.contains("اسم الحساب", na=False)
    if "الرصيد الحالى" in df.columns:
        header_like |= df["الرصيد الحالى"].astype(str).str.contains("الرصيد", na=False)
    df = df[~header_like].copy()

    # required columns
    for col in ["اسم الحساب", "الرصيد الحالى"]:
        if col not in df.columns:
            raise ValueError(f"❌ لا يوجد العمود '{col}'. الأعمدة الموجودة: {list(df.columns)}")

    # map columns
    df["client"] = df["اسم الحساب"]
    df["balance"] = pd.to_numeric(df["الرصيد الحالى"], errors="coerce")
    df["last_sale"] = pd.to_datetime(df.get("آخر بيع"), errors="coerce")
    df["last_payment"] = pd.to_datetime(df.get("آخر قبض"), errors="coerce")

    # *** CRITICAL FIXES ***
    # 1) drop rows where client is actually NaN (grand-total row)
    df = df[df["client"].notna()].copy()

    # 2) now safe to build normalized strings
    df["client_stripped"] = df["client"].astype(str).str.strip()

    # 3) drop totals and literal 'nan'
    total_kw = r"(الإجمال|إجمالي|الإجمالي|المجموع|Total|SUM)"
    df = df[
        (df["client_stripped"] != "") &
        (~df["client_stripped"].str.lower().eq("nan")) &
        (~df["client_stripped"].str.contains(total_kw, na=False))
    ].copy()

    # 4) drop zero/NaN balances
    df = df[df["balance"].fillna(0) > 0].copy()

    # 5) dedupe by a normalized key (remove spaces)
    df["client_key"] = df["client_stripped"].str.replace(r"\s+", "", regex=True)
    df = df.sort_values(
        by=["client_key", "last_sale", "last_payment", "balance"],
        ascending=[True, False, False, False]
    ).drop_duplicates(subset=["client_key"], keep="first")

    return df[["client", "balance", "last_sale", "last_payment"]]

    # بعد حساب overdue مباشرةً أضف هذا الفلتر:
    overdue = overdue[
        overdue["client"].notna() &
        (overdue["client"].astype(str).str.strip() != "") &
        (~overdue["client"].astype(str).str.strip().str.lower().eq("nan"))
    ].copy()





# =============== بناء التقرير ===============
def build_report_ar(df: pd.DataFrame, today_str: str):
    today = pd.to_datetime(today_str)

    total_clients = len(df)
    total_due = float(df["balance"].sum())

    # حساب التأخير على أساس آخر قبض
    df["delay_days"] = (today - df["last_payment"]).dt.days

    # عميل متأخر إذا (لا يوجد قبض) أو (تأخير > 30)
    overdue = df[(df["balance"] > 0) & ((df["delay_days"] > 30) | df["last_payment"].isna())].copy()

    # قاعدة: إذا كان آخر بيع أحدث من آخر قبض -> ليس متأخراً
    mask_invalid = (
        overdue["last_sale"].notna() &
        overdue["last_payment"].notna() &
        (overdue["last_sale"] > overdue["last_payment"])
    )
    overdue = overdue[~mask_invalid]

    overdue_clients = len(overdue)
    overdue_balance = float(overdue["balance"].sum())

    remaining = max(total_due - TARGET_MONTHLY, 0)

    # نص عربي بالكامل
    summary = (
        f"📅 التقرير اليومي: {today_str}\n"
        f"👥 عدد العملاء: {total_clients}\n"
        f"💰 إجمالي المستحق: {total_due:,.0f} ر.س\n"
        f"⏰ المتأخرون (+30): {overdue_clients} | رصيدهم: {overdue_balance:,.0f} ر.س\n"
        f"🎯 الهدف الشهري: {TARGET_MONTHLY:,.0f} ر.س\n"
        f"✅ المحصل (تقديري): {TARGET_MONTHLY:,.0f} ر.س\n"
        f"📉 المتبقي للتحصيل: {remaining:,.0f} ر.س\n\n"
        f"🔎 أهم المتابعات:\n"
    )

    for _, row in overdue.nlargest(10, "balance").iterrows():
        delay_days = int(row["delay_days"]) if pd.notna(row["delay_days"]) else 0
        summary += f"- {row['client']} | {row['balance']:,.0f} ر.س — متأخر منذ {delay_days} يوم.\n"

    return summary, overdue

# =============== الرسوم البيانية (عربي مشكّل) ===============
def make_charts_ar(overdue: pd.DataFrame):
    imgs = []
    if overdue.empty:
        return imgs

    # remove rows with empty/NaN client names
    df = overdue[
        overdue["client"].notna() & (overdue["client"].astype(str).str.strip() != "")
    ].copy()
    if df.empty:
        return imgs

    top = df.nlargest(10, "balance").copy()
    top["label_ar"] = top["client"].map(ar)

    # ---------- BAR (no overlap) ----------
    n = len(top)
    fig_h = max(4.8, 0.6 * n)  # dynamic height based on number of labels
    fig, ax = plt.subplots(figsize=(10, fig_h))
    ax.barh(top["label_ar"], top["balance"])
    ax.set_xlabel(ar("الرصيد (ر.س)"))
    ax.set_title(ar("أعلى العملاء المتأخرين"))
    ax.invert_yaxis()
    ax.tick_params(axis="y", labelsize=11)  # slightly smaller labels
    # extra left margin for long Arabic names
    plt.subplots_adjust(left=0.36, right=0.96, top=0.92, bottom=0.08)
    buf1 = io.BytesIO()
    plt.savefig(buf1, format="png", dpi=150)
    plt.close(fig)
    buf1.seek(0)
    imgs.append(("bar_ar.png", buf1))

    # ---------- PIE (legend outside, no overlapping labels) ----------
    pie_df = top.copy()

    # group small slices if too many labels
    if len(pie_df) > 6:
        major = pie_df.nlargest(6, "balance").copy()
        other = pie_df.drop(major.index)
        other_sum = float(other["balance"].sum())
        if other_sum > 0:
            major = pd.concat(
                [major, pd.DataFrame({"label_ar": [ar("أخرى")], "balance": [other_sum]})],
                ignore_index=True
            )
        pie_df = major

    fig, ax = plt.subplots(figsize=(7.8, 7.2))
    wedges, texts, autotexts = ax.pie(
        pie_df["balance"],
        labels=None,                # avoid overlapping text on the pie
        autopct="%1.1f%%",
        startangle=90
    )
    ax.set_title(ar("توزيع أرصدة العملاء المتأخرين"))
    # legend outside the pie
    ax.legend(
        wedges,
        pie_df["label_ar"],
        title=ar("العملاء"),
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        borderaxespad=0.,
        frameon=False
    )
    plt.tight_layout()
    plt.subplots_adjust(right=0.78)  # space for legend
    buf2 = io.BytesIO()
    plt.savefig(buf2, format="png", dpi=150)
    plt.close(fig)
    buf2.seek(0)
    imgs.append(("pie_ar.png", buf2))

    return imgs


# =============== تصدير إكسل منسّق ===============
def export_formatted_excel(df: pd.DataFrame) -> io.BytesIO:
    out = io.BytesIO()
    try:
        # نحاول xlsxwriter أولاً (أفضل للتنسيق)
        with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
            df_sorted = df.sort_values("balance", ascending=False)
            # إعادة أسماء الأعمدة للعربية في الملف المُصدر
            df_out = df_sorted.rename(columns={
                "client": "اسم الحساب",
                "balance": "الرصيد الحالى",
                "last_sale": "آخر بيع",
                "last_payment": "آخر قبض"
            })
            df_out.to_excel(writer, index=False, sheet_name="العملاء")
            ws = writer.sheets["العملاء"]
            for idx, col in enumerate(df_out.columns):
                max_len = max(df_out[col].astype(str).map(len).max(), len(col)) + 5
                ws.set_column(idx, idx, max_len)
        out.seek(0)
        return out
    except Exception:
        # في حال عدم توفر xlsxwriter، نستخدم openpyxl ونضبط عرض الأعمدة بعد الحفظ
        out2 = io.BytesIO()
        with pd.ExcelWriter(out2, engine="openpyxl") as writer:
            df_sorted = df.sort_values("balance", ascending=False)
            df_out = df_sorted.rename(columns={
                "client": "اسم الحساب",
                "balance": "الرصيد الحالى",
                "last_sale": "آخر بيع",
                "last_payment": "آخر قبض"
            })
            df_out.to_excel(writer, index=False, sheet_name="العملاء")
        out2.seek(0)

        try:
            from openpyxl import load_workbook
            wb = load_workbook(out2)
            ws = wb.active
            for col_cells in ws.columns:
                length = 0
                col_letter = col_cells[0].column_letter
                for c in col_cells:
                    if c.value is not None:
                        length = max(length, len(str(c.value)))
                ws.column_dimensions[col_letter].width = length + 5
            out_final = io.BytesIO()
            wb.save(out_final)
            out_final.seek(0)
            return out_final
        except Exception:
            out2.seek(0)
            return out2

# =============== دالة التحليل العليا ===============
def analyze_excel(file_bytes: bytes, today_str: str):
    df = read_and_clean_excel(file_bytes)
    summary_text, overdue = build_report_ar(df, today_str)
    imgs = make_charts_ar(overdue)
    excel_file = export_formatted_excel(df)
    return summary_text, excel_file, imgs

# =============== Handlers البوت ===============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"User {update.effective_user.id} sent /start")
    await update.message.reply_text("👋 أهلاً عدوول! أرسل ملف إكسل بصيغة .xlsx وسأرسل لك تقريرًا عربيًا مع رسوم بيانية.")

async def handle_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"User {update.effective_user.id} sent a file")
    try:
        tg_file = await update.message.document.get_file()
        file_bytes = await tg_file.download_as_bytearray()

        today_str = datetime.today().strftime("%Y-%m-%d")

        loop = asyncio.get_running_loop()
        # استخدام المنفّذ الافتراضي (أبسط وأكثر أمانًا)
        summary_text, excel_file, charts = await loop.run_in_executor(
            None, analyze_excel, file_bytes, today_str
        )

        # نص التقرير العربي
        await update.message.reply_text(summary_text)

        # ملف الإكسل المنسّق
        await update.message.reply_document(
            InputFile(excel_file, filename=f"تقرير_{today_str}.xlsx")
        )

        # الصور (الرسوم البيانية بالعربية)
        for fname, img in charts:
            await update.message.reply_photo(InputFile(img, filename=fname))

    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ حدث خطأ: {e}")

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.FileExtension("xlsx"), handle_excel))

    logger.info("🚀 Bot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()
