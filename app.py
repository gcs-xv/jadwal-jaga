import re
import csv
import io
import streamlit as st
from datetime import datetime
from supabase import create_client

st.set_page_config(page_title="Jadwal Jaga Residen", layout="wide")

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_ANON_KEY"]
ADMIN_PIN = st.secrets["ADMIN_PIN"]

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------- helpers supabase ----------
def sb_exec(fn):
    try:
        return fn()
    except Exception as e:
        st.error("Supabase error:")
        st.code(str(e))
        st.stop()

def ensure_month(month: str):
    def run():
        res = sb.table("rosters").select("month").eq("month", month).execute()
        if not res.data:
            sb.table("rosters").insert({"month": month}).execute()
    sb_exec(run)

def upsert_roster_day(row: dict):
    def run():
        sb.table("roster_days").upsert(row, on_conflict="month,date").execute()
    sb_exec(run)

def month_exists(month: str) -> bool:
    def run():
        res = sb.table("rosters").select("month").eq("month", month).execute()
        return bool(res.data)
    return sb_exec(run)

def get_roster_day(month: str, date: str):
    def run():
        res = sb.table("roster_days").select("*").eq("month", month).eq("date", date).execute()
        return res.data[0] if res.data else None
    return sb_exec(run)

# ---------- CSV import utils ----------
REQUIRED_CSV_COLUMNS = [
    "month",
    "date",
    "dpjp",
    "pilot",
    "copilot",
    "a12",
    "a13",
    "a14",
    "a15",
    "observers",
]

def split_pipe_list(value: str):
    s = (value or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split("|") if x.strip()]

def parse_roster_csv(uploaded_file):
    """
    Expected CSV columns:
    month,date,dpjp,pilot,copilot,a12,a13,a14,a15,observers
    List fields are pipe-separated, e.g. "Ninik|Kusuma"
    """
    raw = uploaded_file.getvalue()
    text = raw.decode("utf-8", errors="replace")
    # Handle common CSV quirks:
    # - Excel/Sheets exports may use ';' as delimiter
    # - Some files include UTF-8 BOM in the first header
    sample = text[:4096]

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
    except Exception:
        dialect = csv.excel  # default comma

    reader = csv.DictReader(io.StringIO(text), dialect=dialect)

    # Normalize headers (strip spaces, remove BOM)
    fieldnames = []
    for fn in (reader.fieldnames or []):
        fn = (fn or "").strip().lstrip("\ufeff")
        fieldnames.append(fn)

    # Recreate reader with normalized fieldnames
    if reader.fieldnames:
        reader.fieldnames = fieldnames

    missing = [c for c in REQUIRED_CSV_COLUMNS if c not in fieldnames]
    if missing:
        raise ValueError(f"CSV missing columns: {', '.join(missing)}")

    rows = []
    for r in reader:
        month = (r.get("month") or "").strip()
        date = (r.get("date") or "").strip()
        if not month or not date:
            continue

        rows.append({
            "month": month,
            "date": date,
            "dpjp": (r.get("dpjp") or "").strip(),
            "pilot": (r.get("pilot") or "").strip(),
            "copilot": (r.get("copilot") or "").strip(),
            "a12": split_pipe_list(r.get("a12") or ""),
            "a13": split_pipe_list(r.get("a13") or ""),
            "a14": split_pipe_list(r.get("a14") or ""),
            "a15": split_pipe_list(r.get("a15") or ""),
            "observers": split_pipe_list(r.get("observers") or ""),
            "erm_manual": "",
            "review_manual": "",
        })
    return rows

# ---------- UI ----------
st.title("📅 Jadwal Jaga Residen (Import CSV → Roster Lengkap)")

col1, col2 = st.columns(2)
with col1:
    picked_month = st.text_input("Bulan (YYYY-MM)", value=datetime.now().strftime("%Y-%m"))
with col2:
    picked_date = st.date_input("Tanggal", value=datetime.now()).strftime("%Y-%m-%d")

exists = month_exists(picked_month)
st.caption("Status bulan: " + ("✅ tersedia" if exists else "⚠️ belum ada roster"))

tab_use, tab_admin = st.tabs(["Pakai (cek roster)", "Admin (Import CSV)"])

with tab_use:
    if not exists:
        st.warning("Roster bulan ini belum ada. Import dulu di tab Admin.")
    else:
        r = get_roster_day(picked_month, picked_date)
        if not r:
            st.info("Tanggal ini belum ada roster.")
        else:
            st.subheader("Roster")
            st.code(
                "\n".join([
                    f"Tanggal: {r['date']}",
                    f"DPJP: {r.get('dpjp','')}",
                    f"Pilot: {r.get('pilot','')}",
                    f"CoPilot: {r.get('copilot','')}",
                    f"A12: {', '.join(r.get('a12',[]))}",
                    f"A13: {', '.join(r.get('a13',[]))}",
                    f"A14: {', '.join(r.get('a14',[]))}",
                    f"A15: {', '.join(r.get('a15',[]))}",
                    f"Observer: {', '.join(r.get('observers',[]))}",
                ])
            )

with tab_admin:
    pin = st.text_input("Admin PIN", type="password")
    is_admin = (pin.strip() == ADMIN_PIN)

    csv_file = st.file_uploader("Upload CSV Roster (month,date,dpjp,pilot,copilot,a12,a13,a14,a15,observers)", type=["csv"])

    if st.button("IMPORT CSV → isi roster_days", disabled=not is_admin):
        if not csv_file:
            st.error("Upload CSV dulu.")
        else:
            try:
                rows = parse_roster_csv(csv_file)
            except Exception as e:
                st.error("Gagal baca CSV:")
                st.code(str(e))
                st.stop()

            if not rows:
                st.error("CSV kosong / tidak ada baris yang valid.")
                st.stop()

            # Ensure month rows exist for all months in the CSV
            months_in_csv = sorted({r["month"] for r in rows})
            for m in months_in_csv:
                ensure_month(m)

            # Upsert all days
            for r in rows:
                upsert_roster_day(r)

            st.success(f"✅ Import sukses: {len(rows)} tanggal terisi untuk bulan: {', '.join(months_in_csv)}")
            st.info("Coba cek tab Pakai untuk tanggal tertentu.")
