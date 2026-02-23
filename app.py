import re
import streamlit as st
from datetime import datetime
from supabase import create_client
import pdfplumber

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

# ---------- parsing utils ----------
DATE_RE = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")

def to_iso(ddmmyyyy: str) -> str:
    dd, mm, yyyy = ddmmyyyy.split("/")
    return f"{yyyy}-{mm}-{dd}"

def is_weekday_header(line: str) -> bool:
    s = line.strip().lower()
    return ("senin" in s and "selasa" in s and "rabu" in s and "kamis" in s)

def split_into_cells(line: str, n: int):
    """
    pdfplumber biasanya mempertahankan banyak spasi antar kolom.
    Kita split pakai 2+ spasi. Kalau kurang dari n, fallback pakai split 1 spasi (kurang stabil).
    """
    s = (line or "").strip()
    if not s:
        return []
    parts = re.split(r"\s{2,}", s)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) >= n:
        return parts[:n]
    # fallback (kalau PDF ekstraknya nge-collapse spasi)
    parts = s.split(" ")
    return parts

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def parse_pdf_roster_days(pdf_file, month_key: str):
    """
    Output: list[dict] untuk upsert ke roster_days.
    Struktur format PDF kamu (per blok minggu):
      - baris: "Senin Selasa ... Minggu"
      - baris: "02/02/2026 03/02/2026 ... 08/02/2026"
      - lalu 7 DPJP (sering multi-line)
      - lalu 1 baris A9 (7 nama) -> ignore
      - 1 baris A10 (pilot) (7 nama)
      - 1 baris A11 (copilot) (7 nama)
      - 1 baris A12 (7 sel berisi "nama, nama")
      - 1 baris A13
      - 1 baris A14
      - 1 baris A15
      - 1 baris A16 (observer) (kadang wrap 2 baris)
    """
    rows_out = []

    with pdfplumber.open(pdf_file) as pdf:
        # ambil teks per halaman, split jadi lines
        all_lines = []
        for page in pdf.pages:
            txt = page.extract_text() or ""
            for ln in txt.splitlines():
                ln = ln.rstrip()
                if ln.strip():
                    all_lines.append(ln)

    i = 0
    while i < len(all_lines):
        ln = all_lines[i]

        # cari header minggu
        if not is_weekday_header(ln):
            i += 1
            continue

        # baris berikutnya harus berisi banyak tanggal
        i += 1
        if i >= len(all_lines):
            break
        date_line = all_lines[i]
        dates = DATE_RE.findall(date_line)

        if len(dates) < 3:
            # bukan blok tabel yang kita mau
            i += 1
            continue

        # ambil hanya tanggal dalam month_key
        iso_dates = [to_iso(d) for d in dates if to_iso(d).startswith(month_key + "-")]
        # jumlah kolom yang kita parse = jumlah tanggal pada baris itu (bisa 7, halaman terakhir bisa 6)
        ncol = len(dates)

        # --- DPJP: multi-line entries, total ncol entries ---
        i += 1
        dpjp_entries = []
        current = ""

        def starts_dpjp(x: str) -> bool:
            x = x.strip()
            return x.startswith("drg.") or x.startswith("Dr.") or x.startswith("dr.") or x.startswith("Dr. drg.") or x.startswith("Dr. drg")

        while i < len(all_lines) and len(dpjp_entries) < ncol:
            l = all_lines[i]
            # stop kalau sudah masuk baris A9 (biasanya 7 token nama tanpa koma dan tanpa drg.)
            # tapi yang paling aman: A9 line tidak diawali "drg"/"Dr"
            if not starts_dpjp(l) and len(dpjp_entries) == 0 and DATE_RE.search(l):
                break

            if starts_dpjp(l) and current.strip():
                dpjp_entries.append(normalize_spaces(current))
                current = l
            else:
                current = (current + " " + l).strip() if current else l

            i += 1

            # heuristic: kalau current sudah panjang dan dpjp_entries hampir lengkap, lanjut
            if len(dpjp_entries) == ncol - 1 and current.strip():
                # cari break saat next line terlihat seperti A9
                pass

        if current.strip() and len(dpjp_entries) < ncol:
            dpjp_entries.append(normalize_spaces(current))

        # kalau DPJP kurang, skip blok (biar nggak ngaco)
        if len(dpjp_entries) < ncol:
            continue

        # --- A9 (ignore) ---
        if i >= len(all_lines): break
        a9_line = all_lines[i]; i += 1

        # --- A10 pilot ---
        if i >= len(all_lines): break
        pilot_line = all_lines[i]; i += 1
        pilots = split_into_cells(pilot_line, ncol)

        # --- A11 copilot ---
        if i >= len(all_lines): break
        copilot_line = all_lines[i]; i += 1
        copilots = split_into_cells(copilot_line, ncol)

        # --- A12..A16 rows ---
        def read_row_cells():
            nonlocal i
            if i >= len(all_lines):
                return []
            line = all_lines[i]; i += 1
            return split_into_cells(line, ncol)

        a12_cells = read_row_cells()
        a13_cells = read_row_cells()
        a14_cells = read_row_cells()
        a15_cells = read_row_cells()

        # observer kadang wrap 2 line; kita gabung kalau sel kurang dari ncol
        obs_cells = read_row_cells()
        if len(obs_cells) < ncol and i < len(all_lines):
            extra = split_into_cells(all_lines[i], ncol)
            # gabung per kolom kalau masuk akal
            if extra and len(extra) >= 2:
                i += 1
                # gabung string (simple)
                obs_cells = [normalize_spaces((obs_cells[j] if j < len(obs_cells) else "") + " " + (extra[j] if j < len(extra) else "")) for j in range(max(len(obs_cells), len(extra)))]
        # pad/truncate
        def pad(lst):
            lst = lst[:ncol]
            if len(lst) < ncol:
                lst = lst + [""] * (ncol - len(lst))
            return lst

        pilots = pad(pilots)
        copilots = pad(copilots)
        a12_cells = pad(a12_cells)
        a13_cells = pad(a13_cells)
        a14_cells = pad(a14_cells)
        a15_cells = pad(a15_cells)
        obs_cells = pad(obs_cells)

        # build per-date rows
        for idx in range(ncol):
            iso = to_iso(dates[idx])
            if not iso.startswith(month_key + "-"):
                continue

            row = {
                "month": month_key,
                "date": iso,
                "dpjp": dpjp_entries[idx] if idx < len(dpjp_entries) else "",
                "pilot": normalize_spaces(pilots[idx]),
                "copilot": normalize_spaces(copilots[idx]),
                "a12": [x.strip() for x in (a12_cells[idx] or "").split(",") if x.strip()],
                "a13": [x.strip() for x in (a13_cells[idx] or "").split(",") if x.strip()],
                "a14": [x.strip() for x in (a14_cells[idx] or "").split(",") if x.strip()],
                "a15": [x.strip() for x in (a15_cells[idx] or "").split(",") if x.strip()],
                "observers": [x.strip() for x in (obs_cells[idx] or "").split(",") if x.strip()],
                "erm_manual": "",
                "review_manual": "",
            }
            rows_out.append(row)

    return rows_out

# ---------- UI ----------
st.title("📅 Jadwal Jaga Residen (Import PDF → Roster Lengkap)")

col1, col2 = st.columns(2)
with col1:
    picked_month = st.text_input("Bulan (YYYY-MM)", value=datetime.now().strftime("%Y-%m"))
with col2:
    picked_date = st.date_input("Tanggal", value=datetime.now()).strftime("%Y-%m-%d")

exists = month_exists(picked_month)
st.caption("Status bulan: " + ("✅ tersedia" if exists else "⚠️ belum ada roster"))

tab_use, tab_admin = st.tabs(["Pakai (cek roster)", "Admin (Import PDF)"])

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

    pdf = st.file_uploader("Upload PDF Jadwal Bulan Ini", type=["pdf"])

    if st.button("IMPORT PDF → isi roster_days", disabled=not is_admin):
        if not pdf:
            st.error("Upload PDF dulu.")
        else:
            ensure_month(picked_month)
            rows = parse_pdf_roster_days(pdf, picked_month)

            if not rows:
                st.error("Parser tidak menemukan blok tabel. Kemungkinan format text extract beda—nanti aku adjust.")
            else:
                # simpan semua
                for row in rows:
                    upsert_roster_day(row)

                st.success(f"✅ Import sukses: {len(rows)} tanggal terisi.")
                st.info("Coba cek tab Pakai untuk tanggal tertentu.")
