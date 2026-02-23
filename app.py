import os
import re
import json
import streamlit as st
from datetime import datetime
from supabase import create_client
from pypdf import PdfReader

st.set_page_config(page_title="Jadwal Jaga Residen", layout="wide")

SUPABASE_URL = st.secrets.get("SUPABASE_URL", "")
SUPABASE_KEY = st.secrets.get("SUPABASE_ANON_KEY", "")
ADMIN_PIN = st.secrets.get("ADMIN_PIN", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("Supabase secrets belum di-set (SUPABASE_URL & SUPABASE_ANON_KEY).")
    st.stop()

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

def month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")

def iso_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def extract_text_from_pdf(uploaded_file) -> str:
    reader = PdfReader(uploaded_file)
    texts = []
    for page in reader.pages:
        t = page.extract_text() or ""
        texts.append(t)
    return "\n".join(texts)

def ensure_month(month: str):
    # insert if not exists
    existing = sb.table("rosters").select("month").eq("month", month).execute()
    if existing.data:
        return
    sb.table("rosters").insert({"month": month}).execute()

def split_names(s: str):
    s = (s or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]

def upsert_roster_day(payload: dict):
    sb.table("roster_days").upsert(payload).execute()

def get_roster_day(month: str, date: str):
    res = sb.table("roster_days").select("*").eq("month", month).eq("date", date).execute()
    return res.data[0] if res.data else None

def get_month_exists(month: str) -> bool:
    res = sb.table("rosters").select("month").eq("month", month).execute()
    return bool(res.data)

def save_assignment(month: str, date: str, payload: dict):
    sb.table("assignments").upsert({"month": month, "date": date, "payload": payload}).execute()

def load_assignment(month: str, date: str):
    res = sb.table("assignments").select("*").eq("month", month).eq("date", date).execute()
    return res.data[0]["payload"] if res.data else None

# ====== UI ======
st.title("Jadwal Jaga Residen")

colA, colB = st.columns([1, 1])
with colA:
    picked_month = st.text_input("Bulan (YYYY-MM)", value=month_key(datetime.now()))
with colB:
    picked_date = st.date_input("Tanggal", value=datetime.now()).strftime("%Y-%m-%d")

exists = get_month_exists(picked_month)
st.caption("Status bulan: " + ("✅ tersedia" if exists else "⚠️ belum ada roster"))

tabs = st.tabs(["Pakai (Generate)", "Admin (Import PDF / Edit)"])

# ===== TAB 1: Generate =====
with tabs[0]:
    if not exists:
        st.warning("Roster bulan ini belum ada. Admin import dulu.")
    roster = get_roster_day(picked_month, picked_date) if exists else None

    if roster:
        st.subheader("Roster hari ini")
        st.code(
            "\n".join([
                f"DPJP: {roster.get('dpjp','')}",
                f"Pilot: {roster.get('pilot','')}",
                f"Co-Pilot: {roster.get('copilot','')}",
                f"A12: {', '.join(roster.get('a12',[]) or [])}",
                f"A13: {', '.join(roster.get('a13',[]) or [])}",
                f"A14: {', '.join(roster.get('a14',[]) or [])}",
                f"A15: {', '.join(roster.get('a15',[]) or [])}",
                f"Observer: {', '.join(roster.get('observers',[]) or [])}",
            ])
        )

        erm_manual = st.text_input("ERM (manual)", value=roster.get("erm_manual","") or "")
        review_manual = st.text_input("Review (manual)", value=roster.get("review_manual","") or "")

        st.markdown("---")
        st.subheader("Input pasien (ringkas)")

        post_patients = st.text_area("POST OP (1 baris per pasien): Nama | POD I; POD II", height=120,
                                     placeholder="Tn. Irwan Zainuddin | POD III; POD IV")
        pre_patients = st.text_area("PRE OP (1 baris per pasien): Nama", height=100,
                                    placeholder="An. Lanang Almustofa Wahyudi")
        igd_patients = st.text_area("IGD (1 baris per pasien): Nama", height=80,
                                    placeholder="Sri Hartina")

        def pool_people():
            allp = (roster.get("a12",[]) or []) + (roster.get("a13",[]) or []) + (roster.get("a14",[]) or []) + (roster.get("a15",[]) or [])
            # unique preserve order
            seen = set()
            out = []
            for x in allp:
                x = (x or "").strip()
                if x and x not in seen:
                    seen.add(x)
                    out.append(x)
            return out

        def pickN(pool, n, start):
            if not pool:
                return []
            out = []
            for i in range(n):
                out.append(pool[(start+i) % len(pool)])
            return out

        def parse_post_lines(txt):
            items = []
            for line in (txt or "").splitlines():
                line = line.strip()
                if not line:
                    continue
                if "|" in line:
                    name, pods = [x.strip() for x in line.split("|", 1)]
                    pod_labels = [p.strip() for p in pods.split(";") if p.strip()]
                else:
                    name = line
                    pod_labels = ["POD I"]
                items.append((name, pod_labels))
            return items

        def parse_simple_lines(txt):
            return [ln.strip() for ln in (txt or "").splitlines() if ln.strip()]

        if st.button("Generate"):
            date_human = datetime.strptime(picked_date, "%Y-%m-%d").strftime("%A, %d/%m/%Y")
            header = f"Pembagian tugas jaga {date_human}\n\nPilot : {roster.get('pilot','-')}\nCo Pilot : {roster.get('copilot','-')}\n"

            out = header

            pool = pool_people()
            cursor = 0

            post_items = parse_post_lines(post_patients)
            if post_items:
                out += f"\n{len(post_items)} Post Op\n"
                for i, (name, pod_labels) in enumerate(post_items, start=1):
                    out += f"\n{i}. {name}\n"
                    for pod in pod_labels:
                        people = pickN(pool, 6, cursor)
                        cursor += 6
                        out += f"{pod} : {', '.join(people)}\n"

            pre_items = parse_simple_lines(pre_patients)
            if pre_items:
                out += f"\n{len(pre_items)} Pre op\n"
                for i, name in enumerate(pre_items, start=1):
                    out += f"\n{i}. {name}\n"
                    soap = pickN(pool, 4, cursor); cursor += 4
                    rmerm = pickN(pool, 6, cursor); cursor += 6
                    tsr = pickN(pool, 4, cursor); cursor += 4
                    out += f"Soap : {', '.join(soap)}\n"
                    out += f"RM/ERM : {', '.join(rmerm)}\n"
                    out += f"TSR : {', '.join(tsr)}\n"

            igd_items = parse_simple_lines(igd_patients)
            if igd_items:
                out += "\nIGD\n"
                for i, name in enumerate(igd_items, start=1):
                    out += f"\n{i}. {name}\n"
                    soap = pickN(pool, 4, cursor); cursor += 4
                    rmerm = pickN(pool, 6, cursor); cursor += 6
                    er = pickN(pool, 4, cursor); cursor += 4
                    out += f"Soap : {', '.join(soap)}\n"
                    out += f"RM/ERM : {', '.join(rmerm)}\n"
                    out += f"ER : {', '.join(er)}\n"

            out += f"\nObserver : {', '.join(roster.get('observers',[]) or [])}\n"
            out += f"\nERM : {erm_manual}\nReview : {review_manual}\n"

            st.text_area("Output (copy ke WA)", value=out, height=420)

            if st.button("Save output"):
                # sync manual fields back to roster_days
                roster["erm_manual"] = erm_manual
                roster["review_manual"] = review_manual
                upsert_roster_day(roster)

                save_assignment(picked_month, picked_date, {
                    "waText": out,
                    "inputs": {
                        "post": post_patients,
                        "pre": pre_patients,
                        "igd": igd_patients,
                        "erm_manual": erm_manual,
                        "review_manual": review_manual
                    }
                })
                st.success("Saved.")
    else:
        st.info("Roster tanggal ini belum ada (atau bulan belum tersedia).")

# ===== TAB 2: Admin =====
with tabs[1]:
    pin = st.text_input("Admin PIN", type="password")
    is_admin = (pin.strip() == ADMIN_PIN) if ADMIN_PIN else False

    st.markdown("### Import PDF (otomatis)")
    pdf = st.file_uploader("Upload PDF roster bulan ini", type=["pdf"])

    st.caption("Catatan: parsing PDF tabel itu kadang ada yang geser. Setelah import, cek 1–2 tanggal, kalau ada yang meleset kamu edit di bawah.")

    if st.button("Import PDF → Simpan 1 bulan", disabled=not is_admin):
        if not pdf:
            st.error("Pilih file PDF dulu.")
        else:
            ensure_month(picked_month)
            text = extract_text_from_pdf(pdf)

            # MVP parser sangat sederhana: cari pola tanggal dan ambil blok teks per tanggal.
            # Karena PDF tabel bisa berantakan, kita import sebagai DRAFT kosong untuk tiap tanggal yang ketemu,
            # lalu kamu koreksi cepat dengan editor.
            # Upgrade parser tabel detail bisa kita lakukan setelah kamu lihat hasil teksnya.
            dates = sorted(set(re.findall(r"\b(\d{2}/\d{2}/\d{4})\b", text)))
            iso_dates = []
            for d in dates:
                dd, mm, yyyy = d.split("/")
                iso = f"{yyyy}-{mm}-{dd}"
                if iso.startswith(picked_month + "-"):
                    iso_dates.append(iso)

            if not iso_dates:
                st.error("Tidak menemukan tanggal untuk bulan ini dari teks PDF. Kita perlu upgrade parser PDF-nya.")
            else:
                for d in iso_dates:
                    upsert_roster_day({
                        "month": picked_month,
                        "date": d,
                        "dpjp": "",
                        "pilot": "",
                        "copilot": "",
                        "a12": [],
                        "a13": [],
                        "a14": [],
                        "a15": [],
                        "observers": [],
                        "erm_manual": "",
                        "review_manual": ""
                    })
                st.success(f"Draft {len(iso_dates)} tanggal tersimpan. Sekarang isi detail via Editor tanggal di bawah.")

    st.markdown("---")
    st.markdown("### Editor tanggal (isi / koreksi cepat)")

    ed_date = st.date_input("Tanggal yang mau diedit", value=datetime.now()).strftime("%Y-%m-%d")

    if st.button("Load tanggal", disabled=not is_admin):
        r = get_roster_day(picked_month, ed_date)
        if not r:
            st.warning("Tanggal ini belum ada. Pastikan bulan sudah di-import.")
        else:
            st.session_state["edit_roster"] = r

    r = st.session_state.get("edit_roster")
    if r and r.get("date") == ed_date:
        dpjp = st.text_input("DPJP", value=r.get("dpjp","") or "")
        pilot = st.text_input("Pilot", value=r.get("pilot","") or "")
        copilot = st.text_input("Co-Pilot", value=r.get("copilot","") or "")

        a12 = st.text_area("A12 (koma)", value=", ".join(r.get("a12",[]) or []), height=80)
        a13 = st.text_area("A13 (koma)", value=", ".join(r.get("a13",[]) or []), height=80)
        a14 = st.text_area("A14 (koma)", value=", ".join(r.get("a14",[]) or []), height=80)
        a15 = st.text_area("A15 (koma)", value=", ".join(r.get("a15",[]) or []), height=80)
        obs = st.text_area("Observer (koma)", value=", ".join(r.get("observers",[]) or []), height=80)

        if st.button("Save tanggal", disabled=not is_admin):
            r["dpjp"] = dpjp.strip()
            r["pilot"] = pilot.strip()
            r["copilot"] = copilot.strip()
            r["a12"] = split_names(a12)
            r["a13"] = split_names(a13)
            r["a14"] = split_names(a14)
            r["a15"] = split_names(a15)
            r["observers"] = split_names(obs)

            upsert_roster_day(r)
            st.success("Saved.")
