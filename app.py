import streamlit as st
from supabase import create_client
from datetime import datetime
import json
import re
from pypdf import PdfReader

st.set_page_config(page_title="Jadwal Jaga Residen", layout="wide")

# =========================
# SECRETS
# =========================
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_ANON_KEY"]
ADMIN_PIN = st.secrets["ADMIN_PIN"]

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# =========================
# DATABASE HELPERS
# =========================

def safe_execute(fn):
    try:
        return fn()
    except Exception as e:
        st.error("Supabase Error:")
        st.code(str(e))
        st.stop()

def ensure_month(month):
    def run():
        existing = sb.table("rosters").select("month").eq("month", month).execute()
        if not existing.data:
            sb.table("rosters").insert({"month": month}).execute()
    safe_execute(run)

def upsert_roster_day(payload):
    def run():
        sb.table("roster_days") \
          .upsert(payload, on_conflict="month,date") \
          .execute()
    safe_execute(run)

def get_roster_day(month, date):
    def run():
        res = sb.table("roster_days") \
            .select("*") \
            .eq("month", month) \
            .eq("date", date) \
            .execute()
        return res.data[0] if res.data else None
    return safe_execute(run)

def month_exists(month):
    def run():
        res = sb.table("rosters") \
            .select("month") \
            .eq("month", month) \
            .execute()
        return bool(res.data)
    return safe_execute(run)

# =========================
# PDF PARSER (SIMPLE VERSION)
# =========================

def extract_text_from_pdf(uploaded_file):
    reader = PdfReader(uploaded_file)
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    return text

def parse_pdf_simple(uploaded_file, month_key):
    text = extract_text_from_pdf(uploaded_file)

    date_pattern = r"\b\d{2}/\d{2}/\d{4}\b"
    dates = re.findall(date_pattern, text)

    iso_dates = []
    for d in dates:
        dd, mm, yyyy = d.split("/")
        iso = f"{yyyy}-{mm}-{dd}"
        if iso.startswith(month_key):
            iso_dates.append(iso)

    iso_dates = list(set(iso_dates))
    return sorted(iso_dates)

# =========================
# UI
# =========================

st.title("📅 Jadwal Jaga Residen")

col1, col2 = st.columns(2)

with col1:
    picked_month = st.text_input("Bulan (YYYY-MM)", value=datetime.now().strftime("%Y-%m"))

with col2:
    picked_date = st.date_input("Tanggal", value=datetime.now()).strftime("%Y-%m-%d")

exists = month_exists(picked_month)

st.caption("Status bulan: " + ("✅ tersedia" if exists else "⚠️ belum ada roster"))

tabs = st.tabs(["Pakai (Generate)", "Admin"])

# =========================
# TAB 1 - GENERATE
# =========================

with tabs[0]:

    if not exists:
        st.warning("Roster bulan belum tersedia. Admin import dulu.")
    else:
        roster = get_roster_day(picked_month, picked_date)

        if roster:
            st.subheader("Roster Hari Ini")
            st.json(roster)

            st.markdown("---")
            post_patients = st.text_area("POST OP (1 baris per pasien)")
            pre_patients = st.text_area("PRE OP (1 baris per pasien)")
            igd_patients = st.text_area("IGD (1 baris per pasien)")
            erm_manual = st.text_input("ERM Manual")
            review_manual = st.text_input("Review Manual")

            if st.button("Generate WA Format"):

                output = f"Pembagian tugas jaga {picked_date}\n\n"
                output += f"Pilot : {roster.get('pilot','-')}\n"
                output += f"Co Pilot : {roster.get('copilot','-')}\n\n"

                if post_patients.strip():
                    output += "POST OP\n"
                    output += post_patients + "\n\n"

                if pre_patients.strip():
                    output += "PRE OP\n"
                    output += pre_patients + "\n\n"

                if igd_patients.strip():
                    output += "IGD\n"
                    output += igd_patients + "\n\n"

                output += f"Observer : {', '.join(roster.get('observers',[]))}\n"
                output += f"\nERM : {erm_manual}\n"
                output += f"Review : {review_manual}\n"

                st.text_area("Output WA", value=output, height=400)

        else:
            st.info("Tanggal ini belum ada roster.")

# =========================
# TAB 2 - ADMIN
# =========================

with tabs[1]:

    pin = st.text_input("Admin PIN", type="password")

    if pin == ADMIN_PIN:

        pdf = st.file_uploader("Upload PDF Roster Bulan Ini", type=["pdf"])

        if st.button("Import PDF (Auto Create Dates)"):
            if not pdf:
                st.error("Upload file dulu.")
            else:
                ensure_month(picked_month)
                dates = parse_pdf_simple(pdf, picked_month)

                if not dates:
                    st.error("Tidak menemukan tanggal di PDF untuk bulan ini.")
                else:
                    for d in dates:
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
                    st.success(f"{len(dates)} tanggal berhasil dibuat sebagai draft.")

        st.markdown("---")
        st.subheader("Edit Tanggal")

        edit_date = st.date_input("Tanggal Edit", value=datetime.now()).strftime("%Y-%m-%d")

        roster = get_roster_day(picked_month, edit_date)

        if roster:

            dpjp = st.text_input("DPJP", value=roster.get("dpjp",""))
            pilot = st.text_input("Pilot", value=roster.get("pilot",""))
            copilot = st.text_input("Co-Pilot", value=roster.get("copilot",""))

            observers = st.text_area("Observer (koma)", value=", ".join(roster.get("observers",[])))

            if st.button("Save Edit"):

                upsert_roster_day({
                    "month": picked_month,
                    "date": edit_date,
                    "dpjp": dpjp,
                    "pilot": pilot,
                    "copilot": copilot,
                    "a12": [],
                    "a13": [],
                    "a14": [],
                    "a15": [],
                    "observers": [x.strip() for x in observers.split(",") if x.strip()],
                    "erm_manual": roster.get("erm_manual",""),
                    "review_manual": roster.get("review_manual","")
                })

                st.success("Berhasil disimpan.")

    else:
        st.warning("Masukkan Admin PIN untuk akses.")
