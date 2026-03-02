import re
import csv
import io
import json
import random
import streamlit as st
from datetime import datetime, date as dt_date
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

# ---------- assignments helpers ----------
def get_assignment(month: str, date: str):
    def run():
        res = sb.table("assignments").select("*").eq("month", month).eq("date", date).execute()
        return res.data[0] if res.data else None
    return sb_exec(run)

def upsert_assignment(month: str, date: str, payload: dict):
    def run():
        sb.table("assignments").upsert(
            {"month": month, "date": date, "payload": payload},
            on_conflict="month,date"
        ).execute()
    sb_exec(run)

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

# ---------- Assignment generation ----------
DAY_ID = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]

def iso_to_dayname(iso_date: str) -> str:
    y, m, d = [int(x) for x in iso_date.split("-")]
    wd = dt_date(y, m, d).weekday()  # Mon=0
    return DAY_ID[wd]

def parse_patients_lines(text: str):
    """
    Each non-empty line becomes one patient record.
    Formats accepted:
    - "Nama pasien"
    - "Nama pasien | POD III" (for post-op)
    - "Nama pasien | POD 0"
    """
    out = []
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        parts = [p.strip() for p in ln.split("|")]
        name = parts[0]
        meta = parts[1] if len(parts) > 1 else ""
        out.append({"name": name, "meta": meta})
    return out

def roman_to_int(r: str):
    r = (r or "").strip().upper()
    if r in ["0", "O"]:
        return 0
    vals = {"I": 1, "V": 5, "X": 10}
    total = 0
    prev = 0
    for ch in reversed(r):
        v = vals.get(ch, 0)
        if v < prev:
            total -= v
        else:
            total += v
            prev = v
    return total

def int_to_roman(n: int):
    if n <= 0:
        return "0"
    mapping = [
        (10, "X"),
        (9, "IX"),
        (5, "V"),
        (4, "IV"),
        (1, "I"),
    ]
    out = ""
    for v, sym in mapping:
        while n >= v:
            out += sym
            n -= v
    return out

def normalize_pod_label(meta: str):
    meta = (meta or "").strip()
    if not meta:
        return None
    m = re.search(r"POD\s*([0-9]+|[IVX]+)", meta, flags=re.IGNORECASE)
    if not m:
        return None
    val = m.group(1)
    if val.isdigit():
        cur = int(val)
        nxt = cur + 1
        return (f"POD {cur}", f"POD {nxt}")
    cur_i = roman_to_int(val)
    nxt_i = cur_i + 1
    return (f"POD {int_to_roman(cur_i)}", f"POD {int_to_roman(nxt_i)}")

def uniq(seq):
    seen = set()
    out = []
    for x in seq:
        if not x:
            continue
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def seeded_rng(iso_date: str, salt: str = ""):
    # deterministic RNG per date (so regenerate same date => same output)
    seed = f"{iso_date}:{salt}"
    return random.Random(seed)

def shuffled(names: list[str], iso_date: str, salt: str):
    rng = seeded_rng(iso_date, salt)
    xs = [n.strip() for n in (names or []) if n and n.strip()]
    rng.shuffle(xs)
    return xs

def pick_least_used(pool: list[str], k: int, used: dict, iso_date: str, salt: str):
    """
    Pick k distinct people from pool, prioritizing those with the lowest usage count.
    Deterministic tie-break using a seeded shuffle.
    """
    if not pool or k <= 0:
        return []
    # tie-break order
    order = shuffled(pool, iso_date, salt + ":tiebreak")
    ordered = sorted(order, key=lambda x: (used.get(x, 0), x.lower()))
    chosen = []
    for x in ordered:
        if x in chosen:
            continue
        chosen.append(x)
        if len(chosen) >= k:
            break
    for x in chosen:
        used[x] = used.get(x, 0) + 1
    return chosen

def build_assignment(roster: dict, iso_date: str, post_ops: list, pre_ops: list, igds: list,
                     erm_manual: str, review_manual: str):
    # Pools
    a12 = roster.get("a12") or []
    a13 = roster.get("a13") or []
    a14 = roster.get("a14") or []
    a15 = roster.get("a15") or []
    a16 = roster.get("observers") or []  # angkatan 16 now included officially

    # Master pool for work assignment (pre-op/igd) includes everyone on duty (12-16)
    pool_work = uniq(shuffled(a12, iso_date, "w:a12") + shuffled(a13, iso_date, "w:a13") +
                     shuffled(a14, iso_date, "w:a14") + shuffled(a15, iso_date, "w:a15") +
                     shuffled(a16, iso_date, "w:a16"))

    # Post-op pool also includes everyone, but fairness tracked separately
    pool_post = pool_work[:]

    used_work = {p: 0 for p in pool_work}
    used_post = {p: 0 for p in pool_post}

    out = {
        "date": iso_date,
        "day_name": iso_to_dayname(iso_date),
        "pilot": roster.get("pilot", ""),
        "copilot": roster.get("copilot", ""),
        "erm_manual": erm_manual or "",
        "review_manual": review_manual or "",
        "post_op": [],
        "pre_op": [],
        "igd": [],
    }

    # ---- PRE OP (work-heavy): try to give everyone a work role if pre/igd exists ----
    # Role sizes (adapt down if pool is smaller)
    def k4():
        return min(4, max(1, len(pool_work)))
    def k6():
        return min(6, max(1, len(pool_work)))
    def k3():
        return min(3, max(1, len(pool_work)))

    # PRE OP patients
    for idx, p in enumerate(pre_ops, start=1):
        soap = pick_least_used(pool_work, k4(), used_work, iso_date, f"pre{idx}:soap")
        rm_erm = pick_least_used(pool_work, k6(), used_work, iso_date, f"pre{idx}:rm")
        tsr = pick_least_used(pool_work, k4(), used_work, iso_date, f"pre{idx}:tsr")

        out["pre_op"].append({
            "name": p["name"],
            "soap": soap,
            "rm_erm": rm_erm,
            "tsr": tsr,
        })

    # IGD patients
    for idx, p in enumerate(igds, start=1):
        soap = pick_least_used(pool_work, k4(), used_work, iso_date, f"igd{idx}:soap")
        rm_erm = pick_least_used(pool_work, k6(), used_work, iso_date, f"igd{idx}:rm")
        er = pick_least_used(pool_work, k3(), used_work, iso_date, f"igd{idx}:er")

        out["igd"].append({
            "name": p["name"],
            "soap": soap,
            "rm_erm": rm_erm,
            "er": er,
        })

    # ---- POST OP (visit): fairness separate from PRE/IGD ----
    def post_team_size():
        return min(6, max(1, len(pool_post)))

    total_post = len(post_ops)

    for idx, p in enumerate(post_ops, start=1):
        labels = normalize_pod_label(p.get("meta", "")) or ("POD I", "POD II")

        # If ONLY 1 post-op patient → split into 2 different teams
        if total_post == 1:
            team1 = pick_least_used(pool_post, post_team_size(), used_post, iso_date, f"post{idx}:team1")
            team2 = pick_least_used(pool_post, post_team_size(), used_post, iso_date, f"post{idx}:team2")

            out["post_op"].append({
                "name": p["name"],
                "pod_lines": [
                    {"label": labels[0], "team": team1},
                    {"label": labels[1], "team": team2},
                ]
            })

        # If 2 or more post-op patients → same team for POD n and POD n+1
        else:
            team = pick_least_used(pool_post, post_team_size(), used_post, iso_date, f"post{idx}:team")

            out["post_op"].append({
                "name": p["name"],
                "pod_lines": [
                    {"label": labels[0], "team": team},
                    {"label": labels[1], "team": team},
                ]
            })

    return out

def format_wa_text(assign: dict) -> str:
    day = assign["day_name"]
    iso = assign["date"]
    dd, mm, yyyy = iso.split("-")[2], iso.split("-")[1], iso.split("-")[0]
    header = f"Pembagian tugas jaga {day}, {dd}/{mm}/{yyyy}\n\n"
    header += f"Pilot : {assign.get('pilot','')}\n"
    header += f"Co Pilot : {assign.get('copilot','')}\n\n"

    lines = [header]

    # POST OP
    if assign.get("post_op"):
        lines.append(f"*{len(assign['post_op'])} Post Op*\n")
        for i, p in enumerate(assign["post_op"], start=1):
            lines.append(f"{i}. {p['name']}\n")
            for pl in p["pod_lines"]:
                team = ", ".join(pl["team"])
                lines.append(f"{pl['label']} : {team}\n")
            lines.append("\n")

    # PRE OP
    if assign.get("pre_op"):
        lines.append(f"*{len(assign['pre_op'])} Pre op*\n")
        for i, p in enumerate(assign["pre_op"], start=1):
            lines.append(f"{i}. {p['name']}\n")
            lines.append(f"Soap : {', '.join(p['soap'])}\n")
            lines.append(f"RM/ERM : {', '.join(p['rm_erm'])}\n")
            lines.append(f"TSR : {', '.join(p['tsr'])}\n\n")

    # IGD
    if assign.get("igd"):
        lines.append("*IGD*\n")
        for i, p in enumerate(assign["igd"], start=1):
            lines.append(f"{i}. {p['name']}\n")
            lines.append(f"Soap : {', '.join(p['soap'])}\n")
            lines.append(f"RM/ERM : {', '.join(p['rm_erm'])}\n")
            lines.append(f"ER : {', '.join(p['er'])}\n\n")

    # Footer
    lines.append(f"ERM : {assign.get('erm_manual','')}\n")
    lines.append(f"Review : {assign.get('review_manual','')}\n")

    return "".join(lines)

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
            st.subheader("Tim Jaga (dari roster)")
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
                    f"A16: {', '.join(r.get('observers',[]))}",
                ])
            )

            st.markdown("---")
            st.subheader("Buat Pembagian Tugas Jaga")

            saved = get_assignment(picked_month, picked_date)
            saved_payload = saved["payload"] if saved and isinstance(saved.get("payload"), dict) else None

            c1, c2 = st.columns(2)
            with c1:
                default_post = ""
                default_pre = ""
                default_igd = ""
                if saved_payload:
                    # reconstruct simple defaults from saved payload
                    if saved_payload.get("post_op"):
                        default_post = "\n".join([f"{p['name']} | {p['pod_lines'][0]['label']}" for p in saved_payload["post_op"]])
                    if saved_payload.get("pre_op"):
                        default_pre = "\n".join([p["name"] for p in saved_payload["pre_op"]])
                    if saved_payload.get("igd"):
                        default_igd = "\n".join([p["name"] for p in saved_payload["igd"]])

                post_text = st.text_area(
                    "POST OP (1 baris per pasien). Boleh: 'Nama | POD III' atau 'Nama | POD 0'",
                    value=default_post,
                    height=140
                )
                pre_text = st.text_area(
                    "PRE OP (1 baris per pasien)",
                    value=default_pre,
                    height=140
                )
                igd_text = st.text_area(
                    "IGD (1 baris per pasien)",
                    value=default_igd,
                    height=120
                )

            with c2:
                default_erm = saved_payload.get("erm_manual","") if saved_payload else ""
                default_rev = saved_payload.get("review_manual","") if saved_payload else ""
                erm_manual = st.text_input("ERM (manual)", value=default_erm)
                review_manual = st.text_input("Review (manual)", value=default_rev)

                st.caption("Catatan: aturan pairing A14/A15 pada MVP ini dibuat otomatis (urut alfabet).")

            post_ops = parse_patients_lines(post_text)
            pre_ops = parse_patients_lines(pre_text)
            igds = parse_patients_lines(igd_text)

            if st.button("Generate Pembagian"):
                assign = build_assignment(
                    roster=r,
                    iso_date=r["date"],
                    post_ops=post_ops,
                    pre_ops=pre_ops,
                    igds=igds,
                    erm_manual=erm_manual,
                    review_manual=review_manual,
                )
                wa = format_wa_text(assign)
                upsert_assignment(picked_month, picked_date, assign)

                st.success("✅ Pembagian dibuat & disimpan.")
                st.text_area("Output WA (copy-paste)", value=wa, height=420)

            if saved_payload and not st.session_state.get("generated_once", False):
                st.info("Ada pembagian tersimpan untuk tanggal ini. Klik 'Generate Pembagian' untuk regenerate dan overwrite.")

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
