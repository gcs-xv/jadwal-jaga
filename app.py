import re
import csv
import io
import json
import random
import streamlit as st
from datetime import datetime, date as dt_date
from supabase import create_client


st.set_page_config(page_title="Jadwal Jaga Residen", layout="wide")

st.markdown("""
<style>
/* ---- playful but clean theme ---- */
:root {
  --p1: #7c3aed;   /* purple */
  --p2: #ec4899;   /* pink */
  --p3: #22c55e;   /* green */
  --bg: #fff7ff;   /* soft */
}
.block-container { padding-top: 1.2rem; }

.hero {
  background: linear-gradient(135deg, #f3e8ff 0%, #ffe4f2 40%, #ecfeff 100%);
  border-radius: 18px;
  padding: 18px 18px;
  box-shadow: 0 6px 18px rgba(0,0,0,0.08);
  margin-bottom: 14px;
}
.hero-title {
  font-size: 28px;
  font-weight: 800;
  color: #3b0764;
  margin: 0;
  line-height: 1.15;
}
.hero-sub {
  margin-top: 6px;
  color: rgba(60, 7, 100, 0.75);
  font-weight: 600;
}

.card {
  border-radius: 18px;
  padding: 16px 16px 12px 16px;
  box-shadow: 0 5px 16px rgba(0,0,0,0.08);
  margin-bottom: 14px;
  border: 1px solid rgba(124,58,237,0.12);
  background: white;
}

.card.post { border-left: 8px solid var(--p2); }
.card.pre  { border-left: 8px solid var(--p1); }
.card.igd  { border-left: 8px solid var(--p3); }

.card h3 {
  margin: 0 0 10px 0;
  font-size: 18px;
  font-weight: 800;
}

.small-note {
  font-size: 12px;
  opacity: .85;
  margin-top: 6px;
}

.stButton>button {
  border-radius: 14px !important;
  font-weight: 800 !important;
  padding: .55rem .9rem !important;
}

input, textarea, .stSelectbox div[data-baseweb="select"] {
  border-radius: 14px !important;
}

@media (max-width: 768px) {
  .hero-title { font-size: 24px; }
  .card { padding: 14px; }
}
</style>
""", unsafe_allow_html=True)

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
    # Cohort pools (from roster)
    a12 = roster.get("a12") or []
    a13 = roster.get("a13") or []
    a14 = roster.get("a14") or []
    a15 = roster.get("a15") or []
    a16 = roster.get("observers") or []  # A16 participates officially (no separate observer section)

    # Deterministic shuffle per date (so regenerate same date => same result)
    a12_s = shuffled(a12, iso_date, "rot:a12")
    a13_s = shuffled(a13, iso_date, "rot:a13")
    a14_s = shuffled(a14, iso_date, "rot:a14")
    a15_s = shuffled(a15, iso_date, "rot:a15")
    a16_s = shuffled(a16, iso_date, "rot:a16")

    def cohort_order(team: list[str]) -> list[str]:
        """Force output order: A12 → A13 → A14 → A15 → A16 (each in the roster-shuffled order)."""
        s = set(team or [])
        out = []
        for x in a12_s:
            if x in s: out.append(x)
        for x in a13_s:
            if x in s: out.append(x)
        for x in a14_s:
            if x in s: out.append(x)
        for x in a15_s:
            if x in s: out.append(x)
        for x in a16_s:
            if x in s: out.append(x)
        return uniq(out)

    def all_people() -> list[str]:
        return uniq(a12_s + a13_s + a14_s + a15_s + a16_s)

    # Deterministic pairs for A14/A15 (pairing stays consistent within the date)
    def make_pairs(xs: list[str]) -> list[list[str]]:
        pairs = []
        i = 0
        while i < len(xs):
            if i + 1 < len(xs):
                pairs.append([xs[i], xs[i + 1]])
                i += 2
            else:
                pairs.append([xs[i]])
                i += 1
        return pairs

    a14_pairs = make_pairs(a14_s)
    a15_pairs = make_pairs(a15_s)

    class Rot:
        def __init__(self, items):
            self.items = items[:] if items else []
            self.i = 0
        def next(self):
            if not self.items:
                return None
            v = self.items[self.i % len(self.items)]
            self.i += 1
            return v

    r12 = Rot(a12_s)
    r13 = Rot(a13_s)
    r14 = Rot(a14_pairs)
    r15 = Rot(a15_pairs)
    r16 = Rot(a16_s)

    # Track who already got ANY jobdesk today
    unused = set(all_people())

    def take_from_rot(rot: Rot, k: int) -> list[str]:
        picked = []
        seen_guard = 0
        while len(picked) < k and rot.items and seen_guard < (len(rot.items) * 3 + 10):
            v = rot.next()
            if v is None:
                break
            # v can be a pair list or a string
            if isinstance(v, list):
                for x in v:
                    if x and x not in picked:
                        picked.append(x)
                        if len(picked) >= k:
                            break
            else:
                if v and v not in picked:
                    picked.append(v)
            seen_guard += 1
        return picked

    def pick_k_for_role(k: int, salt: str) -> list[str]:
        """
        Pick k people prioritizing those still unused, but keep cohort structure.
        We try to include: A12 (if exists), A13 (if exists), A14 pair, A15 pair, then A16,
        then fill remaining from anyone.
        """
        k = max(1, min(k, len(all_people())))
        picked = []

        # 1) ensure 1 A12 + 1 A13 if possible (prefer unused)
        if a12_s:
            cand = [x for x in a12_s if x in unused]
            picked += cand[:1] if cand else [a12_s[0]]
        if a13_s and len(picked) < k:
            cand = [x for x in a13_s if x in unused and x not in picked]
            picked += cand[:1] if cand else [a13_s[0]]

        # 2) add one A14 pair and one A15 pair (prefer unused inside the pair)
        if len(picked) < k and a14_pairs:
            pair = r14.next() or []
            pair = [x for x in pair if x]
            # prioritize unused members of the pair
            pair_u = [x for x in pair if x in unused and x not in picked]
            pair_r = [x for x in pair if x not in picked]
            for x in (pair_u + pair_r):
                if len(picked) < k and x not in picked:
                    picked.append(x)

        if len(picked) < k and a15_pairs:
            pair = r15.next() or []
            pair = [x for x in pair if x]
            pair_u = [x for x in pair if x in unused and x not in picked]
            pair_r = [x for x in pair if x not in picked]
            for x in (pair_u + pair_r):
                if len(picked) < k and x not in picked:
                    picked.append(x)

        # 3) add one A16 if available and still need
        if len(picked) < k and a16_s:
            cand = [x for x in a16_s if x in unused and x not in picked]
            if cand:
                picked.append(cand[0])
            else:
                # deterministic pick
                picked.append(a16_s[0])

        # 4) fill the rest by taking from everyone, prioritizing unused, deterministic by shuffled order
        everyone = shuffled(all_people(), iso_date, f"fill:{salt}")
        for x in everyone:
            if len(picked) >= k:
                break
            if x in picked:
                continue
            # prefer unused first
            if x in unused:
                picked.append(x)
        # if still short, allow repeats
        for x in everyone:
            if len(picked) >= k:
                break
            if x in picked:
                continue
            picked.append(x)

        # mark used
        for x in picked:
            if x in unused:
                unused.remove(x)

        return cohort_order(picked)

    def split_all_people_into_two_teams() -> tuple[list[str], list[str]]:
        """
        Split fairly within each cohort: roughly half to team1, rest to team2.
        Keep cohort order in each team.
        """
        t1 = []
        t2 = []
        for cohort in [a12_s, a13_s, a14_s, a15_s, a16_s]:
            n = len(cohort)
            if n == 0:
                continue
            half = (n + 1) // 2
            t1 += cohort[:half]
            t2 += cohort[half:]
        return (cohort_order(t1), cohort_order(t2))

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

    # ---------- POST OP ----------
    total_post = len(post_ops)
    for idx, p in enumerate(post_ops, start=1):
        labels = normalize_pod_label(p.get("meta", "")) or ("POD I", "POD II")

        if total_post == 1:
            team1, team2 = split_all_people_into_two_teams()
            # Everyone gets jobdesk from post-op split
            unused.clear()
            out["post_op"].append({
                "name": p["name"],
                "pod_lines": [
                    {"label": labels[0], "team": team1},
                    {"label": labels[1], "team": team2},
                ]
            })
        else:
            # per patient: one stable team for both POD lines, size 6 (adapt)
            k = min(6, max(1, len(all_people())))
            team = pick_k_for_role(k, f"post{idx}")
            out["post_op"].append({
                "name": p["name"],
                "pod_lines": [
                    {"label": labels[0], "team": team},
                    {"label": labels[1], "team": team},
                ]
            })

    # ---------- PRE OP ----------
    # Role sizes (adapt down if fewer people)
    k_soap = min(4, max(1, len(all_people())))
    k_rm = min(6, max(1, len(all_people())))
    k_tsr = min(4, max(1, len(all_people())))

    for idx, p in enumerate(pre_ops, start=1):
        soap = pick_k_for_role(k_soap, f"pre{idx}:soap")
        rm_erm = pick_k_for_role(k_rm, f"pre{idx}:rm")
        tsr = pick_k_for_role(k_tsr, f"pre{idx}:tsr")
        out["pre_op"].append({
            "name": p["name"],
            "soap": soap,
            "rm_erm": rm_erm,
            "tsr": tsr,
        })

    # ---------- IGD ----------
    k_er = min(3, max(1, len(all_people())))
    for idx, p in enumerate(igds, start=1):
        soap = pick_k_for_role(k_soap, f"igd{idx}:soap")
        rm_erm = pick_k_for_role(k_rm, f"igd{idx}:rm")
        er = pick_k_for_role(k_er, f"igd{idx}:er")
        out["igd"].append({
            "name": p["name"],
            "soap": soap,
            "rm_erm": rm_erm,
            "er": er,
        })

    # If there are PRE/IGD tasks and some people are still unused, force-assign them into the last RM/ERM (append, keep order)
    if (pre_ops or igds) and unused:
        leftovers = cohort_order(list(unused))
        if out["igd"]:
            out["igd"][-1]["rm_erm"] = cohort_order(out["igd"][-1]["rm_erm"] + leftovers)
        elif out["pre_op"]:
            out["pre_op"][-1]["rm_erm"] = cohort_order(out["pre_op"][-1]["rm_erm"] + leftovers)
        unused.clear()

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
st.markdown(
    "<div class='hero'>"
    "<div class='hero-title'>🌸 Jadwal Jaga Residen</div>"
    "<div class='hero-sub'>Isi pasien tinggal klik tambah — tanpa tanda | dan tanpa ribet. Output siap copy ke WhatsApp.</div>"
    "</div>",
    unsafe_allow_html=True
)

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
                # Initialize session state rows
                if "post_rows" not in st.session_state:
                    st.session_state.post_rows = []
                if "pre_rows" not in st.session_state:
                    st.session_state.pre_rows = []
                if "igd_rows" not in st.session_state:
                    st.session_state.igd_rows = []

                # If there is a saved payload and the UI is still empty, preload once
                if saved_payload and not st.session_state.get("_ui_preloaded", False):
                    st.session_state.post_rows = [
                        {"name": p.get("name", ""), "pod": (p.get("pod_lines", [{}])[0].get("label", "POD 0").replace("POD ", "").strip() or "0")}
                        for p in (saved_payload.get("post_op") or [])
                    ]
                    st.session_state.pre_rows = [{"name": p.get("name", "")} for p in (saved_payload.get("pre_op") or [])]
                    st.session_state.igd_rows = [{"name": p.get("name", "")} for p in (saved_payload.get("igd") or [])]
                    st.session_state._ui_preloaded = True

                # ---- POST OP card ----
                st.markdown("<div class='card post'><h3>🔴 POST OP</h3>", unsafe_allow_html=True)
                st.caption("Klik tambah pasien. POD cukup pilih angka/romawi di dropdown. (Tidak perlu ketik '|' ya.)")

                if st.button("➕ Tambah Post Op", key="btn_add_post"):
                    st.session_state.post_rows.append({"name": "", "pod": "0"})

                POD_OPTIONS = ["0", "I", "II", "III", "IV", "V", "VI", "VII"]
                for i, row in enumerate(st.session_state.post_rows):
                    r1, r2, r3 = st.columns([3, 2, 1])
                    row["name"] = r1.text_input("Nama pasien", value=row.get("name", ""), key=f"post_name_{i}")
                    row["pod"] = r2.selectbox("POD hari ini", POD_OPTIONS, index=POD_OPTIONS.index(row.get("pod","0")) if row.get("pod","0") in POD_OPTIONS else 0, key=f"post_pod_{i}")
                    if r3.button("🗑️", key=f"post_del_{i}"):
                        st.session_state.post_rows.pop(i)
                        st.rerun()

                st.markdown("<div class='small-note'>Tip: kalau pasien POST OP cuma 1, sistem otomatis bagi semua orang jadi 2 tim (POD n dan POD n+1) supaya tidak ada yang nganggur.</div></div>", unsafe_allow_html=True)

                # ---- PRE OP card ----
                st.markdown("<div class='card pre'><h3>🟣 PRE OP</h3>", unsafe_allow_html=True)
                st.caption("Isi nama pasien saja. Pembagian SOAP/RM/ERM/TSR dibuat otomatis dan fair.")

                if st.button("➕ Tambah Pre Op", key="btn_add_pre"):
                    st.session_state.pre_rows.append({"name": ""})

                for i, row in enumerate(st.session_state.pre_rows):
                    r1, r2 = st.columns([5, 1])
                    row["name"] = r1.text_input("Nama pasien", value=row.get("name", ""), key=f"pre_name_{i}")
                    if r2.button("🗑️", key=f"pre_del_{i}"):
                        st.session_state.pre_rows.pop(i)
                        st.rerun()

                st.markdown("</div>", unsafe_allow_html=True)

                # ---- IGD card ----
                st.markdown("<div class='card igd'><h3>🟢 IGD</h3>", unsafe_allow_html=True)
                st.caption("Isi nama pasien saja. Untuk IGD, TSR diganti ER.")

                if st.button("➕ Tambah IGD", key="btn_add_igd"):
                    st.session_state.igd_rows.append({"name": ""})

                for i, row in enumerate(st.session_state.igd_rows):
                    r1, r2 = st.columns([5, 1])
                    row["name"] = r1.text_input("Nama pasien", value=row.get("name", ""), key=f"igd_name_{i}")
                    if r2.button("🗑️", key=f"igd_del_{i}"):
                        st.session_state.igd_rows.pop(i)
                        st.rerun()

                st.markdown("</div>", unsafe_allow_html=True)

            with c2:
                default_erm = saved_payload.get("erm_manual","") if saved_payload else ""
                default_rev = saved_payload.get("review_manual","") if saved_payload else ""
                erm_manual = st.text_input("ERM (manual)", value=default_erm)
                review_manual = st.text_input("Review (manual)", value=default_rev)

                st.caption("Catatan: pairing A14/A15 dibuat otomatis dan konsisten per tanggal (acak tapi deterministik). A16 ikut masuk pembagian dan selalu ditaruh paling belakang.")
                st.caption("✨ Output WA akan muncul di bawah setelah Generate. Kamu tinggal copy-paste.")

            post_ops = [{"name": x.get("name","").strip(), "meta": f"POD {x.get('pod','0')}".strip()} for x in (st.session_state.get("post_rows") or []) if (x.get("name") or "").strip()]
            pre_ops = [{"name": x.get("name","").strip(), "meta": ""} for x in (st.session_state.get("pre_rows") or []) if (x.get("name") or "").strip()]
            igds = [{"name": x.get("name","").strip(), "meta": ""} for x in (st.session_state.get("igd_rows") or []) if (x.get("name") or "").strip()]

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
