import re
import csv
import io
import json
import random
import streamlit as st
from datetime import datetime, date as dt_date
from supabase import create_client

st.set_page_config(page_title="Jadwal Jaga Residen", layout="wide")

# Theme styling (Vibrant Violet/Pink/Green palette)
st.markdown("""
<style>
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

# ---------- Supabase execution helper ----------
def sb_exec(fn):
    try:
        return fn()
    except Exception as e:
        st.error("Supabase error:")
        st.code(str(e))
        st.stop()

# ---------- Optimized Supabase Query Caching ----------
@st.cache_data(ttl=300)
def fetch_roster_month(month: str):
    def run():
        res = sb.table("roster_days").select("*").eq("month", month).execute()
        return {r["date"]: r for r in res.data} if res.data else {}
    return sb_exec(run)

@st.cache_data(ttl=300)
def fetch_assignments_month(month: str):
    def run():
        res = sb.table("assignments").select("*").eq("month", month).execute()
        return {r["date"]: r for r in res.data} if res.data else {}
    return sb_exec(run)

@st.cache_data(ttl=600)
def fetch_month_exists(month: str) -> bool:
    def run():
        res = sb.table("rosters").select("month").eq("month", month).execute()
        return bool(res.data)
    return sb_exec(run)

# Helper to clear Streamlit cache after modification
def invalidate_caches():
    st.cache_data.clear()

# ---------- Global Configuration & Seniority Setup ----------
DEFAULT_CONFIG = {
    "cohorts": {
        "a12": {"label": "Angkatan 12", "active": True, "jaga_level": "Jaga 4"},
        "a13": {"label": "Angkatan 13", "active": True, "jaga_level": "Jaga 3"},
        "a14": {"label": "Angkatan 14", "active": True, "jaga_level": "Jaga 2"},
        "a15": {"label": "Angkatan 15", "active": True, "jaga_level": "Jaga 1"},
        "observers": {"label": "Observers (A16)", "active": True, "jaga_level": "Observers"}
    },
    "blacklist": [
        ["Ferrel", "Maman"]
    ]
}

@st.cache_data(ttl=600)
def fetch_global_config():
    def run():
        res = sb.table("assignments").select("payload").eq("month", "config").eq("date", "global").execute()
        if res.data and "payload" in res.data[0]:
            config = res.data[0]["payload"]
            # Merge missing properties if they don't exist
            if "cohorts" not in config:
                config["cohorts"] = DEFAULT_CONFIG["cohorts"]
            else:
                for k, v in DEFAULT_CONFIG["cohorts"].items():
                    if k not in config["cohorts"]:
                        config["cohorts"][k] = v
            if "blacklist" not in config:
                config["blacklist"] = DEFAULT_CONFIG["blacklist"]
            return config
        return DEFAULT_CONFIG
    return sb_exec(run)

def save_global_config(config_dict):
    def run():
        sb.table("assignments").upsert(
            {"month": "config", "date": "global", "payload": config_dict},
            on_conflict="month,date"
        ).execute()
    sb_exec(run)
    invalidate_caches()

# ---------- Supabase modification write helpers ----------
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

def upsert_assignment(month: str, date: str, payload: dict):
    def run():
        sb.table("assignments").upsert(
            {"month": month, "date": date, "payload": payload},
            on_conflict="month,date"
        ).execute()
    sb_exec(run)
    invalidate_caches()

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
    raw = uploaded_file.getvalue()
    text = raw.decode("utf-8", errors="replace")
    sample = text[:4096]

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
    except Exception:
        dialect = csv.excel

    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    fieldnames = []
    for fn in (reader.fieldnames or []):
        fn = (fn or "").strip().lstrip("\ufeff")
        fieldnames.append(fn)

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

# ---------- Assignment generation helpers ----------
DAY_ID = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]

def iso_to_dayname(iso_date: str) -> str:
    y, m, d = [int(x) for x in iso_date.split("-")]
    wd = dt_date(y, m, d).weekday()
    return DAY_ID[wd]

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
    seed = f"{iso_date}:{salt}"
    return random.Random(seed)

def shuffled(names: list[str], iso_date: str, salt: str):
    rng = seeded_rng(iso_date, salt)
    xs = [n.strip() for n in (names or []) if n and n.strip()]
    rng.shuffle(xs)
    return xs

# ---------- Proportional Fair Distribution Algorithm ----------
def distribute_cohort_to_patients(residents: list[str], patients: list[dict], iso_date: str, salt: str) -> list[list[str]]:
    """
    Distribute list of residents from a cohort to patients as evenly as possible.
    Returns a list of lists, where out[i] is the list of residents assigned to patients[i].
    """
    m = len(patients)
    n = len(residents)
    
    if m == 0 or n == 0:
        return [[] for _ in range(m)]
        
    # Shuffle residents deterministically per day and salt to make it fair but stable
    shuffled_res = shuffled(residents, iso_date, salt)
    assignments = [[] for _ in range(m)]
    
    if n >= m:
        # Each patient gets at least floor(n/m) residents, and the remainder are distributed
        base = n // m
        rem = n % m
        
        # Assign base residents to all patients
        res_idx = 0
        for i in range(m):
            assignments[i].extend(shuffled_res[res_idx : res_idx + base])
            res_idx += base
            
        # Distribute the remainder deterministically but fairly across patients
        rem_patient_indices = list(range(m))
        rng = seeded_rng(iso_date, salt + "_rem_patients")
        rng.shuffle(rem_patient_indices)
        
        for i in range(rem):
            patient_idx = rem_patient_indices[i]
            assignments[patient_idx].append(shuffled_res[res_idx])
            res_idx += 1
    else:
        # n < m
        # Each resident covers floor(m/n) or ceil(m/n) patients
        patient_order = list(range(m))
        rng = seeded_rng(iso_date, salt + "_patients")
        rng.shuffle(patient_order)
        
        for idx, patient_idx in enumerate(patient_order):
            res_idx = idx % n
            assignments[patient_idx].append(shuffled_res[res_idx])
            
    return assignments

# ---------- Helper to identify which cohort a resident belongs to ----------
def find_resident_cohort(res_name: str, roster: dict) -> str:
    for c in ["a12", "a13", "a14", "a15", "observers"]:
        if res_name in roster.get(c, []):
            return c
    return None

# ---------- Helper to sort names by cohort seniority ----------
def sort_by_cohort(names: list[str], roster: dict) -> list[str]:
    cohort_order = ["a12", "a13", "a14", "a15", "observers"]
    
    def get_sort_key(name):
        for idx, c in enumerate(cohort_order):
            if name in roster.get(c, []):
                return idx
        return len(cohort_order)
        
    return sorted(list(set(names)), key=get_sort_key)

# ---------- Role Assignment Logic based on seniority (Jaga Levels) ----------
def assign_patient_roles_pre(patient_residents: list[str], config: dict, roster: dict) -> tuple[list[str], list[str], list[str]]:
    soap = []
    rm_erm = []
    tsr = []
    
    for res in patient_residents:
        cohort = find_resident_cohort(res, roster)
        if not cohort:
            continue
        jaga_level = config["cohorts"].get(cohort, {}).get("jaga_level", "Jaga 1")
        
        if jaga_level in ["Jaga 1", "Observers"]:
            soap.append(res)
            rm_erm.append(res)
        elif jaga_level == "Jaga 2":
            rm_erm.append(res)
            tsr.append(res)
        elif jaga_level in ["Jaga 3", "Jaga 4"]:
            tsr.append(res)
            soap.append(res)
            
    # Guarantee representation in roles
    all_res = sort_by_cohort(patient_residents, roster)
    if all_res:
        if not soap:
            soap.append(all_res[0])
        if not rm_erm:
            rm_erm.append(all_res[-1])
        if not tsr:
            tsr.append(all_res[0])
            
    return soap, rm_erm, tsr

def assign_patient_roles_igd(patient_residents: list[str], config: dict, roster: dict) -> tuple[list[str], list[str], list[str]]:
    soap = []
    rm_erm = []
    er = []
    
    for res in patient_residents:
        cohort = find_resident_cohort(res, roster)
        if not cohort:
            continue
        jaga_level = config["cohorts"].get(cohort, {}).get("jaga_level", "Jaga 1")
        
        if jaga_level in ["Jaga 1", "Observers"]:
            soap.append(res)
            rm_erm.append(res)
        elif jaga_level == "Jaga 2":
            rm_erm.append(res)
            er.append(res)
        elif jaga_level in ["Jaga 3", "Jaga 4"]:
            er.append(res)
            soap.append(res)
            
    # Guarantee representation in roles
    all_res = sort_by_cohort(patient_residents, roster)
    if all_res:
        if not soap:
            soap.append(all_res[0])
        if not rm_erm:
            rm_erm.append(all_res[-1])
        if not er:
            er.append(all_res[0])
            
    return soap, rm_erm, er

# ---------- Blacklist Resolvers ----------
def enforce_blacklist_two_teams(t1: list[str], t2: list[str], config: dict, roster: dict) -> tuple[list[str], list[str]]:
    blacklist = config.get("blacklist", [])
    if not blacklist:
        return t1, t2
        
    t1_set = set(t1)
    t2_set = set(t2)
    
    for _ in range(10):
        changed = False
        for pair in blacklist:
            if len(pair) >= 2:
                p1, p2 = pair[0], pair[1]
                # Check team 1
                if p1 in t1_set and p2 in t1_set:
                    cohort = find_resident_cohort(p2, roster)
                    if cohort:
                        candidates = [x for x in t2 if find_resident_cohort(x, roster) == cohort and x != p1 and x != p2]
                        if candidates:
                            cand = candidates[0]
                            t1 = [cand if x == p2 else x for x in t1]
                            t2 = [p2 if x == cand else x for x in t2]
                            t1_set = set(t1)
                            t2_set = set(t2)
                            changed = True
                            break
                # Check team 2
                if p1 in t2_set and p2 in t2_set:
                    cohort = find_resident_cohort(p2, roster)
                    if cohort:
                        candidates = [x for x in t1 if find_resident_cohort(x, roster) == cohort and x != p1 and x != p2]
                        if candidates:
                            cand = candidates[0]
                            t2 = [cand if x == p2 else x for x in t2]
                            t1 = [p2 if x == cand else x for x in t1]
                            t1_set = set(t1)
                            t2_set = set(t2)
                            changed = True
                            break
        if not changed:
            break
            
    return t1, t2

def resolve_blacklist_post_op(patient_teams: list[list[str]], config: dict, roster: dict) -> list[list[str]]:
    blacklist = config.get("blacklist", [])
    if not blacklist or len(patient_teams) <= 1:
        return patient_teams
        
    for _ in range(50):
        violation_found = False
        for i, team in enumerate(patient_teams):
            team_set = set(team)
            for pair in blacklist:
                if len(pair) >= 2:
                    p1, p2 = pair[0], pair[1]
                    if p1 in team_set and p2 in team_set:
                        cohort = find_resident_cohort(p2, roster)
                        if not cohort:
                            continue
                        
                        swapped = False
                        for j, other_team in enumerate(patient_teams):
                            if i == j:
                                continue
                            other_candidates = [x for x in other_team if find_resident_cohort(x, roster) == cohort]
                            for cand in other_candidates:
                                if cand != p1 and cand != p2:
                                    temp_other = [p2 if x == cand else x for x in other_team]
                                    other_safe = True
                                    for bp in blacklist:
                                        if bp[0] in temp_other and bp[1] in temp_other:
                                            other_safe = False
                                            break
                                    if other_safe:
                                        patient_teams[i] = [cand if x == p2 else x for x in team]
                                        patient_teams[j] = temp_other
                                        violation_found = True
                                        swapped = True
                                        break
                            if swapped:
                                break
                        if swapped:
                            break
            if violation_found:
                break
        if not violation_found:
            break
            
    return patient_teams

def resolve_blacklist_pre_op(patient_assignments: list[dict], config: dict, roster: dict) -> list[dict]:
    blacklist = config.get("blacklist", [])
    if not blacklist:
        return patient_assignments
        
    for _ in range(50):
        violation_found = False
        for i, pa in enumerate(patient_assignments):
            for role in ["soap", "rm_erm", "tsr"]:
                role_names = pa[role]
                for pair in blacklist:
                    if len(pair) >= 2:
                        p1, p2 = pair[0], pair[1]
                        if p1 in role_names and p2 in role_names:
                            cohort = find_resident_cohort(p2, roster)
                            if not cohort:
                                continue
                                
                            swapped = False
                            for j, other_pa in enumerate(patient_assignments):
                                if i == j:
                                    continue
                                other_candidates = [x for x in other_pa[role] if find_resident_cohort(x, roster) == cohort]
                                for cand in other_candidates:
                                    if cand != p1 and cand != p2:
                                        temp_other = [p2 if x == cand else x for x in other_pa[role]]
                                        other_safe = True
                                        for bp in blacklist:
                                            if bp[0] in temp_other and bp[1] in temp_other:
                                                other_safe = False
                                                break
                                        
                                        if other_safe:
                                            pa[role] = [cand if x == p2 else x for x in pa[role]]
                                            other_pa[role] = temp_other
                                            violation_found = True
                                            swapped = True
                                            break
                                if swapped:
                                    break
                            if swapped:
                                break
                if violation_found:
                    break
            if violation_found:
                break
        if not violation_found:
            break
            
    return patient_assignments

def resolve_blacklist_igd(patient_assignments: list[dict], config: dict, roster: dict) -> list[dict]:
    blacklist = config.get("blacklist", [])
    if not blacklist:
        return patient_assignments
        
    for _ in range(50):
        violation_found = False
        for i, pa in enumerate(patient_assignments):
            for role in ["soap", "rm_erm", "er"]:
                role_names = pa[role]
                for pair in blacklist:
                    if len(pair) >= 2:
                        p1, p2 = pair[0], pair[1]
                        if p1 in role_names and p2 in role_names:
                            cohort = find_resident_cohort(p2, roster)
                            if not cohort:
                                continue
                                
                            swapped = False
                            for j, other_pa in enumerate(patient_assignments):
                                if i == j:
                                    continue
                                other_candidates = [x for x in other_pa[role] if find_resident_cohort(x, roster) == cohort]
                                for cand in other_candidates:
                                    if cand != p1 and cand != p2:
                                        temp_other = [p2 if x == cand else x for x in other_pa[role]]
                                        other_safe = True
                                        for bp in blacklist:
                                            if bp[0] in temp_other and bp[1] in temp_other:
                                                other_safe = False
                                                break
                                        
                                        if other_safe:
                                            pa[role] = [cand if x == p2 else x for x in pa[role]]
                                            other_pa[role] = temp_other
                                            violation_found = True
                                            swapped = True
                                            break
                                if swapped:
                                    break
                            if swapped:
                                break
                if violation_found:
                    break
            if violation_found:
                break
        if not violation_found:
            break
            
    return patient_assignments

# ---------- Core Assignment Engine ----------
def build_assignment(roster: dict, iso_date: str, post_ops: list, pre_ops: list, igds: list,
                     erm_manual: str, review_manual: str, config: dict, pilot_info: dict, copilot_info: dict):
    
    # Roster has cohorts: a12, a13, a14, a15, observers
    a12 = roster.get("a12") or []
    a13 = roster.get("a13") or []
    a14 = roster.get("a14") or []
    a15 = roster.get("a15") or []
    observers = roster.get("observers") or []
    
    daily_roster = {
        "a12": [x.strip() for x in a12 if x.strip()],
        "a13": [x.strip() for x in a13 if x.strip()],
        "a14": [x.strip() for x in a14 if x.strip()],
        "a15": [x.strip() for x in a15 if x.strip()],
        "observers": [x.strip() for x in observers if x.strip()]
    }
    
    active_cohorts = []
    for c, info in config["cohorts"].items():
        if info.get("active", True):
            active_cohorts.append(c)
            
    # Filter daily_roster to only active cohorts
    filtered_roster = {c: daily_roster.get(c, []) for c in active_cohorts}
    
    # 1. Build Post-Op assignments
    post_op_assignments = []
    if post_ops:
        m = len(post_ops)
        if m == 1:
            # Split all people into two teams (POD n and POD n+1)
            t1 = []
            t2 = []
            for c in active_cohorts:
                pool = shuffled(filtered_roster.get(c, []), iso_date, f"post:split:{c}")
                n_pool = len(pool)
                if n_pool > 0:
                    half = (n_pool + 1) // 2
                    t1.extend(pool[:half])
                    t2.extend(pool[half:])
            
            t1, t2 = enforce_blacklist_two_teams(t1, t2, config, daily_roster)
            team1 = sort_by_cohort(t1, daily_roster)
            team2 = sort_by_cohort(t2, daily_roster)
            
            labels = normalize_pod_label(post_ops[0].get("meta", "")) or ("POD I", "POD II")
            post_op_assignments.append({
                "name": post_ops[0]["name"],
                "pod_lines": [
                    {"label": labels[0], "team": team1},
                    {"label": labels[1], "team": team2},
                ]
            })
        else:
            # Distribute cohorts proportionally
            cohort_distributions = {}
            for c in active_cohorts:
                pool = filtered_roster.get(c, [])
                cohort_distributions[c] = distribute_cohort_to_patients(pool, post_ops, iso_date, f"post:{c}")
                
            # Combine into teams per patient
            patient_teams = [[] for _ in range(m)]
            for i in range(m):
                for c in active_cohorts:
                    patient_teams[i].extend(cohort_distributions[c][i])
                    
            patient_teams = resolve_blacklist_post_op(patient_teams, config, daily_roster)
            
            for i, p in enumerate(post_ops):
                labels = normalize_pod_label(p.get("meta", "")) or ("POD I", "POD II")
                team = sort_by_cohort(patient_teams[i], daily_roster)
                post_op_assignments.append({
                    "name": p["name"],
                    "pod_lines": [
                        {"label": labels[0], "team": team},
                        {"label": labels[1], "team": team},
                    ]
                })

    # 2. Build Pre-Op assignments
    pre_op_assignments = []
    if pre_ops:
        m = len(pre_ops)
        cohort_distributions = {}
        for c in active_cohorts:
            pool = filtered_roster.get(c, [])
            cohort_distributions[c] = distribute_cohort_to_patients(pool, pre_ops, iso_date, f"pre:{c}")
            
        for i, p in enumerate(pre_ops):
            patient_residents = []
            for c in active_cohorts:
                patient_residents.extend(cohort_distributions[c][i])
                
            soap, rm_erm, tsr = assign_patient_roles_pre(patient_residents, config, daily_roster)
            
            soap = sort_by_cohort(soap, daily_roster)
            rm_erm = sort_by_cohort(rm_erm, daily_roster)
            tsr = sort_by_cohort(tsr, daily_roster)
            
            pre_op_assignments.append({
                "name": p["name"],
                "soap": soap,
                "rm_erm": rm_erm,
                "tsr": tsr
            })
            
        pre_op_assignments = resolve_blacklist_pre_op(pre_op_assignments, config, daily_roster)

    # 3. Build IGD assignments
    igd_assignments = []
    if igds:
        m = len(igds)
        cohort_distributions = {}
        for c in active_cohorts:
            pool = filtered_roster.get(c, [])
            cohort_distributions[c] = distribute_cohort_to_patients(pool, igds, iso_date, f"igd:{c}")
            
        for i, p in enumerate(igds):
            patient_residents = []
            for c in active_cohorts:
                patient_residents.extend(cohort_distributions[c][i])
                
            soap, rm_erm, er = assign_patient_roles_igd(patient_residents, config, daily_roster)
            
            soap = sort_by_cohort(soap, daily_roster)
            rm_erm = sort_by_cohort(rm_erm, daily_roster)
            er = sort_by_cohort(er, daily_roster)
            
            igd_assignments.append({
                "name": p["name"],
                "soap": soap,
                "rm_erm": rm_erm,
                "er": er
            })
            
        igd_assignments = resolve_blacklist_igd(igd_assignments, config, daily_roster)

    out = {
        "date": iso_date,
        "day_name": iso_to_dayname(iso_date),
        "pilot": pilot_info.get("name", ""),
        "pilot_cohort": pilot_info.get("cohort", ""),
        "copilot": copilot_info.get("name", ""),
        "copilot_cohort": copilot_info.get("cohort", ""),
        "erm_manual": erm_manual or "",
        "review_manual": review_manual or "",
        "post_op": post_op_assignments,
        "pre_op": pre_op_assignments,
        "igd": igd_assignments,
    }
    
    return out

# ---------- WA Formatter ----------
def format_wa_text(assign: dict) -> str:
    day = assign["day_name"]
    iso = assign["date"]
    dd, mm, yyyy = iso.split("-")[2], iso.split("-")[1], iso.split("-")[0]
    header = f"Pembagian tugas jaga {day}, {dd}/{mm}/{yyyy}\n\n"
    
    pilot_lbl = f" ({assign['pilot_cohort'].upper()})" if assign.get("pilot_cohort") else ""
    copilot_lbl = f" ({assign['copilot_cohort'].upper()})" if assign.get("copilot_cohort") else ""
    
    header += f"Pilot : {assign.get('pilot','')}{pilot_lbl}\n"
    header += f"Co Pilot : {assign.get('copilot','')}{copilot_lbl}\n\n"

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

# ---------- Streamlit Interface Layout ----------
st.markdown(
    "<div class='hero'>"
    "<div class='hero-title'>🌸 Jadwal Jaga Residen</div>"
    "<div class='hero-sub'>Penyesuaian angkatan dan penugasan Pilot & Co-Pilot dinamis. Roster & pembagian loading instan!</div>"
    "</div>",
    unsafe_allow_html=True
)

# Date Pickers
col1, col2 = st.columns(2)
with col1:
    picked_month = st.text_input("Bulan (YYYY-MM)", value=datetime.now().strftime("%Y-%m"))
with col2:
    picked_date = st.date_input("Tanggal", value=datetime.now()).strftime("%Y-%m-%d")

# Fetch cache-optimized roster check
exists = fetch_month_exists(picked_month)
st.caption("Status bulan: " + ("✅ tersedia di database" if exists else "⚠️ belum ada roster"))

# Retrieve global configuration (loaded once & cached)
global_config = fetch_global_config()

tab_use, tab_config, tab_admin = st.tabs(["Pakai (cek roster)", "⚙️ Konfigurasi Angkatan", "Admin (Import CSV)"])

with tab_use:
    if not exists:
        st.warning("Roster bulan ini belum ada. Import dulu di tab Admin.")
    else:
        # Fetch roster and assignment for this date from the cached month data (instant!)
        roster_data = fetch_roster_month(picked_month)
        assignment_data = fetch_assignments_month(picked_month)
        
        r = roster_data.get(picked_date)
        
        if not r:
            st.info("Tanggal ini belum ada roster. Tambah roster di tab Admin atau pilih tanggal lain.")
        else:
            # Active cohorts according to global config
            active_keys = [k for k, v in global_config["cohorts"].items() if v.get("active", True)]
            
            # 1. Roster Viewer and Editable Overrides
            with st.expander("📋 Tim Jaga Hari Ini (Roster)", expanded=False):
                st.write("Sesuaikan list residen jaga khusus untuk hari ini (jika ada yang sakit / absen). Tulis nama dipisahkan tanda koma.")
                
                # We store editable list in session state so user edits persist on rerun
                override_roster = {}
                for key in ["a12", "a13", "a14", "a15", "observers"]:
                    label = global_config["cohorts"].get(key, {}).get("label", key.upper())
                    is_active = global_config["cohorts"].get(key, {}).get("active", True)
                    
                    if is_active:
                        default_val = ", ".join(r.get(key, []))
                        # Session state key for roster edit
                        ss_key = f"roster_edit_{picked_date}_{key}"
                        if ss_key not in st.session_state:
                            st.session_state[ss_key] = default_val
                            
                        override_val = st.text_input(f"{label} ({'Active' if is_active else 'Inactive'})", value=st.session_state[ss_key], key=f"ti_{ss_key}")
                        st.session_state[ss_key] = override_val
                        override_roster[key] = [x.strip() for x in override_val.split(",") if x.strip()]
                    else:
                        override_roster[key] = []
            
            # Get roster to use for assignment (either default or edited overrides)
            roster_to_use = {
                "date": picked_date,
                "a12": override_roster.get("a12", r.get("a12", [])),
                "a13": override_roster.get("a13", r.get("a13", [])),
                "a14": override_roster.get("a14", r.get("a14", [])),
                "a15": override_roster.get("a15", r.get("a15", [])),
                "observers": override_roster.get("observers", r.get("observers", []))
            }
            
            # Combine all available people in active cohorts for dropdowns
            all_available_res = []
            cohort_of_res = {}
            for c in active_keys:
                names = roster_to_use.get(c, [])
                for name in names:
                    if name:
                        all_available_res.append(name)
                        cohort_of_res[name] = c
            all_available_res = sorted(list(set(all_available_res)))
            
            st.markdown("---")
            st.subheader("Buat Pembagian Tugas Jaga")

            saved = assignment_data.get(picked_date)
            saved_payload = saved["payload"] if saved and isinstance(saved.get("payload"), dict) else None

            # Load Pilot/Co-Pilot preferences
            default_pilot = saved_payload.get("pilot", r.get("pilot", "")) if saved_payload else r.get("pilot", "")
            default_copilot = saved_payload.get("copilot", r.get("copilot", "")) if saved_payload else r.get("copilot", "")
            
            # If the saved pilot/copilot names are in the roster list, find their cohort
            default_pilot_c = saved_payload.get("pilot_cohort", find_resident_cohort(default_pilot, roster_to_use) or "a13") if saved_payload else (find_resident_cohort(default_pilot, roster_to_use) or "a13")
            default_copilot_c = saved_payload.get("copilot_cohort", find_resident_cohort(default_copilot, roster_to_use) or "a14") if saved_payload else (find_resident_cohort(default_copilot, roster_to_use) or "a14")
            
            # Pilot / Co-Pilot Dropdowns
            st.markdown("##### ✈️ Koordinator Jaga (Pilot & Co-Pilot)")
            pc1, pc2, pc3, pc4 = st.columns(4)
            
            active_labels = {k: v["label"] for k, v in global_config["cohorts"].items() if v.get("active", True)}
            active_keys_ordered = [k for k in ["a12", "a13", "a14", "a15", "observers"] if k in active_labels]
            
            with pc1:
                # Pilot Cohort Select
                pilot_idx = active_keys_ordered.index(default_pilot_c) if default_pilot_c in active_keys_ordered else 0
                pilot_cohort = st.selectbox("Pilot Angkatan", options=active_keys_ordered, format_func=lambda x: active_labels[x], index=pilot_idx)
            
            with pc2:
                # Pilot Name Select
                pilot_names = roster_to_use.get(pilot_cohort, [])
                # Add "Manual / No One" option
                options_p = ["-- Pilih --"] + pilot_names + (["Manual: " + default_pilot] if default_pilot and default_pilot not in pilot_names else []) + ["Ketik Manual..."]
                
                # Determine index
                if default_pilot in pilot_names:
                    p_idx = options_p.index(default_pilot)
                elif default_pilot and ("Manual: " + default_pilot) in options_p:
                    p_idx = options_p.index("Manual: " + default_pilot)
                else:
                    p_idx = 0
                    
                selected_pilot_opt = st.selectbox("Nama Pilot", options=options_p, index=p_idx)
                
                if selected_pilot_opt == "Ketik Manual...":
                    pilot_name = st.text_input("Ketik Nama Pilot Manual")
                elif selected_pilot_opt.startswith("Manual: "):
                    pilot_name = selected_pilot_opt.replace("Manual: ", "")
                elif selected_pilot_opt == "-- Pilih --":
                    pilot_name = ""
                else:
                    pilot_name = selected_pilot_opt

            with pc3:
                # Co-Pilot Cohort Select
                copilot_idx = active_keys_ordered.index(default_copilot_c) if default_copilot_c in active_keys_ordered else 0
                copilot_cohort = st.selectbox("Co-Pilot Angkatan", options=active_keys_ordered, format_func=lambda x: active_labels[x], index=copilot_idx)
            
            with pc4:
                # Co-Pilot Name Select
                copilot_names = roster_to_use.get(copilot_cohort, [])
                options_cp = ["-- Pilih --"] + copilot_names + (["Manual: " + default_copilot] if default_copilot and default_copilot not in copilot_names else []) + ["Ketik Manual..."]
                
                # Determine index
                if default_copilot in copilot_names:
                    cp_idx = options_cp.index(default_copilot)
                elif default_copilot and ("Manual: " + default_copilot) in options_cp:
                    cp_idx = options_cp.index("Manual: " + default_copilot)
                else:
                    cp_idx = 0
                    
                selected_copilot_opt = st.selectbox("Nama Co-Pilot", options=options_cp, index=cp_idx)
                
                if selected_copilot_opt == "-- Pilih --":
                    copilot_name = ""
                elif selected_copilot_opt == "Ketik Manual...":
                    copilot_name = st.text_input("Ketik Nama Co-Pilot Manual")
                elif selected_copilot_opt.startswith("Manual: "):
                    copilot_name = selected_copilot_opt.replace("Manual: ", "")
                else:
                    copilot_name = selected_copilot_opt

            st.markdown("---")

            c1, c2 = st.columns(2)
            with c1:
                # Initialize session state rows
                if "post_rows" not in st.session_state:
                    st.session_state.post_rows = []
                if "pre_rows" not in st.session_state:
                    st.session_state.pre_rows = []
                if "igd_rows" not in st.session_state:
                    st.session_state.igd_rows = []

                # Preload UI inputs once from Supabase assignment if it exists
                # Unique key incorporates dates to reload when date changes
                preload_key = f"_ui_preloaded_{picked_date}"
                if saved_payload and not st.session_state.get(preload_key, False):
                    st.session_state.post_rows = [
                        {"name": p.get("name", ""), "pod": (p.get("pod_lines", [{}])[0].get("label", "POD 0").replace("POD ", "").strip() or "0")}
                        for p in (saved_payload.get("post_op") or [])
                    ]
                    st.session_state.pre_rows = [{"name": p.get("name", "")} for p in (saved_payload.get("pre_op") or [])]
                    st.session_state.igd_rows = [{"name": p.get("name", "")} for p in (saved_payload.get("igd") or [])]
                    st.session_state[preload_key] = True
                elif not saved_payload and not st.session_state.get(preload_key, False):
                    # Reset UI if moving to an unassigned date
                    st.session_state.post_rows = []
                    st.session_state.pre_rows = []
                    st.session_state.igd_rows = []
                    st.session_state[preload_key] = True

                # ---- POST OP Card ----
                st.markdown("<div class='card post'><h3>🔴 POST OP</h3>", unsafe_allow_html=True)
                st.caption("Klik tambah pasien. POD cukup pilih angka/romawi di dropdown.")

                if st.button("➕ Tambah Post Op", key="btn_add_post"):
                    st.session_state.post_rows.append({"name": "", "pod": "0"})

                POD_OPTIONS = ["0", "I", "II", "III", "IV", "V", "VI", "VII"]
                for i, row in enumerate(st.session_state.post_rows):
                    r1, r2, r3 = st.columns([3, 2, 1])
                    row["name"] = r1.text_input("Nama pasien", value=row.get("name", ""), key=f"post_name_{picked_date}_{i}")
                    row["pod"] = r2.selectbox("POD hari ini", POD_OPTIONS, index=POD_OPTIONS.index(row.get("pod","0")) if row.get("pod","0") in POD_OPTIONS else 0, key=f"post_pod_{picked_date}_{i}")
                    if r3.button("🗑️", key=f"post_del_{picked_date}_{i}"):
                        st.session_state.post_rows.pop(i)
                        st.rerun()

                st.markdown("<div class='small-note'>Tip: kalau pasien POST OP cuma 1, sistem otomatis bagi semua orang jadi 2 tim (POD n dan POD n+1) supaya tidak ada yang nganggur.</div></div>", unsafe_allow_html=True)

                # ---- PRE OP Card ----
                st.markdown("<div class='card pre'><h3>🟣 PRE OP</h3>", unsafe_allow_html=True)
                st.caption("Isi nama pasien saja. Pembagian SOAP/RM/ERM/TSR dibuat otomatis, fair & proporsional.")

                if st.button("➕ Tambah Pre Op", key="btn_add_pre"):
                    st.session_state.pre_rows.append({"name": ""})

                for i, row in enumerate(st.session_state.pre_rows):
                    r1, r2 = st.columns([5, 1])
                    row["name"] = r1.text_input("Nama pasien", value=row.get("name", ""), key=f"pre_name_{picked_date}_{i}")
                    if r2.button("🗑️", key=f"pre_del_{picked_date}_{i}"):
                        st.session_state.pre_rows.pop(i)
                        st.rerun()

                st.markdown("</div>", unsafe_allow_html=True)

                # ---- IGD Card ----
                st.markdown("<div class='card igd'><h3>🟢 IGD</h3>", unsafe_allow_html=True)
                st.caption("Isi nama pasien saja. Untuk IGD, TSR diganti ER.")

                if st.button("➕ Tambah IGD", key="btn_add_igd"):
                    st.session_state.igd_rows.append({"name": ""})

                for i, row in enumerate(st.session_state.igd_rows):
                    r1, r2 = st.columns([5, 1])
                    row["name"] = r1.text_input("Nama pasien", value=row.get("name", ""), key=f"igd_name_{picked_date}_{i}")
                    if r2.button("🗑️", key=f"igd_del_{picked_date}_{i}"):
                        st.session_state.igd_rows.pop(i)
                        st.rerun()

                st.markdown("</div>", unsafe_allow_html=True)

            with c2:
                default_erm = saved_payload.get("erm_manual","") if saved_payload else ""
                default_rev = saved_payload.get("review_manual","") if saved_payload else ""
                erm_manual = st.text_input("ERM (manual)", value=default_erm, key=f"erm_{picked_date}")
                review_manual = st.text_input("Review (manual)", value=default_rev, key=f"rev_{picked_date}")

                st.caption("Catatan: Pembagian tugas adil & proporsional dihitung otomatis secara terpisah untuk Pre-Op & Post-Op. Angkatan tidak aktif diabaikan.")
                st.caption("✨ Output WA akan muncul di bawah setelah Generate. Kamu tinggal copy-paste.")

            post_ops = [{"name": x.get("name","").strip(), "meta": f"POD {x.get('pod','0')}".strip()} for x in (st.session_state.get("post_rows") or []) if (x.get("name") or "").strip()]
            pre_ops = [{"name": x.get("name","").strip(), "meta": ""} for x in (st.session_state.get("pre_rows") or []) if (x.get("name") or "").strip()]
            igds = [{"name": x.get("name","").strip(), "meta": ""} for x in (st.session_state.get("igd_rows") or []) if (x.get("name") or "").strip()]

            # Generate shift distribution
            if st.button("Generate Pembagian"):
                pilot_info = {"cohort": pilot_cohort, "name": pilot_name}
                copilot_info = {"cohort": copilot_cohort, "name": copilot_name}
                
                assign = build_assignment(
                    roster=roster_to_use,
                    iso_date=roster_to_use["date"],
                    post_ops=post_ops,
                    pre_ops=pre_ops,
                    igds=igds,
                    erm_manual=erm_manual,
                    review_manual=review_manual,
                    config=global_config,
                    pilot_info=pilot_info,
                    copilot_info=copilot_info
                )
                wa = format_wa_text(assign)
                upsert_assignment(picked_month, picked_date, assign)

                st.success("✅ Pembagian berhasil dibuat & disimpan ke database.")
                st.text_area("Output WA (copy-paste)", value=wa, height=420)

            # Display saved assignment if it exists (but not generated yet this run)
            if saved_payload:
                st.info("Ada pembagian tugas tersimpan untuk tanggal ini. Berikut adalah teks tersimpan:")
                wa_saved = format_wa_text(saved_payload)
                st.text_area("Teks Jaga Tersimpan", value=wa_saved, height=350)

# ---------- Tab Configuration (Seniority & Active Cohorts) ----------
with tab_config:
    st.subheader("⚙️ Konfigurasi Angkatan & Tingkat Jaga (Senioritas)")
    st.write("Ubah status aktif angkatan dan pemetaan tingkat jaga (Jaga 1 - Jaga 4). Pengaturan ini akan tersimpan secara global di database.")
    
    # Render table config input
    new_cohorts_config = {}
    
    # 5 standard cohorts
    standard_keys = ["a12", "a13", "a14", "a15", "observers"]
    
    for key in standard_keys:
        info = global_config["cohorts"].get(key, DEFAULT_CONFIG["cohorts"][key])
        
        st.markdown(f"##### **{info['label']}**")
        c_col1, c_col2 = st.columns(2)
        
        with c_col1:
            active = st.checkbox("Aktif dalam Pembagian", value=info.get("active", True), key=f"config_active_{key}")
        with c_col2:
            levels = ["Jaga 1", "Jaga 2", "Jaga 3", "Jaga 4", "Observers"]
            level_idx = levels.index(info.get("jaga_level", "Jaga 1")) if info.get("jaga_level", "Jaga 1") in levels else 0
            jaga_level = st.selectbox("Level Penugasan", options=levels, index=level_idx, key=f"config_level_{key}", help="Jaga 1 & Observers -> SOAP/RM. Jaga 2 -> RM/TSR. Jaga 3/4 -> TSR/Supervisi.")
            
        new_cohorts_config[key] = {
            "label": info["label"],
            "active": active,
            "jaga_level": jaga_level
        }
        
    st.markdown("---")
    st.subheader("🚫 Blacklist Pairing (Anti-Kombinasi)")
    st.write("Pasangan residen di bawah ini tidak akan pernah ditempatkan dalam satu tim atau role line yang sama.")
    
    # Display current blacklist
    blacklist_list = list(global_config.get("blacklist", DEFAULT_CONFIG["blacklist"]))
    
    # Simple form to edit blacklist
    st.write("**Daftar Blacklist Saat Ini:**")
    new_blacklist = []
    
    for i, pair in enumerate(blacklist_list):
        if len(pair) >= 2:
            b_col1, b_col2, b_col3 = st.columns([3, 3, 1])
            p1 = b_col1.text_input(f"Orang 1 (Pasangan {i+1})", value=pair[0], key=f"bl_p1_{i}")
            p2 = b_col2.text_input(f"Orang 2 (Pasangan {i+1})", value=pair[1], key=f"bl_p2_{i}")
            remove = b_col3.checkbox("Hapus", key=f"bl_remove_{i}")
            if not remove and p1.strip() and p2.strip():
                new_blacklist.append([p1.strip(), p2.strip()])
                
    # Add new entry
    st.write("➕ Tambah Pasangan Blacklist Baru:")
    add_col1, add_col2 = st.columns(2)
    new_p1 = add_col1.text_input("Nama Orang 1 (Baru)")
    new_p2 = add_col2.text_input("Nama Orang 2 (Baru)")
    
    if new_p1.strip() and new_p2.strip():
        new_blacklist.append([new_p1.strip(), new_p2.strip()])
        
    if st.button("Simpan Konfigurasi Secara Global"):
        updated_config = {
            "cohorts": new_cohorts_config,
            "blacklist": new_blacklist
        }
        save_global_config(updated_config)
        st.success("Konfigurasi global telah disimpan dan diterapkan ke pembagian!")
        st.rerun()

# ---------- Tab Admin (CSV Import) ----------
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

            # Invalidate Streamlit cache immediately
            invalidate_caches()

            st.success(f"✅ Import sukses: {len(rows)} tanggal terisi untuk bulan: {', '.join(months_in_csv)}")
            st.info("Coba cek tab Pakai untuk tanggal tertentu.")
