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

# ---------- pairing blacklist ----------
# Hard constraint: these two must NEVER be in the same team/role line.
BLACKLIST_PAIRS = {
    ("Ferrel", "Maman"),
    ("Maman", "Ferrel"),
}

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
    """
    Surgical rewrite of the ASSIGNMENT ENGINE only.
    Goals:
    - A16 is NOT an observer section; A16 participates and must appear fairly.
    - Fairness is separated per section: POST OP vs PRE OP vs IGD.
    - Multi-patient POST OP: each patient gets a stable team; teams are disjoint by cohort splits (so no identical teams).
    - PRE/IGD: first distribute people per cohort across patients, then assign SOAP / RM-ERM / TSR(ER) using the per-patient buckets.
      This prevents "A15/A16 only in RM/ERM" and keeps cohort order in output.
    - Output order within each role line is ALWAYS A12 → A13 → A14 → A15 → A16.
    """

    # Cohort pools (from roster)
    a12 = roster.get("a12") or []
    a13 = roster.get("a13") or []
    a14 = roster.get("a14") or []
    a15 = roster.get("a15") or []
    a16 = roster.get("observers") or []  # A16 participates officially

    # Deterministic shuffle per date
    a12_s = shuffled(a12, iso_date, "rot:a12")
    a13_s = shuffled(a13, iso_date, "rot:a13")
    a14_s = shuffled(a14, iso_date, "rot:a14")
    a15_s = shuffled(a15, iso_date, "rot:a15")
    a16_s = shuffled(a16, iso_date, "rot:a16")

    def cohort_order(team: list[str]) -> list[str]:
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

    # ---- blacklist enforcement ----
    def violates_blacklist(team: list[str]) -> bool:
        s = set(team or [])
        for a, b in BLACKLIST_PAIRS:
            if a in s and b in s:
                return True
        return False

    def move_one_blacklisted(team_from: list[str], team_to: list[str]) -> tuple[list[str], list[str]]:
        """
        If a blacklisted pair exists in team_from, move one member to team_to.
        Keeps ordering later via cohort_order().
        """
        s = set(team_from or [])
        for a, b in BLACKLIST_PAIRS:
            if a in s and b in s:
                # move b first (arbitrary but deterministic)
                if b in team_from:
                    team_from = [x for x in team_from if x != b]
                    team_to = team_to + [b]
                    return (team_from, team_to)
                if a in team_from:
                    team_from = [x for x in team_from if x != a]
                    team_to = team_to + [a]
                    return (team_from, team_to)
        return (team_from, team_to)

    def enforce_blacklist_two_teams(t1: list[str], t2: list[str]) -> tuple[list[str], list[str]]:
        """
        Ensure no team contains a blacklisted pair by moving one person across teams.
        """
        # Try a few passes (small problem size)
        for _ in range(4):
            if violates_blacklist(t1):
                t1, t2 = move_one_blacklisted(t1, t2)
            if violates_blacklist(t2):
                t2, t1 = move_one_blacklisted(t2, t1)
        return (t1, t2)

    def enforce_blacklist_many(teams: list[list[str]]) -> list[list[str]]:
        """
        Ensure blacklisted pairs are not in the same team across a list of teams.
        Best-effort: swap within the same cohort pool by moving one member to the next team.
        """
        if not teams:
            return teams
        for _ in range(6):
            changed = False
            for i in range(len(teams)):
                if violates_blacklist(teams[i]):
                    j = (i + 1) % len(teams)
                    a, b = None, None
                    s = set(teams[i])
                    for x, y in BLACKLIST_PAIRS:
                        if x in s and y in s:
                            a, b = x, y
                            break
                    # move y to next team
                    if a and b:
                        teams[i] = [z for z in teams[i] if z != b]
                        teams[j] = teams[j] + [b]
                        changed = True
            if not changed:
                break
        return teams

    # Pairing for A14/A15 (stable per date)
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

    def split_even_pairs(pairs: list[list[str]], n_groups: int, salt: str):
        """Split a list of pairs evenly into n_groups, deterministically."""
        if n_groups <= 0:
            return []
        # deterministic shuffle of pair "keys"
        keys = list(range(len(pairs)))
        rng = seeded_rng(iso_date, f"{salt}:pairs")
        rng.shuffle(keys)
        groups = [[] for _ in range(n_groups)]
        for i, k in enumerate(keys):
            groups[i % n_groups].extend([x for x in (pairs[k] or []) if x])
        return groups

    def split_even_people(xs: list[str], n_groups: int, salt: str):
        if n_groups <= 0:
            return []
        sh = shuffled(xs, iso_date, f"{salt}:people")
        groups = [[] for _ in range(n_groups)]
        for i, x in enumerate(sh):
            groups[i % n_groups].append(x)
        return groups

    def split_all_people_into_two_teams() -> tuple[list[str], list[str]]:
        t1 = []
        t2 = []
        for cohort in [a12_s, a13_s, a14_s, a15_s, a16_s]:
            n = len(cohort)
            if n == 0:
                continue
            half = (n + 1) // 2
            t1 += cohort[:half]
            t2 += cohort[half:]
        t1o = cohort_order(t1)
        t2o = cohort_order(t2)
        t1o, t2o = enforce_blacklist_two_teams(t1o, t2o)
        return (cohort_order(t1o), cohort_order(t2o))

    # ---- Role assignment from per-patient buckets ----
    def assign_roles_from_buckets(buckets: dict, mode: str):
        """
        Strict PRE/IGD fairness engine.

        Rules:
        - For EVERY patient: SOAP, RM/ERM, TSR(ER) must EACH contain
          at least 1 person from every cohort (A12–A16) IF available.
        - RM/ERM is heavier, so extra people go there.
        - No role is allowed to be empty if any people exist.
        """

        cohort_keys = ["a12", "a13", "a14", "a15", "a16"]

        # Clone buckets safely
        local = {k: list(buckets.get(k) or []) for k in cohort_keys}

        soap = []
        rm = []
        third = []

        # STEP 1 — guarantee representation per cohort per role
        for k in cohort_keys:
            members = local.get(k) or []
            if not members:
                continue

            # Always assign at least one per role if possible
            if len(members) >= 1:
                soap.append(members[0])
            if len(members) >= 2:
                rm.append(members[1])
            else:
                rm.append(members[0])

            if len(members) >= 3:
                third.append(members[2])
            else:
                third.append(members[0])

            # Remaining go to RM (heavier workload)
            if len(members) > 3:
                rm.extend(members[3:])

        # STEP 2 — remove duplicates inside each role
        soap = uniq(soap)
        rm = uniq(rm)
        third = uniq(third)

        # STEP 3 — ensure no empty role if people exist
        everyone = uniq(soap + rm + third)
        if everyone:
            if not soap:
                soap.append(everyone[0])
            if not rm:
                rm.append(everyone[0])
            if not third:
                third.append(everyone[0])

        # Final order A12→A16
        soap = cohort_order(soap)
        rm = cohort_order(rm)
        third = cohort_order(third)

        if mode == "pre":
            return {"soap": soap, "rm_erm": rm, "tsr": third}
        else:
            return {"soap": soap, "rm_erm": rm, "er": third}

    # ---- Output payload ----
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

    # =========================
    # POST OP
    # =========================
    if post_ops:
        total_post = len(post_ops)

        for idx, p in enumerate(post_ops, start=1):
            labels = normalize_pod_label(p.get("meta", "")) or ("POD I", "POD II")

            if total_post == 1:
                team1, team2 = split_all_people_into_two_teams()
                out["post_op"].append({
                    "name": p["name"],
                    "pod_lines": [
                        {"label": labels[0], "team": team1},
                        {"label": labels[1], "team": team2},
                    ]
                })
            else:
                # Multi-patient POST OP: build stable teams first (min 1 from each cohort per patient)
                n = total_post

                # Build teams only once
                if "_post_teams" not in out:
                    teams = [[] for _ in range(n)]

                    # 1) Base members (guarantee A12–A16 presence per patient if available)
                    for i_team in range(n):
                        base = []
                        if a12_s:
                            base.append(a12_s[i_team % len(a12_s)])
                        if a13_s:
                            base.append(a13_s[i_team % len(a13_s)])
                        if a14_s:
                            base.append(a14_s[i_team % len(a14_s)])
                        if a15_s:
                            base.append(a15_s[i_team % len(a15_s)])
                        if a16_s:
                            base.append(a16_s[i_team % len(a16_s)])
                        teams[i_team].extend(base)

                    # 2) Distribute remaining people per cohort round-robin (so everyone appears)
                    #    This is DISJOINT distribution within a cohort (except when cohort size < n).
                    def distribute_extras(pool):
                        if not pool:
                            return
                        for j, name in enumerate(pool):
                            tgt = j % n
                            # avoid duplicating the same person twice in the same team
                            if name not in teams[tgt]:
                                teams[tgt].append(name)

                    distribute_extras(a12_s)
                    distribute_extras(a13_s)
                    distribute_extras(a14_s)
                    distribute_extras(a15_s)
                    distribute_extras(a16_s)

                    # 3) Finalize ordering + ensure minimum team size
                    finalized = []
                    everyone = all_people()
                    for i_team in range(n):
                        t = cohort_order(uniq(teams[i_team]))

                        # If somehow the team is still too small (edge cases), pad using global order
                        # This should never happen in normal cases, but prevents 1–2 people teams.
                        if len(t) < 5 and everyone:
                            for name in everyone:
                                if name not in t:
                                    t.append(name)
                                if len(t) >= 5:
                                    break
                            t = cohort_order(uniq(t))

                        finalized.append(t)

                    # Enforce blacklist across teams (best effort)
                    finalized = enforce_blacklist_many(finalized)
                    out["_post_teams"] = [cohort_order(t) for t in finalized]

                i = idx - 1
                team = out["_post_teams"][i]

                out["post_op"].append({
                    "name": p["name"],
                    "pod_lines": [
                        {"label": labels[0], "team": team},
                        {"label": labels[1], "team": team},
                    ]
                })

    # =========================
    # PRE OP (STRICT + ALL NAMES MUST APPEAR)
    # =========================
    if pre_ops:
        n = len(pre_ops)

        a12_r = shuffled(a12_s, iso_date, "pre:rep:a12")
        a13_r = shuffled(a13_s, iso_date, "pre:rep:a13")
        a14_r = shuffled(a14_s, iso_date, "pre:rep:a14")
        a15_r = shuffled(a15_s, iso_date, "pre:rep:a15")
        a16_r = shuffled(a16_s, iso_date, "pre:rep:a16")

        def pick_rot(pool, i):
            if not pool:
                return None
            return pool[i % len(pool)]

        # ===== CASE: ONLY 1 PRE OP =====
        if n == 1:
            p = pre_ops[0]

            # Pick "double" + "rm-only" reps per cohort (fair within cohort)
            def pick(pool, idx):
                if not pool:
                    return None
                return pool[idx % len(pool)]

            a12_double = pick(a12_r, 0)
            a12_rm = pick(a12_r, 1 if len(a12_r) > 1 else 0)

            a13_double = pick(a13_r, 0)
            a13_rm = pick(a13_r, 1 if len(a13_r) > 1 else 0)

            a14_double = pick(a14_r, 0)
            a14_rm = pick(a14_r, 1 if len(a14_r) > 1 else 0)

            # A15 core executors: 1 SOAP, 1 TSR, 2 RM
            a15_soap = pick(a15_r, 0)
            a15_tsr = pick(a15_r, 1 if len(a15_r) > 1 else 0)
            a15_rm_1 = pick(a15_r, 2 if len(a15_r) > 2 else 0)
            a15_rm_2 = pick(a15_r, 3 if len(a15_r) > 3 else (1 if len(a15_r) > 1 else 0))

            # A16 distribution: 1 SOAP, 2 RM, 2 TSR (as available)
            a16_soap = pick(a16_r, 0)
            a16_rm_1 = pick(a16_r, 1 if len(a16_r) > 1 else 0)
            a16_rm_2 = pick(a16_r, 2 if len(a16_r) > 2 else 0)
            a16_tsr_1 = pick(a16_r, 3 if len(a16_r) > 3 else (1 if len(a16_r) > 1 else 0))
            a16_tsr_2 = pick(a16_r, 4 if len(a16_r) > 4 else (2 if len(a16_r) > 2 else 0))

            # --- SOAP ---
            soap = [a12_double, a13_double, a14_double, a15_soap, a16_soap]

            # --- RM/ERM (heavy) ---
            rm = [
                a12_rm, a13_rm, a14_rm,
                a15_rm_1, a15_rm_2,
                a16_rm_1, a16_rm_2
            ]

            # --- TSR ---
            tsr = [
                a12_double, a13_double, a14_double,
                a15_tsr,
                a16_tsr_1, a16_tsr_2
            ]

            # Clean role lists
            soap = cohort_order(uniq([x for x in soap if x]))
            rm = cohort_order(uniq([x for x in rm if x]))
            tsr = cohort_order(uniq([x for x in tsr if x]))

            # Ensure all names appear at least once (fallback into RM if missing)
            everyone = set(a12_r + a13_r + a14_r + a15_r + a16_r)
            appeared = set(soap + rm + tsr)
            missing = everyone - appeared
            if missing:
                rm = cohort_order(uniq(rm + list(missing)))

            # Enforce blacklist: avoid Ferrel & Maman in the same role line if possible
            if violates_blacklist(soap):
                soap, rm = enforce_blacklist_two_teams(soap, rm)
            if violates_blacklist(tsr):
                tsr, rm = enforce_blacklist_two_teams(tsr, rm)
            if violates_blacklist(rm):
                rm, tsr = enforce_blacklist_two_teams(rm, tsr)

            out["pre_op"].append({
                "name": p["name"],
                "soap": soap,
                "rm_erm": rm,
                "tsr": tsr,
            })

        # ===== CASE: MULTIPLE PRE OPS =====
        else:
            # Target composition per patient (multi pre-op):
            # SOAP: 1 A12, 1 A13, 1 A14, 1 A15, 1 A16
            # RM/ERM: 1 A12, 1 A13, 1 A14, 2 A15, 2 A16  (heavy)
            # TSR: 1 A12, 1 A13, 1 A14, 1 A15, 2 A16     (heavy)
            #
            # This branch enforces new A15 distribution rule.

            # Precompute reps per patient index to keep stable + fair
            reps = []
            for i in range(n):
                reps.append({
                    "a12_soap": pick_rot(a12_r, i),
                    "a13_soap": pick_rot(a13_r, i),
                    "a14_soap": pick_rot(a14_r, i),
                    "a16_soap": pick_rot(a16_r, i),

                    "a12_tsr": pick_rot(a12_r, i + 1),
                    "a13_tsr": pick_rot(a13_r, i + 1),
                    "a14_tsr": pick_rot(a14_r, i + 1),
                    "a16_tsr_1": pick_rot(a16_r, i + 1),
                    "a16_tsr_2": pick_rot(a16_r, i + 2),

                    "a12_rm": pick_rot(a12_r, i + 2),
                    "a13_rm": pick_rot(a13_r, i + 2),
                    "a14_rm": pick_rot(a14_r, i + 2),
                    "a16_rm_1": pick_rot(a16_r, i + 2),
                    "a16_rm_2": pick_rot(a16_r, i + 3),
                })

            for i, p in enumerate(pre_ops):
                rrep = reps[i]

                # ---- A15 distribution rule ----
                # Default: each patient gets 2 A15 (SOAP + TSR)
                # If PRE OP patients exceed clean pairing, the LAST patient absorbs remaining A15
                a15_soap = None
                a15_tsr = None
                a15_extra_rm = []

                if a15_r:
                    total_a15 = len(a15_r)
                    base_per_patient = 2

                    # how many would be consumed before this patient
                    used_before = i * base_per_patient

                    # if last patient → absorb remainder
                    if i == n - 1:
                        remaining = a15_r[used_before:] if used_before < total_a15 else []
                        if remaining:
                            a15_soap = remaining[0]
                        if len(remaining) > 1:
                            a15_tsr = remaining[1]
                        if len(remaining) > 2:
                            a15_extra_rm = remaining[2:]
                    else:
                        slice_start = used_before
                        slice_end = slice_start + base_per_patient
                        pair = a15_r[slice_start:slice_end]
                        if pair:
                            a15_soap = pair[0]
                        if len(pair) > 1:
                            a15_tsr = pair[1]

                # ---- SOAP (target 5 people) ----
                soap = [
                    rrep["a12_soap"],
                    rrep["a13_soap"],
                    rrep["a14_soap"],
                    a15_soap,
                    rrep["a16_soap"],
                ]

                # ---- RM/ERM (heavy) ----
                # Must contain 2 A15: (SOAP A15 + TSR A15), and absorb extra A15 if last patient
                rm = [
                    rrep["a12_rm"],
                    rrep["a13_rm"],
                    rrep["a14_rm"],
                    a15_soap,
                    a15_tsr,
                    rrep["a16_rm_1"],
                    rrep["a16_rm_2"],
                ] + a15_extra_rm

                # ---- TSR (heavy) ----
                # Must contain exactly 1 A15: TSR executor only
                tsr = [
                    rrep["a12_tsr"],
                    rrep["a13_tsr"],
                    rrep["a14_tsr"],
                    a15_tsr,
                    rrep["a16_tsr_1"],
                    rrep["a16_tsr_2"],
                ]

                # Clean role lists
                soap = cohort_order(uniq([x for x in soap if x]))
                rm = cohort_order(uniq([x for x in rm if x]))
                tsr = cohort_order(uniq([x for x in tsr if x]))

                # Guarantee representation A12–A16 in EACH role if available
                def ensure_role_has(role_list: list[str], pool: list[str], pick_idx: int):
                    if not pool:
                        return role_list
                    if any(x in set(pool) for x in role_list):
                        return role_list
                    cand = pick_rot(pool, pick_idx)
                    if cand:
                        return cohort_order(uniq(role_list + [cand]))
                    return role_list

                soap = ensure_role_has(soap, a12_r, i)
                soap = ensure_role_has(soap, a13_r, i)
                soap = ensure_role_has(soap, a14_r, i)
                # DO NOT auto-insert A15 here — A15 distribution is handled strictly by the A15 rule above
                soap = ensure_role_has(soap, a16_r, i)

                rm = ensure_role_has(rm, a12_r, i + 2)
                rm = ensure_role_has(rm, a13_r, i + 2)
                rm = ensure_role_has(rm, a14_r, i + 2)
                # A15 must follow strict per‑patient distribution (2,2,... remainder on last)
                rm = ensure_role_has(rm, a16_r, i + 2)

                tsr = ensure_role_has(tsr, a12_r, i + 1)
                tsr = ensure_role_has(tsr, a13_r, i + 1)
                tsr = ensure_role_has(tsr, a14_r, i + 1)
                # A15 already assigned earlier (SOAP/TSR logic)
                tsr = ensure_role_has(tsr, a16_r, i + 1)

                # Make sure across the whole PRE OP section, everyone appears at least once:
                # Put "missing" names into RM/ERM (heavy) for THIS patient (spread by index).
                everyone = set(a12_r + a13_r + a14_r + a15_r + a16_r)
                appeared = set()  # recomputed per patient below from global state
                # We'll compute global appeared so far using out["pre_op"] already appended items
                for prev in out["pre_op"]:
                    appeared.update(prev.get("soap", []))
                    appeared.update(prev.get("rm_erm", []))
                    appeared.update(prev.get("tsr", []))
                appeared.update(soap + rm + tsr)

                missing = list(everyone - appeared)
                if missing:
                    missing = shuffled(missing, iso_date, f"pre:missing:{i}")
                    rm = cohort_order(uniq(rm + missing[: max(0, 4 - (len(rm) % 4))]))  # add a few, not too many

                out["pre_op"].append({
                    "name": p["name"],
                    "soap": soap,
                    "rm_erm": rm,
                    "tsr": tsr,
                })

    # =========================
    # IGD
    # =========================
    if igds:
        usage = {}

        for idx, p in enumerate(igds, start=1):
            buckets = {
                "a12": a12_s[:],
                "a13": a13_s[:],
                "a14": a14_s[:],
                "a15": a15_s[:],
                "a16": a16_s[:],
            }

            soap = []
            rm = []
            er = []

            for cohort_key, cohort_list in buckets.items():
                if not cohort_list:
                    continue

                ordered = sorted(cohort_list, key=lambda x: usage.get(x, 0))

                s = ordered[0]
                soap.append(s)
                usage[s] = usage.get(s, 0) + 1

                r = ordered[1] if len(ordered) > 1 else ordered[0]
                rm.append(r)
                usage[r] = usage.get(r, 0) + 1

                e = ordered[2] if len(ordered) > 2 else ordered[0]
                er.append(e)
                usage[e] = usage.get(e, 0) + 1

            out["igd"].append({
                "name": p["name"],
                "soap": cohort_order(uniq(soap)),
                "rm_erm": cohort_order(uniq(rm)),
                "er": cohort_order(uniq(er)),
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
