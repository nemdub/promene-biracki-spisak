#!/usr/bin/env bash
# check_anomalies.sh — Detect statistically suspicious week-over-week patterns
# in the weekly voter-registry CSVs and write ANOMALIES.md.
#
# Reads data/promene_*.csv and data/biraci_*.csv, computes a per-(municipality,
# change-type) baseline from all weeks PRIOR to the latest one, and flags:
#   * city × change-type spikes
#   * nationwide change-type spikes
#   * sustained 3-week trends
#   * voter-count week-over-week jumps
#
# Output is deterministic from the data state — re-running on unchanged data
# produces a byte-identical ANOMALIES.md (so no spam commits).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="${SCRIPT_DIR}/data"
OUTPUT_FILE="${SCRIPT_DIR}/ANOMALIES.md"

python3 - "$INPUT_DIR" "$OUTPUT_FILE" <<'PYEOF'
import sys, csv, os, re
from collections import defaultdict
from statistics import median

input_dir, output_file = sys.argv[1], sys.argv[2]

# ── Tunables ──────────────────────────────────────────────────────────────────
MIN_HISTORY_WEEKS  = 4      # need >=4 prior weeks to flag a (city,type) spike
SPIKE_FACTOR       = 3.0    # latest >= 3x historical median
SPIKE_MAD_K        = 5.0    # OR latest >= median + 5*MAD (whichever is larger)
SPIKE_ABS_FLOOR    = 10     # latest absolute value must be >= 10 to flag
NATIONAL_FACTOR    = 1.5    # nationwide weekly total >= 1.5x historical median
NATIONAL_ABS_FLOOR = 50
TREND_WINDOW       = 3      # last N weeks all elevated
TREND_FACTOR       = 1.5    # each of last N weeks > 1.5 * pre-window median
TREND_ABS_FLOOR    = 10
VOTER_JUMP_PCT     = 0.003  # 0.3% week-over-week change in total voters

PROMENE_RE = re.compile(r'^promene_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.csv$')
BIRACI_RE  = re.compile(r'^biraci_(\d{4}-\d{2}-\d{2})\.csv$')


def mad(values, med):
    if not values:
        return 0.0
    return median([abs(v - med) for v in values])


def make_row(*cells):
    return "| " + " | ".join(str(c) for c in cells) + " |"


def fmt_med(v):
    return f"{v:.0f}" if v == int(v) else f"{v:.1f}"


def fmt_factor(f):
    if f == float('inf'):
        return "∞"
    return f"{f:.1f}×"


def fmt_pct(p):
    return f"{p*100:+.2f}%"


def sr_prior_weeks(n):
    """Serbian plural for 'N prior weeks'."""
    mod100 = n % 100
    mod10 = n % 10
    if 11 <= mod100 <= 14:
        return f"{n} претходних недеља"
    if mod10 == 1:
        return f"{n} претходна недеља"
    if mod10 in (2, 3, 4):
        return f"{n} претходне недеље"
    return f"{n} претходних недеља"


# ── Discover and load promene files ──────────────────────────────────────────
promene_files = []
for fname in os.listdir(input_dir):
    m = PROMENE_RE.match(fname)
    if m:
        promene_files.append((m.group(1), m.group(2), fname))
promene_files.sort()  # ISO start-date string sorts chronologically

if not promene_files:
    sys.stderr.write("ERROR: no promene_*.csv files found\n")
    sys.exit(1)

# data[(opstina, vrsta)][iso_start] = count
data = defaultdict(lambda: defaultdict(int))
# nat[vrsta][iso_start] = nationwide weekly total
nat = defaultdict(lambda: defaultdict(int))
periods = []  # list of (iso_start, iso_end)

for iso_od, iso_do, fname in promene_files:
    fpath = os.path.join(input_dir, fname)
    with open(fpath, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            opstina = row["ОПШТИНА_ГРАД"].strip()
            vrsta = row["ВРСТА_ПРОМЕНЕ"].strip()
            try:
                count = int(row["БРОЈ_ПРОМЕНА"].strip())
            except (KeyError, ValueError):
                continue
            data[(opstina, vrsta)][iso_od] += count
            nat[vrsta][iso_od] += count
    periods.append((iso_od, iso_do))

latest_iso_od, latest_iso_do = periods[-1]
historical_periods = [p[0] for p in periods[:-1]]
n_history = len(historical_periods)

# ── Detection: city × change-type spikes ─────────────────────────────────────
spikes = []  # (opstina, vrsta, latest_val, hist_med, factor)
for (opstina, vrsta), by_period in data.items():
    latest_val = by_period.get(latest_iso_od, 0)
    if latest_val < SPIKE_ABS_FLOOR:
        continue
    hist_vals = [by_period.get(p, 0) for p in historical_periods]
    if len(hist_vals) < MIN_HISTORY_WEEKS:
        continue
    med = median(hist_vals)
    m = mad(hist_vals, med)
    if med == 0 and m == 0:
        # All-zero history: latest >= floor is itself a brand-new signal
        threshold = SPIKE_ABS_FLOOR
    else:
        threshold = max(SPIKE_FACTOR * med, med + SPIKE_MAD_K * m)
    if latest_val >= threshold:
        factor = (latest_val / med) if med > 0 else float('inf')
        spikes.append((opstina, vrsta, latest_val, med, factor))

spikes.sort(key=lambda r: (
    -(r[4] if r[4] != float('inf') else 9e18),
    -r[2],
    r[0],
    r[1],
))

# ── Detection: nationwide change-type spikes ─────────────────────────────────
nat_spikes = []  # (vrsta, latest, hist_med, factor)
for vrsta, by_period in nat.items():
    latest_val = by_period.get(latest_iso_od, 0)
    if latest_val < NATIONAL_ABS_FLOOR:
        continue
    hist_vals = [by_period.get(p, 0) for p in historical_periods]
    if len(hist_vals) < MIN_HISTORY_WEEKS:
        continue
    med = median(hist_vals)
    if med <= 0:
        continue
    if latest_val >= NATIONAL_FACTOR * med:
        nat_spikes.append((vrsta, latest_val, med, latest_val / med))

nat_spikes.sort(key=lambda r: (-r[3], r[0]))

# ── Detection: sustained 3-week trends ───────────────────────────────────────
trends = []  # (opstina, vrsta, window_vals, pre_med, avg_factor)
window_labels = []
if len(periods) >= TREND_WINDOW + MIN_HISTORY_WEEKS:
    window_periods = [p[0] for p in periods[-TREND_WINDOW:]]
    pre_periods = [p[0] for p in periods[:-TREND_WINDOW]]
    window_labels = window_periods
    for (opstina, vrsta), by_period in data.items():
        window_vals = [by_period.get(p, 0) for p in window_periods]
        if window_vals[-1] < TREND_ABS_FLOOR:
            continue
        pre_vals = [by_period.get(p, 0) for p in pre_periods]
        if len(pre_vals) < MIN_HISTORY_WEEKS:
            continue
        pre_med = median(pre_vals)
        if pre_med <= 0:
            continue
        if all(v > TREND_FACTOR * pre_med for v in window_vals):
            avg_factor = (sum(window_vals) / len(window_vals)) / pre_med
            trends.append((opstina, vrsta, window_vals, pre_med, avg_factor))

trends.sort(key=lambda r: (-r[4], r[0], r[1]))

# ── Discover and load biraci files ───────────────────────────────────────────
biraci_files = []
for fname in os.listdir(input_dir):
    m = BIRACI_RE.match(fname)
    if m:
        biraci_files.append((m.group(1), fname))
biraci_files.sort()


def load_biraci(fname):
    out = {}
    with open(os.path.join(input_dir, fname), encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            try:
                out[row["ОПШТИНА_ГРАД"].strip()] = int(row["БРОЈ_БИРАЧА"].strip())
            except (KeyError, ValueError):
                continue
    return out


voter_jumps = []  # (opstina, latest_count, prev_count, pct)
prev_b_iso = latest_b_iso = None
if len(biraci_files) >= 2:
    prev_b_iso, prev_fname = biraci_files[-2]
    latest_b_iso, latest_fname = biraci_files[-1]
    prev_b = load_biraci(prev_fname)
    latest_b = load_biraci(latest_fname)
    for opstina, latest_count in latest_b.items():
        if opstina not in prev_b:
            continue
        prev_count = prev_b[opstina]
        if prev_count <= 0:
            continue
        pct = (latest_count - prev_count) / prev_count
        if abs(pct) > VOTER_JUMP_PCT:
            voter_jumps.append((opstina, latest_count, prev_count, pct))
    voter_jumps.sort(key=lambda r: (-abs(r[3]), r[0]))

# ── Render markdown ──────────────────────────────────────────────────────────
lines = []
lines.append("# Извештај о аномалијама")
lines.append("")
lines.append(
    f"_Последња анализирана недеља: **{latest_iso_od} – {latest_iso_do}** · "
    f"историја: **{sr_prior_weeks(n_history)}**_"
)
lines.append("")

total = len(spikes) + len(nat_spikes) + len(trends) + len(voter_jumps)

lines.append("## Преглед")
lines.append(f"- Скокови по општини и врсти промене: **{len(spikes)}**")
lines.append(f"- Национални скокови по врсти промене: **{len(nat_spikes)}**")
lines.append(f"- Постојани тронедељни трендови: **{len(trends)}**")
lines.append(f"- Скокови у броју бирача: **{len(voter_jumps)}**")
lines.append("")

if total == 0:
    lines.append("**Нема аномалија.**")
    lines.append("")

if spikes:
    lines.append("## Скокови по општини и врсти промене")
    lines.append(make_row("Општина/Град", "Врста промене", "Ова недеља", "Ист. медијана", "Фактор"))
    lines.append("|---|---|---:|---:|---:|")
    for opstina, vrsta, val, med, factor in spikes:
        lines.append(make_row(opstina, vrsta, val, fmt_med(med), fmt_factor(factor)))
    lines.append("")

if nat_spikes:
    lines.append("## Национални скокови по врсти промене")
    lines.append(make_row("Врста промене", "Ова недеља", "Ист. медијана", "Фактор"))
    lines.append("|---|---:|---:|---:|")
    for vrsta, val, med, factor in nat_spikes:
        lines.append(make_row(vrsta, val, fmt_med(med), fmt_factor(factor)))
    lines.append("")

if trends:
    lines.append(f"## Постојани тронедељни трендови")
    header_cells = ["Општина/Град", "Врста промене"] + window_labels + ["Основна медијана", "Просечан фактор"]
    lines.append(make_row(*header_cells))
    sep = "|---|---|" + "---:|" * TREND_WINDOW + "---:|---:|"
    lines.append(sep)
    for opstina, vrsta, vals, pre_med, factor in trends:
        row_cells = [opstina, vrsta] + list(vals) + [fmt_med(pre_med), fmt_factor(factor)]
        lines.append(make_row(*row_cells))
    lines.append("")

if voter_jumps:
    lines.append(f"## Скокови у броју бирача ({prev_b_iso} → {latest_b_iso})")
    lines.append(make_row("Општина/Град", "Пре", "Сада", "Δ", "Δ %"))
    lines.append("|---|---:|---:|---:|---:|")
    for opstina, latest_count, prev_count, pct in voter_jumps:
        delta = latest_count - prev_count
        sign = "+" if delta >= 0 else ""
        lines.append(make_row(opstina, prev_count, latest_count, f"{sign}{delta}", fmt_pct(pct)))
    lines.append("")

text = "\n".join(lines)
if not text.endswith("\n"):
    text += "\n"

with open(output_file, "w", encoding="utf-8") as f:
    f.write(text)

print(f"OK: {output_file}")
print(f"  Latest period: {latest_iso_od} – {latest_iso_do}  (history: {n_history} weeks)")
print(f"  Spikes: {len(spikes)} | Nat. spikes: {len(nat_spikes)} | Trends: {len(trends)} | Voter jumps: {len(voter_jumps)}")
PYEOF
