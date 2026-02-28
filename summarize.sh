#!/usr/bin/env bash
# summarize.sh — Aggregate all weekly CSVs from data/ into a single pivot CSV.
#
# Output: data/summary.csv
#   Rows    = unique (ОПШТИНА_ГРАД, ВРСТА_ПРОМЕНЕ) pairs, sorted
#   Columns = one column per weekly period (sorted chronologically) + УКУПНО
#   Footer  = УКУПНО row with column totals
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INPUT_DIR="${SCRIPT_DIR}/data"
OUTPUT_FILE="${INPUT_DIR}/summary.csv"

python3 - "$INPUT_DIR" "$OUTPUT_FILE" <<'PYEOF'
import sys, csv, os, re
from collections import defaultdict

input_dir, output_file = sys.argv[1], sys.argv[2]

# ── Load all weekly CSV files ──────────────────────────────────────────────────
# data[(opstina, vrsta)][period_label] = count
data = defaultdict(lambda: defaultdict(int))
periods = {}   # period_label -> (od, do) for sorting

CSV_PATTERN = re.compile(r'^promene_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.csv$')

files_read = 0
for fname in sorted(os.listdir(input_dir)):
    if not CSV_PATTERN.match(fname):
        continue
    fpath = os.path.join(input_dir, fname)
    with open(fpath, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            period_od  = row["ПЕРИОД_ОД"].strip()
            period_do  = row["ПЕРИОД_ДО"].strip()
            opstina    = row["ОПШТИНА_ГРАД"].strip()
            vrsta      = row["ВРСТА_ПРОМЕНЕ"].strip()
            try:
                count  = int(row["БРОЈ_ПРОМЕНА"].strip())
            except ValueError:
                continue

            label = f"{period_od} – {period_do}"
            data[(opstina, vrsta)][label] += count
            if label not in periods:
                periods[label] = (period_od, period_do)
    files_read += 1

if files_read == 0:
    print(f"ERROR: no weekly CSV files found in {input_dir}")
    sys.exit(1)

# ── Sort periods chronologically by start date ─────────────────────────────────
def parse_date(d):
    """DD.MM.YYYY -> (YYYY, MM, DD) tuple for sorting."""
    parts = d.replace(".", " ").split()
    if len(parts) == 3:
        return (int(parts[2]), int(parts[1]), int(parts[0]))
    return (0, 0, 0)

sorted_periods = sorted(periods.keys(), key=lambda lbl: parse_date(periods[lbl][0]))

# ── Sort rows: first by municipality, then by change type ─────────────────────
all_keys = sorted(data.keys(), key=lambda k: (k[0], k[1]))

# ── Write pivot CSV ────────────────────────────────────────────────────────────
with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)

    # Header
    writer.writerow(["ОПШТИНА_ГРАД", "ВРСТА_ПРОМЕНЕ"] + sorted_periods + ["УКУПНО"])

    # Column totals accumulator
    col_totals = defaultdict(int)

    for (opstina, vrsta) in all_keys:
        row_values = [data[(opstina, vrsta)].get(p, 0) for p in sorted_periods]
        row_total  = sum(row_values)
        writer.writerow([opstina, vrsta] + row_values + [row_total])
        for p, v in zip(sorted_periods, row_values):
            col_totals[p] += v

    # Totals footer row
    totals_row = [col_totals[p] for p in sorted_periods]
    grand_total = sum(totals_row)
    writer.writerow(["УКУПНО", ""] + totals_row + [grand_total])

print(f"OK:{output_file}:{files_read} files:{len(all_keys)} rows:{len(sorted_periods)} periods")
PYEOF

RESULT=$?
if [ $RESULT -ne 0 ]; then
    echo "ERROR: summarize failed" >&2
    exit 1
fi