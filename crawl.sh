#!/usr/bin/env bash
# crawl.sh — Fetch voter-registry change counts from birackispisak.gov.rs
# and append weekly data to a CSV file.
#
# Cron example (every Monday at 06:00):
#   0 6 * * 1 /opt/promene-biracki-spisak/crawl.sh
set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────────
URL="https://upit.birackispisak.gov.rs/BrojPromena"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/data"
LOG_FILE="${SCRIPT_DIR}/crawl.log"
# ──────────────────────────────────────────────────────────────────────────────

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE"; }

mkdir -p "$OUTPUT_DIR"

log "Fetching $URL"

HTML_FILE=$(mktemp)
trap 'rm -f "$HTML_FILE"' EXIT

curl -sf --max-time 30 -k \
    -H "Accept: text/html,application/xhtml+xml" \
    -A "Mozilla/5.0 (compatible; birackispisak-crawler/1.0)" \
    "$URL" > "$HTML_FILE" \
    || { log "ERROR: curl failed (exit $?)"; exit 1; }

RESULT=$(python3 - "$HTML_FILE" "$OUTPUT_DIR" <<'PYEOF'
import sys, csv, re
from html.parser import HTMLParser

html_path, output_dir = sys.argv[1], sys.argv[2]

with open(html_path, encoding="utf-8") as f:
    html = f.read()

class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.period_text = ""
        self._in_strong = False
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.rows = []
        self._current_row = []
        self._current_cell = ""

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table" and "table" in attrs_dict.get("class", ""):
            self.in_table = True
        if self.in_table and tag == "tr":
            self.in_row = True
            self._current_row = []
        if self.in_row and tag in ("td", "th"):
            self.in_cell = True
            self._current_cell = ""
        if tag == "strong":
            self._in_strong = True

    def handle_endtag(self, tag):
        if tag == "table":
            self.in_table = False
        if self.in_table and tag == "tr":
            self.in_row = False
            if self._current_row:
                self.rows.append(self._current_row[:])
        if self.in_row and tag in ("td", "th"):
            self.in_cell = False
            self._current_row.append(self._current_cell.strip())
        if tag == "strong":
            self._in_strong = False

    def handle_data(self, data):
        if self._in_strong and not self.period_text:
            text = data.strip()
            if "ОД" in text and "ДО" in text:
                self.period_text = text
        if self.in_cell:
            self._current_cell += data

parser = TableParser()
parser.feed(html)

# Extract "DD.MM.YYYY" dates from period heading
dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', parser.period_text)
if len(dates) >= 2:
    period_od, period_do = dates[0], dates[1]
    # Build ISO dates for the filename
    def dd_mm_yyyy_to_iso(d):
        return f"{d[6:10]}-{d[3:5]}-{d[0:2]}"
    filename = f"promene_{dd_mm_yyyy_to_iso(period_od)}_{dd_mm_yyyy_to_iso(period_do)}.csv"
else:
    from datetime import date
    period_od = period_do = ""
    filename = f"promene_{date.today().isoformat()}.csv"

output_path = f"{output_dir}/{filename}"

# Filter out the header row; keep only data rows with exactly 3 cells
data_rows = [r for r in parser.rows if len(r) == 3 and r[0] != "ОПШТИНА/ГРАД"]

if not data_rows:
    print("ERROR:no data rows found")
    sys.exit(1)

file_exists = False
try:
    open(output_path)
    file_exists = True
except FileNotFoundError:
    pass

with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["ПЕРИОД_ОД", "ПЕРИОД_ДО", "ОПШТИНА_ГРАД", "ВРСТА_ПРОМЕНЕ", "БРОЈ_ПРОМЕНА"])
    for row in data_rows:
        writer.writerow([period_od, period_do, row[0], row[1], row[2]])

print(f"OK:{output_path}:{len(data_rows)}")
PYEOF
)

if [[ "$RESULT" == ERROR:* ]]; then
    log "ERROR: ${RESULT#ERROR:}"
    exit 1
fi

OUTPUT_PATH="${RESULT#OK:}"
OUTPUT_PATH="${OUTPUT_PATH%%:*}"
ROW_COUNT="${RESULT##*:}"

log "Done — ${ROW_COUNT} rows written to ${OUTPUT_PATH}"
