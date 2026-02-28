#!/usr/bin/env bash
# crawl.sh — Fetch weekly voter-registry data from birackispisak.gov.rs
# Produces two CSV files per run:
#   data/promene_YYYY-MM-DD_YYYY-MM-DD.csv  — changes by municipality & type
#   data/biraci_YYYY-MM-DD.csv              — total voters by municipality
#
# Cron example (every Monday at 06:00):
#   0 6 * * 1 /opt/promene-biracki-spisak/crawl.sh
set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────────
URL_PROMENE="https://upit.birackispisak.gov.rs/BrojPromena"
URL_BIRACI="https://upit.birackispisak.gov.rs/PregledBrojaBiraca"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/data"
LOG_FILE="${SCRIPT_DIR}/crawl.log"
# ──────────────────────────────────────────────────────────────────────────────

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE"; }

mkdir -p "$OUTPUT_DIR"

HTML_PROMENE=$(mktemp)
HTML_BIRACI=$(mktemp)
trap 'rm -f "$HTML_PROMENE" "$HTML_BIRACI"' EXIT

# ── Fetch both pages ───────────────────────────────────────────────────────────
_curl() {
    curl -sf --max-time 30 -k \
        -H "Accept: text/html,application/xhtml+xml" \
        "$1" > "$2"
}

log "Fetching $URL_PROMENE"
_curl "$URL_PROMENE" "$HTML_PROMENE" || { log "ERROR: curl failed for promene (exit $?)"; exit 1; }

log "Fetching $URL_BIRACI"
_curl "$URL_BIRACI"  "$HTML_BIRACI"  || { log "ERROR: curl failed for biraci (exit $?)";  exit 1; }

# ── Parse both pages and write CSVs ───────────────────────────────────────────
RESULTS=$(python3 - "$HTML_PROMENE" "$HTML_BIRACI" "$OUTPUT_DIR" <<'PYEOF'
import sys, csv, re
from html.parser import HTMLParser

html_promene, html_biraci, output_dir = sys.argv[1], sys.argv[2], sys.argv[3]

# ── Shared HTML table parser ───────────────────────────────────────────────────
class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.heading = ""
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
        if self._in_strong and not self.heading:
            self.heading = data.strip()
        if self.in_cell:
            self._current_cell += data

def dd_mm_yyyy_to_iso(d):
    return f"{d[6:10]}-{d[3:5]}-{d[0:2]}"

def parse_html(path):
    with open(path, encoding="utf-8") as f:
        html = f.read()
    p = TableParser()
    p.feed(html)
    return p

def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

# ── Promene: period "ОД DD.MM.YYYY. ДО DD.MM.YYYY." ──────────────────────────
p = parse_html(html_promene)
dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', p.heading)
if len(dates) >= 2:
    period_od, period_do = dates[0], dates[1]
    fname = f"promene_{dd_mm_yyyy_to_iso(period_od)}_{dd_mm_yyyy_to_iso(period_do)}.csv"
else:
    from datetime import date as _date
    period_od = period_do = ""
    fname = f"promene_{_date.today().isoformat()}.csv"

data_rows = [r for r in p.rows if len(r) == 3 and r[0] != "ОПШТИНА/ГРАД"]
if not data_rows:
    print("ERROR:promene:no data rows found"); sys.exit(1)

out = f"{output_dir}/{fname}"
write_csv(out, ["ПЕРИОД_ОД", "ПЕРИОД_ДО", "ОПШТИНА_ГРАД", "ВРСТА_ПРОМЕНЕ", "БРОЈ_ПРОМЕНА"],
          [[period_od, period_do, r[0], r[1], r[2]] for r in data_rows])
print(f"OK:promene:{out}:{len(data_rows)}")

# ── Biraci: single date "СТАЊЕ НА ДАН DD.MM.YYYY." ───────────────────────────
b = parse_html(html_biraci)
dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', b.heading)
if dates:
    datum = dates[0]
    fname = f"biraci_{dd_mm_yyyy_to_iso(datum)}.csv"
else:
    from datetime import date as _date
    datum = ""
    fname = f"biraci_{_date.today().isoformat()}.csv"

data_rows = [r for r in b.rows if len(r) == 2 and r[0] != "ОПШТИНА/ГРАД"]
if not data_rows:
    print("ERROR:biraci:no data rows found"); sys.exit(1)

out = f"{output_dir}/{fname}"
write_csv(out, ["ДАТУМ_СТАЊА", "ОПШТИНА_ГРАД", "БРОЈ_БИРАЧА"],
          [[datum, r[0], r[1]] for r in data_rows])
print(f"OK:biraci:{out}:{len(data_rows)}")
PYEOF
)

# ── Check results and log ──────────────────────────────────────────────────────
while IFS= read -r line; do
    if [[ "$line" == ERROR:* ]]; then
        log "ERROR: ${line#ERROR:}"
        exit 1
    fi
    # line format: OK:<type>:<path>:<count>
    TYPE="${line#OK:}";  TYPE="${TYPE%%:*}"
    REST="${line#OK:${TYPE}:}"
    PATH_="${REST%%:*}"
    COUNT="${REST##*:}"
    log "${TYPE} — ${COUNT} rows → ${PATH_}"
done <<< "$RESULTS"
