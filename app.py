import os
import csv
from datetime import datetime

from flask import Flask, render_template, request, send_file, redirect, url_for, flash
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = "replace-this-with-a-random-secret"

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {"csv"}
LAST_REPORT_PATH = None


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def normalize_header(name):
    return str(name).strip().lower()


def normalize_value(value):
    if value is None:
        return ""
    return str(value).strip()


def parse_datetime(value):
    value = " ".join(normalize_value(value).split())
    formats = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %I:%M:%S %p",
        "%d/%m/%Y %I:%M %p",
        "%d/%m/%Y %I:%M:%S%p",
        "%d/%m/%Y %I:%M%p",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized datetime format: {value}")


def read_csv_normalized(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            normalized_row = {}
            for k, v in row.items():
                normalized_row[normalize_header(k)] = v
            rows.append(normalized_row)
        return rows


def get_field(row, possible_names, required=True, default=""):
    for name in possible_names:
        key = normalize_header(name)
        if key in row and row[key] is not None:
            return row[key]
    if required:
        raise ValueError(f"Missing required column(s): {', '.join(possible_names)}")
    return default


def overlap(trip_start, trip_end, linkt_start, linkt_end):
    return trip_start <= linkt_end and trip_end >= linkt_start


def clean_identifier(value):
    return normalize_value(value).upper().strip()


def process_files(turo_path, linkt_path):
    turo_rows = read_csv_normalized(turo_path)
    linkt_rows = read_csv_normalized(linkt_path)

    # Parse Turo rows
    turo = []
    for row in turo_rows:
        lpn = clean_identifier(get_field(row, ["lpn"], required=False, default=""))
        tag_number = clean_identifier(get_field(row, ["tag number", "tag_number", "tagnumber"], required=False, default=""))

        turo.append({
            "trip_id": get_field(row, ["trip_id"]).strip(),
            "lpn": lpn,
            "tag_number": tag_number,
            "start_datetime": parse_datetime(get_field(row, ["start_datetime", "start datetime", "start date"])),
            "end_datetime": parse_datetime(get_field(row, ["end_datetime", "end datetime", "end date"])),
            "guest_name": get_field(row, ["guest_name", "guest name"]).strip(),
        })

    # Parse Linkt rows, using absolute amounts
    linkt = []
    for row in linkt_rows:
        amount_raw = normalize_value(get_field(row, ["amount"], required=False, default="0")).replace("$", "").replace(",", "")
        try:
            amount = abs(float(amount_raw)) if amount_raw else 0.0
        except ValueError:
            amount = 0.0

        lpn = clean_identifier(get_field(row, ["lpn"], required=False, default=""))
        tag_number = clean_identifier(get_field(row, ["tag number", "tag_number", "tagnumber"], required=False, default=""))

        linkt.append({
            "lpn": lpn,
            "tag_number": tag_number,
            "details": get_field(row, ["details"], required=False, default=""),
            "start_datetime": parse_datetime(get_field(row, ["start date", "start_date"])),
            "end_datetime": parse_datetime(get_field(row, ["end date", "end_date"])),
            "amount": amount,
        })

    detailed_rows = []

    for idx, toll in enumerate(linkt, start=1):
        candidates = []
        match_method = ""

        # 1) Prefer LPN match
        if toll["lpn"]:
            for trip in turo:
                if trip["lpn"] and trip["lpn"] == toll["lpn"] and overlap(
                    trip["start_datetime"], trip["end_datetime"], toll["start_datetime"], toll["end_datetime"]
                ):
                    candidates.append(trip)
            if candidates:
                match_method = "LPN"

        # 2) Fallback to Tag Number match
        if not candidates and toll["tag_number"]:
            for trip in turo:
                if trip["tag_number"] and trip["tag_number"] == toll["tag_number"] and overlap(
                    trip["start_datetime"], trip["end_datetime"], toll["start_datetime"], toll["end_datetime"]
                ):
                    candidates.append(trip)
            if candidates:
                match_method = "Tag Number"

        if len(candidates) == 1:
            trip = candidates[0]
            status = f"Matched ({match_method})"
        elif len(candidates) > 1:
            candidates.sort(key=lambda x: (x["end_datetime"] - x["start_datetime"]).total_seconds())
            trip = candidates[0]
            status = f"Matched-Multiple ({match_method})"
        else:
            trip = None
            status = "Unmatched"

        detailed_rows.append({
            "trip_id": trip["trip_id"] if trip else "",
            "guest_name": trip["guest_name"] if trip else "",
            "turo_lpn": trip["lpn"] if trip else "",
            "turo_tag_number": trip["tag_number"] if trip else "",
            "linkt_lpn": toll["lpn"],
            "linkt_tag_number": toll["tag_number"],
            "toll_no": idx,
            "linkt_start_datetime": toll["start_datetime"].strftime("%Y-%m-%d %H:%M:%S"),
            "linkt_end_datetime": toll["end_datetime"].strftime("%Y-%m-%d %H:%M:%S"),
            "toll_details": toll["details"],
            "toll_amount": f"{toll['amount']:.2f}",
            "total_charge": f"{toll['amount']:.2f}",
            "status": status,
        })

    matched_rows = [r for r in detailed_rows if r["status"].startswith("Matched")]

    summary_map = {}
    for row in matched_rows:
        key = (row["trip_id"], row["guest_name"])
        if key not in summary_map:
            summary_map[key] = {
                "trip_id": row["trip_id"],
                "guest_name": row["guest_name"],
                "toll_count": 0,
                "toll_amount": 0.0,
                "total_charge": 0.0,
            }
        summary_map[key]["toll_count"] += 1
        summary_map[key]["toll_amount"] += float(row["toll_amount"])
        summary_map[key]["total_charge"] += float(row["total_charge"])

    summary_rows = []
    for item in summary_map.values():
        summary_rows.append({
            "trip_id": item["trip_id"],
            "guest_name": item["guest_name"],
            "toll_count": item["toll_count"],
            "toll_amount": f"{item['toll_amount']:.2f}",
            "total_charge": f"{item['total_charge']:.2f}",
        })

    return matched_rows, summary_rows, detailed_rows


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


@app.route("/", methods=["GET", "POST"])
def index():
    global LAST_REPORT_PATH

    if request.method == "POST":
        if "turo_file" not in request.files or "linkt_file" not in request.files:
            flash("Please upload both Turo and Linkt CSV files.")
            return redirect(request.url)

        turo_file = request.files["turo_file"]
        linkt_file = request.files["linkt_file"]

        if not turo_file.filename or not linkt_file.filename:
            flash("Please select both files.")
            return redirect(request.url)

        if not (allowed_file(turo_file.filename) and allowed_file(linkt_file.filename)):
            flash("Only CSV files are allowed.")
            return redirect(request.url)

        turo_filename = secure_filename(turo_file.filename)
        linkt_filename = secure_filename(linkt_file.filename)

        turo_path = os.path.join(UPLOAD_FOLDER, turo_filename)
        linkt_path = os.path.join(UPLOAD_FOLDER, linkt_filename)

        turo_file.save(turo_path)
        linkt_file.save(linkt_path)

        try:
            matched_rows, summary_rows, detailed_rows = process_files(turo_path, linkt_path)
        except Exception as e:
            flash(str(e))
            return redirect(request.url)

        report_path = os.path.join(UPLOAD_FOLDER, "billing_report.csv")
        report_fields = [
            "trip_id", "guest_name", "turo_lpn", "turo_tag_number",
            "linkt_lpn", "linkt_tag_number", "toll_no",
            "linkt_start_datetime", "linkt_end_datetime",
            "toll_details", "toll_amount", "total_charge", "status"
        ]
        summary_fields = [
            "trip_id", "guest_name", "toll_count", "toll_amount", "total_charge"
        ]

        write_csv(report_path, report_fields, detailed_rows)
        write_csv(os.path.join(UPLOAD_FOLDER, "billing_summary.csv"), summary_fields, summary_rows)

        LAST_REPORT_PATH = report_path

        return render_template(
            "results.html",
            report_rows=matched_rows,
            summary_rows=summary_rows,
            report_count=len(matched_rows),
        )

    return render_template("index.html")


@app.route("/download")
def download():
    global LAST_REPORT_PATH
    if LAST_REPORT_PATH and os.path.exists(LAST_REPORT_PATH):
        return send_file(LAST_REPORT_PATH, as_attachment=True, download_name="billing_report.csv")
    flash("No report available yet. Please process files first.")
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80, debug=False)
