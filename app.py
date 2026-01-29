from flask import (
    Flask,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple
import json
import os
import pyodbc


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATS_EXCLUSIONS_PATH = os.path.join(BASE_DIR, "stats_exclusions.json")

app = Flask(__name__)
app.secret_key = os.environ["FLASK_SECRET_KEY"]

USERS = {
    "admin": "Silverlake1!",
    "paul": "Silverlake1!",
    "morgan": "Silverlake1!",
    "cain": "Silverlake1!",
    "stores": "stores",
    "Stores": "stores",
    "Josh": "Silverlake1!",
    "Casper": "Silverlake1!",
    "carlo": "Silverlake1!",
    "nacho": "Silverlake1!",
}

ATLAS_DB_HOST = os.environ["ATLAS_DB_HOST"]
ATLAS_DB_PORT = int(os.getenv("ATLAS_DB_PORT", "1433"))
ATLAS_DB_NAME = os.getenv("ATLAS_DB_NAME", "silverlake")
ATLAS_DB_NAMES = os.getenv("ATLAS_DB_NAMES", "")
ATLAS_DB_USER = os.environ["ATLAS_DB_USER"]
ATLAS_DB_PASSWORD = os.environ["ATLAS_DB_PASSWORD"]
ATLAS_DB_DRIVER = os.getenv("ATLAS_DB_DRIVER", "ODBC Driver 18 for SQL Server")
ATLAS_DB_ENCRYPT = os.getenv("ATLAS_DB_ENCRYPT", "yes")
ATLAS_DB_TRUST_CERT = os.getenv("ATLAS_DB_TRUST_CERT", "yes")
ATLAS_DETAIL_LIMIT = int(os.getenv("ATLAS_DETAIL_LIMIT", "500"))

def _load_json_file(path: str, fallback):
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return fallback


def _normalise_exclusion_store(raw_data) -> dict:
    if not isinstance(raw_data, dict):
        return {}
    normalised = {}
    for user, payload in raw_data.items():
        if isinstance(payload, dict):
            normalised[user] = {
                "insurance_company": [
                    str(item) for item in payload.get("insurance_company", [])
                ],
            }
    return normalised


def load_stats_exclusions(user: Optional[str], dimension: str) -> List[str]:
    raw_data = _load_json_file(STATS_EXCLUSIONS_PATH, {})
    store = _normalise_exclusion_store(raw_data)
    key = user or "__default__"
    return store.get(key, {}).get(dimension, [])


def persist_stats_exclusions(user: Optional[str], dimension: str, exclusions: List[str]) -> None:
    raw_data = _load_json_file(STATS_EXCLUSIONS_PATH, {})
    store = _normalise_exclusion_store(raw_data)
    key = user or "__default__"
    user_entry = store.setdefault(key, {"insurance_company": []})
    user_entry[dimension] = [str(item) for item in exclusions]
    with open(STATS_EXCLUSIONS_PATH, "w", encoding="utf-8") as handle:
        json.dump(store, handle)


def _get_atlas_db_name_candidates() -> List[str]:
    explicit_names = [name.strip() for name in ATLAS_DB_NAMES.split(",") if name.strip()]
    if ATLAS_DB_NAME:
        return [ATLAS_DB_NAME, *explicit_names]
    return explicit_names


def get_atlas_db_connection(database_name: str):
    available_drivers = [driver.strip() for driver in pyodbc.drivers()]
    driver = ATLAS_DB_DRIVER
    if driver not in available_drivers:
        preferred_drivers = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
        ]
        fallback = next(
            (candidate for candidate in preferred_drivers if candidate in available_drivers),
            None,
        )
        if not fallback:
            fallback = next(
                (candidate for candidate in available_drivers if "SQL Server" in candidate),
                None,
            )
        if fallback:
            driver = fallback
        else:
            available_list = ", ".join(available_drivers) or "None found"
            raise RuntimeError(
                "ODBC driver not available. "
                f"Requested '{ATLAS_DB_DRIVER}'. "
                f"Available drivers: {available_list}. "
                "Install the SQL Server ODBC driver or set ATLAS_DB_DRIVER to a valid name."
            )
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={ATLAS_DB_HOST},{ATLAS_DB_PORT};"
        f"DATABASE={database_name};"
        f"UID={ATLAS_DB_USER};"
        f"PWD={ATLAS_DB_PASSWORD};"
        f"Encrypt={ATLAS_DB_ENCRYPT};"
        f"TrustServerCertificate={ATLAS_DB_TRUST_CERT};"
    )
    return pyodbc.connect(conn_str, timeout=150)


def fetch_atlas_vehicle_counts_by_insurance(start_date: date, end_date: date):
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            query = """
                SELECT
                    ic.Name,
                    COUNT(*) AS VehicleCount
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                INNER JOIN InsuranceBranches ib ON v.InsuranceBranchId = ib.Id
                INNER JOIN InsuranceCompanies ic ON ib.InsuranceCompanyId = ic.Id
                WHERE CAST(sr.DateRecovered AS datetime2) >= ?
                  AND CAST(sr.DateRecovered AS datetime2) < ?
                GROUP BY ic.Name
                ORDER BY VehicleCount DESC, ic.Name
            """
            cur.execute(query, (start_date, end_date))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return database_name, rows
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")


def fetch_atlas_vehicle_counts_by_contract_group(start_date: date, end_date: date):
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            query = """
                SELECT
                    COALESCE(cg.Name, 'Unassigned') AS ContractGroup,
                    ic.Name AS InsuranceCompany,
                    COUNT(*) AS VehicleCount
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                INNER JOIN InsuranceBranches ib ON v.InsuranceBranchId = ib.Id
                INNER JOIN InsuranceCompanies ic ON ib.InsuranceCompanyId = ic.Id
                LEFT JOIN ContractGroups cg ON ic.ContractGroupId = cg.Id
                WHERE CAST(sr.DateRecovered AS datetime2) >= ?
                  AND CAST(sr.DateRecovered AS datetime2) < ?
                GROUP BY COALESCE(cg.Name, 'Unassigned'), ic.Name
                ORDER BY VehicleCount DESC, ContractGroup
            """
            cur.execute(query, (start_date, end_date))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return database_name, rows
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")


def fetch_atlas_vehicle_details_by_insurance(start_date: date, end_date: date):
    detail_limit = max(1, min(ATLAS_DETAIL_LIMIT, 5000))
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            query = f"""
                SELECT
                    TOP ({detail_limit})
                    v.Id,
                    v.RegNo AS Registration,
                    CAST(v.DateEntered AS datetime2) AS DateEntered,
                    m.Name AS Manufacturer,
                    mg.Name AS Model,
                    dd.TrimLevel,
                    col.Name AS colour,
                    dm.Name AS Derivative,
                    ib.Name AS InsuranceBranch,
                    ic.Name AS InsuranceCompany,
                    c.Code AS Category_Code,
                    c.Name AS Category,
                    CAST(v.DateRecoveredStart AS datetime2) AS [Date Recovered START],
                    CAST(v.DateRecoveredEnd AS datetime2) AS [Date Recovered END],
                    CAST(sr.DateRecovered AS datetime2) AS [Date Recovered],
                    CAST(sc.DateCleared AS datetime2) AS DateCleared,
                    CAST(scn.DateCancelled AS datetime2) AS DateCancelled,
                    CAST(ss.DateSold AS datetime2) AS DateSold,
                    ss.IncVAT AS Sold_price,
                    stc.Name AS Status
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                LEFT JOIN PartDataManufacturers m ON v.ManufacturerId = m.Id
                LEFT JOIN PartDataModelGroups mg ON v.ModelGroupId = mg.Id
                LEFT JOIN PartDataDerivativeDetails dd ON v.DerivativeId = dd.Id
                LEFT JOIN PartDataModels dm ON v.DerivativeId = dm.Id
                INNER JOIN InsuranceBranches ib ON v.InsuranceBranchId = ib.Id
                INNER JOIN InsuranceCompanies ic ON ib.InsuranceCompanyId = ic.Id
                LEFT JOIN Categories c ON v.CategoryId = c.Id
                OUTER APPLY (
                    SELECT TOP (1) sc.DateCleared
                    FROM SalvageClears sc
                    WHERE sc.CtVehicleId = v.Id
                    ORDER BY sc.DateCleared DESC
                ) sc
                OUTER APPLY (
                    SELECT TOP (1) scn.DateCancelled
                    FROM SalvagesCancelled scn
                    WHERE scn.CtVehicleId = v.Id
                    ORDER BY scn.DateCancelled DESC
                ) scn
                OUTER APPLY (
                    SELECT TOP (1) ss.DateSold, ss.IncVAT
                    FROM SalvageSales ss
                    WHERE ss.CtVehicleId = v.Id
                    ORDER BY ss.DateSold DESC
                ) ss
                LEFT JOIN PartDataColours col ON v.ColourId = col.Id
                LEFT JOIN StatusColors stc ON v.StatusEnum = stc.Id
                WHERE CAST(sr.DateRecovered AS datetime2) >= ?
                  AND CAST(sr.DateRecovered AS datetime2) < ?
                ORDER BY v.Id DESC
            """
            cur.execute(query, (start_date, end_date))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            cur.close()
            conn.close()
            return database_name, columns, rows
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")


def parse_date_filter(
    filter_type: str, start_date_str: Optional[str] = None, end_date_str: Optional[str] = None
) -> Tuple[date, date]:
    today = date.today()

    if filter_type == "today":
        return today, today + timedelta(days=1)
    if filter_type == "yesterday":
        return today - timedelta(days=1), today
    if filter_type == "this_week":
        start_of_week = today - timedelta(days=today.weekday())
        return start_of_week, start_of_week + timedelta(days=7)
    if filter_type == "last_week":
        start_of_week = today - timedelta(days=today.weekday())
        last_week_start = start_of_week - timedelta(days=7)
        return last_week_start, start_of_week
    if filter_type == "this_month":
        return today.replace(day=1), (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    if filter_type == "last_month":
        first_this_month = today.replace(day=1)
        last_month_end = first_this_month - timedelta(days=1)
        return last_month_end.replace(day=1), first_this_month
    if filter_type == "this_year":
        return date(today.year, 1, 1), date(today.year + 1, 1, 1)
    if filter_type == "last_year":
        return date(today.year - 1, 1, 1), date(today.year, 1, 1)
    if filter_type == "custom" and start_date_str and end_date_str:
        start_date = date.fromisoformat(start_date_str)
        end_date = date.fromisoformat(end_date_str) + timedelta(days=1)
        return start_date, end_date

    return today, today + timedelta(days=1)


def describe_date_range(filter_type: str, start_date: date, end_date: date) -> str:
    labels = {
        "today": "Today",
        "yesterday": "Yesterday",
        "this_week": "This Week",
        "last_week": "Last Week",
        "this_month": "This Month",
        "last_month": "Last Month",
        "this_year": "This Year",
        "last_year": "Last Year",
        "custom": "Custom",
    }
    inclusive_end = end_date - timedelta(days=1)

    def format_date(value: date) -> str:
        return value.strftime("%d/%m/%Y")

    if start_date == inclusive_end:
        range_text = format_date(start_date)
    else:
        range_text = f"{format_date(start_date)} - {format_date(inclusive_end)}"

    label = labels.get(filter_type, "Custom")
    return f"{label} ({range_text})"


def build_vehicle_stats_context(
    filter_type: str,
    start_date_str: Optional[str],
    end_date_str: Optional[str],
    exclude_args: List[str],
    group_mode: str,
):
    start_date, end_date = parse_date_filter(filter_type, start_date_str, end_date_str)
    date_range_label = describe_date_range(filter_type, start_date, end_date)

    resolved_group_mode = "contract" if group_mode == "contract" else "company"
    if resolved_group_mode == "contract":
        database_name, rows = fetch_atlas_vehicle_counts_by_contract_group(
            start_date, end_date
        )
    else:
        database_name, rows = fetch_atlas_vehicle_counts_by_insurance(start_date, end_date)
    details_db_name, detail_columns, detail_rows = fetch_atlas_vehicle_details_by_insurance(
        start_date, end_date
    )
    current_user = session.get("username")
    default_exclusions = load_stats_exclusions(current_user, "insurance_company")
    excluded_companies = exclude_args or default_exclusions

    if resolved_group_mode == "contract":
        group_totals: dict[str, int] = {}
        all_companies = sorted({row[1] for row in rows})
        for contract_group, company, count in rows:
            if company in excluded_companies:
                continue
            group_totals[contract_group] = group_totals.get(contract_group, 0) + int(
                count
            )
        filtered_rows = sorted(
            group_totals.items(),
            key=lambda item: (-item[1], item[0]),
        )
        entity_label = "Contract Group"
    else:
        filtered_rows = [(row[0], int(row[1])) for row in rows if row[0] not in excluded_companies]
        all_companies = sorted({row[0] for row in rows})
        entity_label = "Insurance Company"

    sum_total = sum(row[1] for row in filtered_rows)

    chart_labels = [row[0] for row in filtered_rows]
    chart_values = [float(row[1]) for row in filtered_rows]
    chart_title_base = f"Vehicles by {entity_label}"

    return {
        "filter_type": filter_type,
        "start_date": start_date,
        "end_date": end_date,
        "date_range_label": date_range_label,
        "rows": filtered_rows,
        "sum_total": sum_total,
        "chart_labels": chart_labels,
        "chart_values": chart_values,
        "all_companies": all_companies,
        "excluded_companies": excluded_companies,
        "database_name": database_name or details_db_name,
        "detail_columns": detail_columns,
        "detail_rows": detail_rows,
        "group_mode": resolved_group_mode,
        "entity_label": entity_label,
        "chart_title_base": chart_title_base,
    }


@app.context_processor
def inject_current_user():
    return {"current_user": session.get("username")}


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    next_url = request.args.get("next") or request.form.get("next")
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        if username in USERS and USERS[username] == password:
            session["logged_in"] = True
            session["login_time"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            session["username"] = username
            if next_url:
                return redirect(next_url)
            return redirect(url_for("vehicle_stats"))
        error = "Invalid Credentials. Please try again."
    return render_template("login.html", error=error, next_url=next_url)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.before_request
def require_login():
    allowed_routes = {"login", "static"}
    if request.endpoint not in allowed_routes and not session.get("logged_in"):
        next_url = request.url
        return redirect(url_for("login", next=next_url))
    if session.get("logged_in"):
        login_time = session.get("login_time")
        if login_time:
            login_time = datetime.strptime(login_time, "%Y-%m-%d %H:%M:%S")
            if datetime.utcnow() - login_time > timedelta(hours=24):
                session.clear()
                return redirect(url_for("login"))


@app.route("/")
def index():
    return redirect(url_for("vehicle_stats"))


@app.route("/vehicle_stats", methods=["GET"])
def vehicle_stats():
    filter_type = request.args.get("filter", "today")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    excluded_args = request.args.getlist("exclude")
    group_mode = request.args.get("group", "company")
    live_enabled = str(request.args.get("live", "")).lower() in {"1", "true", "yes", "on"}

    error_message = None
    start_date, end_date = parse_date_filter(filter_type, start_date_str, end_date_str)
    entity_label = "Contract Group" if group_mode == "contract" else "Insurance Company"
    if live_enabled:
        try:
            context = build_vehicle_stats_context(
                filter_type, start_date_str, end_date_str, excluded_args, group_mode
            )
        except Exception as exc:
            context = {
                "filter_type": filter_type,
                "start_date": start_date,
                "end_date": end_date,
                "date_range_label": describe_date_range(filter_type, start_date, end_date),
                "rows": [],
                "sum_total": 0,
                "chart_labels": [],
                "chart_values": [],
                "all_companies": [],
                "excluded_companies": excluded_args,
                "database_name": None,
                "detail_columns": [],
                "detail_rows": [],
                "group_mode": group_mode,
                "entity_label": entity_label,
                "chart_title_base": f"Vehicles by {entity_label}",
            }
            error_message = f"Unable to load vehicle stats: {exc}"
    else:
        context = {
            "filter_type": filter_type,
            "start_date": start_date,
            "end_date": end_date,
            "date_range_label": describe_date_range(filter_type, start_date, end_date),
            "rows": [],
            "sum_total": 0,
            "chart_labels": [],
            "chart_values": [],
            "all_companies": [],
            "excluded_companies": excluded_args,
            "database_name": None,
            "detail_columns": [],
            "detail_rows": [],
            "group_mode": group_mode,
            "entity_label": entity_label,
            "chart_title_base": f"Vehicles by {entity_label}",
        }
        error_message = f"Unable to load vehicle stats: {exc}"

    return render_template(
        "vehicle_stats.html",
        **context,
        live_enabled=live_enabled,
        error_message=error_message,
        active_page="vehicle_stats",
    )


@app.route("/vehicle_stats/data", methods=["GET"])
def vehicle_stats_data():
    filter_type = request.args.get("filter", "today")
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    excluded_args = request.args.getlist("exclude")
    group_mode = request.args.get("group", "company")

    context = build_vehicle_stats_context(
        filter_type, start_date_str, end_date_str, excluded_args, group_mode
    )

    detail_rows = []
    for row in context.get("detail_rows", []):
        serialized_row = []
        for value in row:
            if value is None:
                serialized_row.append("")
            elif isinstance(value, (datetime, date)):
                serialized_row.append(value.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                serialized_row.append(value)
        detail_rows.append(serialized_row)

    return jsonify(
        {
            "date_range_label": context["date_range_label"],
            "rows": [
                {"label": row[0], "total": float(row[1])} for row in context["rows"]
            ],
            "sum_total": context["sum_total"],
            "chart_labels": context["chart_labels"],
            "chart_values": context["chart_values"],
            "detail_columns": context.get("detail_columns", []),
            "detail_rows": detail_rows,
            "entity_label": context.get("entity_label", "Insurance Company"),
            "chart_title_base": context.get(
                "chart_title_base", "Vehicles by Insurance Company"
            ),
        }
    )


@app.route("/vehicle_stats/exclusions", methods=["POST"])
def save_vehicle_stats_exclusions():
    filter_type = request.form.get("filter", "today")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    excluded_companies = request.form.getlist("exclude")
    group_mode = request.form.get("group", "company")

    user = session.get("username")
    persist_stats_exclusions(user, "insurance_company", excluded_companies)

    return redirect(
        url_for(
            "vehicle_stats",
            filter=filter_type,
            start_date=start_date,
            end_date=end_date,
            exclude=excluded_companies,
            group=group_mode,
        )
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
