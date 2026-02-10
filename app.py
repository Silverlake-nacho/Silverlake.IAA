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
import time
from typing import List, Optional, Tuple
import os
import pyodbc


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
IAA_INSURANCE_COMPANY_NAME = os.getenv("IAA_INSURANCE_COMPANY_NAME", "IAA")

DATE_FIELD_CONFIG = {
    "recovered": {
        "expression": "sr.DateRecovered",
        "label": "Date Recovered",
    },
    "entered": {
        "expression": "v.DateEntered",
        "label": "Date Entered",
    },
}


def resolve_date_field(date_field: str):
    return DATE_FIELD_CONFIG.get(date_field, DATE_FIELD_CONFIG["entered"]), (
        date_field if date_field in DATE_FIELD_CONFIG else "entered"
    )

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


def fetch_table_columns(cursor, table_name: str) -> List[str]:
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?
        """,
        (table_name,),
    )
    return [row[0] for row in cursor.fetchall()]


def resolve_vehicle_note_relationships(cursor):
    table_columns = {
        "CT_VehicleNotes": fetch_table_columns(cursor, "CT_VehicleNotes"),
        "CT_VehicleNoteBodies": fetch_table_columns(cursor, "CT_VehicleNoteBodies"),
    }
    notes_set = set(table_columns["CT_VehicleNotes"])
    bodies_set = set(table_columns["CT_VehicleNoteBodies"])

    vehicle_fk = next(
        (
            candidate
            for candidate in ["CtVehicleId", "VehicleId", "CTVehicleId", "Id"]
            if candidate in notes_set
        ),
        None,
    )
    body_fk = next(
        (
            candidate
            for candidate in ["CtVehicleNoteId", "VehicleNoteId", "CTVehicleNoteId", "Id"]
            if candidate in bodies_set
        ),
        None,
    )
    body_text_column = next(
        (
            candidate
            for candidate in ["Body", "NoteBody", "Comment", "Contents", "Value", "Text"]
            if candidate in bodies_set
        ),
        None,
    )
    return {
        "vehicle_fk": vehicle_fk,
        "body_fk": body_fk,
        "body_text_column": body_text_column,
        "notes_columns": table_columns["CT_VehicleNotes"],
        "bodies_columns": table_columns["CT_VehicleNoteBodies"],
    }


def fetch_atlas_vehicle_counts_by_insurance(start_date: date, end_date: date, date_field: str):
    date_field_config, _ = resolve_date_field(date_field)
    date_expression = date_field_config["expression"]
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            query = f"""
                SELECT
                    ic.Name,
                    COUNT(*) AS VehicleCount
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                INNER JOIN InsuranceBranches ib ON v.InsuranceBranchId = ib.Id
                INNER JOIN InsuranceCompanies ic ON ib.InsuranceCompanyId = ic.Id
                WHERE {date_expression} >= ?
                  AND {date_expression} < ?
                  AND ic.Name = ?
                GROUP BY ic.Name
                ORDER BY VehicleCount DESC, ic.Name
            """
            cur.execute(query, (start_date, end_date, IAA_INSURANCE_COMPANY_NAME))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return database_name, rows
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")


def fetch_atlas_vehicle_counts_by_branch(start_date: date, end_date: date, date_field: str):
    date_field_config, _ = resolve_date_field(date_field)
    date_expression = date_field_config["expression"]
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            query = f"""
                SELECT
                    ib.Name AS InsuranceBranch,
                    COUNT(*) AS VehicleCount
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                INNER JOIN InsuranceBranches ib ON v.InsuranceBranchId = ib.Id
                INNER JOIN InsuranceCompanies ic ON ib.InsuranceCompanyId = ic.Id
                LEFT JOIN ContractGroups cg ON ic.ContractGroupId = cg.Id
                WHERE {date_expression} >= ?
                  AND {date_expression} < ?
                  AND ic.Name = ?
                GROUP BY ib.Name
                ORDER BY VehicleCount DESC, InsuranceBranch
            """
            cur.execute(query, (start_date, end_date, IAA_INSURANCE_COMPANY_NAME))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return database_name, rows
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")


def fetch_atlas_vehicle_counts_by_status(start_date: date, end_date: date, date_field: str):
    date_field_config, _ = resolve_date_field(date_field)
    date_expression = date_field_config["expression"]
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            query = f"""
                SELECT
                    COALESCE(stc.Name, 'Unknown') AS Status,
                    COUNT(*) AS VehicleCount
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                INNER JOIN InsuranceBranches ib ON v.InsuranceBranchId = ib.Id
                INNER JOIN InsuranceCompanies ic ON ib.InsuranceCompanyId = ic.Id
                LEFT JOIN StatusColors stc ON v.StatusEnum = stc.Id
                WHERE {date_expression} >= ?
                  AND {date_expression} < ?
                  AND ic.Name = ?
                GROUP BY COALESCE(stc.Name, 'Unknown')
                ORDER BY VehicleCount DESC, Status
            """
            cur.execute(query, (start_date, end_date, IAA_INSURANCE_COMPANY_NAME))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return database_name, rows
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")


def fetch_atlas_vehicle_details_by_insurance(start_date: date, end_date: date, date_field: str):
    detail_limit = max(1, min(ATLAS_DETAIL_LIMIT, 5000))
    date_field_config, _ = resolve_date_field(date_field)
    date_expression = date_field_config["expression"]
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            note_relationships = resolve_vehicle_note_relationships(cur)
            vehicle_fk = note_relationships.get("vehicle_fk")
            has_comments_expression = (
                f"CASE WHEN EXISTS (SELECT 1 FROM CT_VehicleNotes vn WHERE vn.{vehicle_fk} = v.Id AND ISNULL(vn.IsSendToWeb, 0) = 0) THEN 'YES' ELSE 'NO' END AS HasComments"
                if vehicle_fk
                else "'NO' AS HasComments"
            )
            query = f"""
                SELECT
                    TOP ({detail_limit})
                    v.GroupRef AS [IAA Id],
                    v.Id AS [SLK Id],
                    v.RegNo AS Registration,
                    CASE WHEN v.DateEntered IS NULL THEN '' ELSE CONVERT(varchar(10), v.DateEntered, 105) + ' ' + CONVERT(varchar(8), v.DateEntered, 108) END AS DateEntered,
                    stc.Name AS Status,
                    {has_comments_expression},
                    m.Name AS Manufacturer,
                    mg.Name AS Model,
                    dd.TrimLevel,
                    col.Name AS [Colour],
                    ib.Name AS InsuranceBranch,
                    ic.Name AS InsuranceCompany,
                    c.Code AS Category_Code,
                    c.Name AS Category,
                    CASE WHEN sr.DateRecovered IS NULL THEN '' ELSE CONVERT(varchar(10), sr.DateRecovered, 105) + ' ' + CONVERT(varchar(8), sr.DateRecovered, 108) END AS [Date Recovered],
                    CASE WHEN sc.DateCleared IS NULL THEN '' ELSE CONVERT(varchar(10), sc.DateCleared, 105) + ' ' + CONVERT(varchar(8), sc.DateCleared, 108) END AS [Date Cleared],
                    CASE WHEN scn.DateCancelled IS NULL THEN '' ELSE CONVERT(varchar(10), scn.DateCancelled, 105) + ' ' + CONVERT(varchar(8), scn.DateCancelled, 108) END AS [Date Cancelled]
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                LEFT JOIN PartDataManufacturers m ON v.ManufacturerId = m.Id
                LEFT JOIN PartDataModelGroups mg ON v.ModelGroupId = mg.Id
                LEFT JOIN PartDataDerivativeDetails dd ON v.DerivativeId = dd.Id
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
                WHERE {date_expression} >= ?
                  AND {date_expression} < ?
                  AND ic.Name = ?
                ORDER BY v.Id DESC
            """
            cur.execute(query, (start_date, end_date, IAA_INSURANCE_COMPANY_NAME))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            cur.close()
            conn.close()
            return database_name, columns, rows
        except Exception as exc:
            last_error = exc
    raise last_error if last_error else RuntimeError("No Atlas database names configured.")


def fetch_atlas_vehicle_search_by_insurance(search_field: str, search_query: str):
    detail_limit = max(1, min(ATLAS_DETAIL_LIMIT, 5000))
    search_column_map = {
        "IAA Id": "CAST(v.GroupRef AS nvarchar(255))",
        "SLK Id": "CAST(v.Id AS nvarchar(255))",
        "Registration": "CAST(v.RegNo AS nvarchar(255))",
    }
    search_column = search_column_map.get(search_field, search_column_map["IAA Id"])
    query_value = f"%{search_query}%"

    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()
            note_relationships = resolve_vehicle_note_relationships(cur)
            vehicle_fk = note_relationships.get("vehicle_fk")
            has_comments_expression = (
                f"CASE WHEN EXISTS (SELECT 1 FROM CT_VehicleNotes vn WHERE vn.{vehicle_fk} = v.Id AND ISNULL(vn.IsSendToWeb, 0) = 0) THEN 'YES' ELSE 'NO' END AS HasComments"
                if vehicle_fk
                else "'NO' AS HasComments"
            )
            query = f"""
                SELECT
                    TOP ({detail_limit})
                    v.GroupRef AS [IAA Id],
                    v.Id AS [SLK Id],
                    v.RegNo AS Registration,
                    CASE WHEN v.DateEntered IS NULL THEN '' ELSE CONVERT(varchar(10), v.DateEntered, 105) + ' ' + CONVERT(varchar(8), v.DateEntered, 108) END AS DateEntered,
                    stc.Name AS Status,
                    {has_comments_expression},
                    m.Name AS Manufacturer,
                    mg.Name AS Model,
                    dd.TrimLevel,
                    col.Name AS [Colour],
                    ib.Name AS InsuranceBranch,
                    ic.Name AS InsuranceCompany,
                    c.Code AS Category_Code,
                    c.Name AS Category,
                    CASE WHEN sr.DateRecovered IS NULL THEN '' ELSE CONVERT(varchar(10), sr.DateRecovered, 105) + ' ' + CONVERT(varchar(8), sr.DateRecovered, 108) END AS [Date Recovered],
                    CASE WHEN sc.DateCleared IS NULL THEN '' ELSE CONVERT(varchar(10), sc.DateCleared, 105) + ' ' + CONVERT(varchar(8), sc.DateCleared, 108) END AS [Date Cleared],
                    CASE WHEN scn.DateCancelled IS NULL THEN '' ELSE CONVERT(varchar(10), scn.DateCancelled, 105) + ' ' + CONVERT(varchar(8), scn.DateCancelled, 108) END AS [Date Cancelled]
                FROM CT_Vehicles v
                LEFT JOIN SalvageRecoveries sr ON v.SalvageRecoveryId = sr.Id
                LEFT JOIN PartDataManufacturers m ON v.ManufacturerId = m.Id
                LEFT JOIN PartDataModelGroups mg ON v.ModelGroupId = mg.Id
                LEFT JOIN PartDataDerivativeDetails dd ON v.DerivativeId = dd.Id
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
                LEFT JOIN PartDataColours col ON v.ColourId = col.Id
                LEFT JOIN StatusColors stc ON v.StatusEnum = stc.Id
                WHERE ic.Name = ?
                  AND {search_column} LIKE ?
                ORDER BY v.Id DESC
            """
            cur.execute(query, (IAA_INSURANCE_COMPANY_NAME, query_value))
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


def serialize_detail_rows(rows) -> List[List[object]]:
    serialized_rows: List[List[object]] = []
    for row in rows:
        serialized_row = []
        for value in row:
            if value is None:
                serialized_row.append("")
            elif isinstance(value, (datetime, date)):
                serialized_row.append(value.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                serialized_row.append(value)
        serialized_rows.append(serialized_row)
    return serialized_rows


def serialize_detail_rows(rows) -> List[List[object]]:
    serialized_rows: List[List[object]] = []
    for row in rows:
        serialized_row = []
        for value in row:
            if value is None:
                serialized_row.append("")
            elif isinstance(value, (datetime, date)):
                serialized_row.append(value.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                serialized_row.append(value)
        serialized_rows.append(serialized_row)
    return serialized_rows


def build_vehicle_stats_context(
    filter_type: str,
    start_date_str: Optional[str],
    end_date_str: Optional[str],
    group_mode: str,
    date_field: str,
):
    start_date, end_date = parse_date_filter(filter_type, start_date_str, end_date_str)
    date_range_label = describe_date_range(filter_type, start_date, end_date)
    date_field_config, resolved_date_field = resolve_date_field(date_field)

    resolved_group_mode = "branch" if group_mode in {"branch", "contract"} else "status"
    if resolved_group_mode == "branch":
        database_name, rows = fetch_atlas_vehicle_counts_by_branch(
            start_date, end_date, resolved_date_field
        )
    else:
        database_name, rows = fetch_atlas_vehicle_counts_by_status(
            start_date, end_date, resolved_date_field
        )
    details_db_name, detail_columns, detail_rows = fetch_atlas_vehicle_details_by_insurance(
        start_date, end_date, resolved_date_field
    )
    detail_rows = serialize_detail_rows(detail_rows)
    if resolved_group_mode == "branch":
        filtered_rows = [(row[0], int(row[1])) for row in rows]
        entity_label = "Insurance Branch"
    else:
        filtered_rows = [(row[0], int(row[1])) for row in rows]
        entity_label = "Status"

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
        "database_name": database_name or details_db_name,
        "detail_columns": detail_columns,
        "detail_rows": detail_rows,
        "group_mode": resolved_group_mode,
        "entity_label": entity_label,
        "chart_title_base": chart_title_base,
        "date_field": resolved_date_field,
        "date_field_label": date_field_config["label"],
        "insurance_company_name": IAA_INSURANCE_COMPANY_NAME,
    }


def get_atlas_vehicle_notes(vehicle_id: int):
    last_error = None
    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()

            notes = None
            for vehicle_fk in ["CtVehicleId", "VehicleId", "CTVehicleId", "Id"]:
                try:
                    cur.execute(
                        f"""
                        SELECT TOP (100)
                            vn.Id,
                            vn.Subject,
                            vn.UserName,
                            CAST(vn.DateCreated AS datetime2) AS DateCreated,
                            vn.IsSendToWeb
                        FROM CT_VehicleNotes vn
                        WHERE vn.{vehicle_fk} = ?
                          AND ISNULL(vn.IsSendToWeb, 0) = 0
                        ORDER BY vn.Id DESC
                        """,
                        (vehicle_id,),
                    )
                    rows = cur.fetchall()
                    notes = []
                    for row in rows:
                        date_created = row[3]
                        notes.append(
                            {
                                "Id": row[0],
                                "Subject": row[1],
                                "UserName": row[2],
                                "DateCreated": (
                                    date_created.strftime("%Y-%m-%d %H:%M:%S")
                                    if isinstance(date_created, datetime)
                                    else date_created
                                ),
                                "IsSendToWeb": row[4],
                            }
                        )
                    break
                except Exception:
                    notes = None

            cur.close()
            conn.close()

            if notes is not None:
                return notes
        except Exception as exc:
            last_error = exc

    if last_error:
        return []
    return []


def get_atlas_vehicle_note_body(note_id: int):
    fk_candidates = ["CtVehicleNoteId", "VehicleNoteId", "CTVehicleNoteId", "Id"]
    body_text_candidates = ["Body", "NoteBody", "Comment", "Contents", "Value", "Text"]

    for database_name in _get_atlas_db_name_candidates():
        try:
            conn = get_atlas_db_connection(database_name)
            cur = conn.cursor()

            for body_fk in fk_candidates:
                for body_text_column in body_text_candidates:
                    try:
                        cur.execute(
                            f"""
                            SELECT TOP (1) vnb.{body_text_column}
                            FROM CT_VehicleNoteBodies vnb
                            WHERE vnb.{body_fk} = ?
                            ORDER BY vnb.Id DESC
                            """,
                            (note_id,),
                        )
                        row = cur.fetchone()
                        if row is not None:
                            cur.close()
                            conn.close()
                            return {"content": row[0] or ""}
                    except Exception:
                        continue

            cur.close()
            conn.close()
        except Exception:
            continue

    return {"content": ""}


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
    allowed_routes = {"login", "static", "db_check"}
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
    group_mode = request.args.get("group", "status")
    date_field = request.args.get("date_field", "entered")
    live_enabled = str(request.args.get("live", "")).lower() in {"1", "true", "yes", "on"}

    error_message = None
    last_error = None
    start_date, end_date = parse_date_filter(filter_type, start_date_str, end_date_str)
    entity_label = "Insurance Branch" if group_mode in {"branch", "contract"} else "Status"
    try:
        context = build_vehicle_stats_context(
            filter_type, start_date_str, end_date_str, group_mode, date_field
        )
    except Exception as exc:
        last_error = exc
        context = {
            "filter_type": filter_type,
            "start_date": start_date,
            "end_date": end_date,
            "date_range_label": describe_date_range(filter_type, start_date, end_date),
            "rows": [],
            "sum_total": 0,
            "chart_labels": [],
            "chart_values": [],
            "database_name": None,
            "detail_columns": [],
            "detail_rows": [],
            "group_mode": group_mode,
            "entity_label": entity_label,
            "chart_title_base": f"Vehicles by {entity_label}",
            "date_field": "entered",
            "date_field_label": DATE_FIELD_CONFIG["entered"]["label"],
            "insurance_company_name": IAA_INSURANCE_COMPANY_NAME,
        }

    if last_error is not None:
        error_message = f"Unable to load vehicle stats: {last_error}"

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
    group_mode = request.args.get("group", "status")
    date_field = request.args.get("date_field", "entered")
    
    context = build_vehicle_stats_context(
        filter_type, start_date_str, end_date_str, group_mode, date_field
    )

    payload = {
        "date_range_label": context["date_range_label"],
        "rows": [
            {"label": row[0], "total": float(row[1])} for row in context["rows"]
        ],
        "sum_total": context["sum_total"],
        "chart_labels": context["chart_labels"],
        "chart_values": context["chart_values"],
        "detail_columns": context.get("detail_columns", []),
        "detail_rows": context.get("detail_rows", []),
        "entity_label": context.get("entity_label", "Status"),
        "chart_title_base": context.get("chart_title_base", "Vehicles by Status"),
        "date_field": context.get("date_field", "entered"),
        "date_field_label": context.get(
            "date_field_label", DATE_FIELD_CONFIG["entered"]["label"]
        ),
    }
    return jsonify(payload)


@app.route("/vehicle_stats/search", methods=["GET"])
def vehicle_stats_search():
    search_field = request.args.get("field", "IAA Id")
    search_query = (request.args.get("q") or "").strip()

    if not search_query:
        return jsonify({"detail_columns": [], "detail_rows": []})

    _, detail_columns, detail_rows = fetch_atlas_vehicle_search_by_insurance(
        search_field, search_query
    )

    return jsonify(
        {
            "detail_columns": detail_columns,
            "detail_rows": serialize_detail_rows(detail_rows),
        }
    )


@app.route("/vehicle_notes/<int:vehicle_id>", methods=["GET"])
def vehicle_notes(vehicle_id: int):
    notes = get_atlas_vehicle_notes(vehicle_id)
    return jsonify({"notes": notes})


@app.route("/vehicle_note_body/<int:note_id>", methods=["GET"])
def vehicle_note_body(note_id: int):
    body = get_atlas_vehicle_note_body(note_id)
    return jsonify(body)

@app.route("/db_check", methods=["GET"])
def db_check():
    start_ts = time.time()
    try:
        database_name, _ = fetch_atlas_vehicle_counts_by_insurance(date.today(), date.today(), "recovered")
        conn = get_atlas_db_connection(database_name)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        elapsed_ms = int((time.time() - start_ts) * 1000)
        return jsonify({"status": "ok", "database": database_name, "elapsed_ms": elapsed_ms})
    except Exception as exc:
        elapsed_ms = int((time.time() - start_ts) * 1000)
        return jsonify({"status": "error", "error": str(exc), "elapsed_ms": elapsed_ms}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
