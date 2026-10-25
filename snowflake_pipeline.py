# snowflake_pipeline.py
# By Andy Liu
# Python + Snowflake ETL/ELT pipeline: purchase orders, supplier invoices,
# supplier master, and weather, joined into validated analytics tables.

import os
import re
import csv
from pathlib import Path
import snowflake.connector
import psycopg2

# Snowflake Utilities

def connect_sf(user, pwd, acct, wh, db, schema, role=None):
    return snowflake.connector.connect(
        user=user,
        password=pwd,
        account=acct,
        warehouse=wh,
        database=db,
        schema=schema,
        role=role
    )

def run(cur, sql, note=None):
    cur.execute(sql)
    if note:
        print(f"[SF] {note}")

# small helpers used later
def qid(name: str) -> str:
    return '"' + name.replace('"','') + '"'

def one(cur, sql):
    cur.execute(sql)
    return cur.fetchone()

# Step 1: Purchases CSV -> Snowflake

def load_purchases(cur, csv_dir, stage="stg_purchases"):
    run(cur, f"CREATE STAGE IF NOT EXISTS {stage}", "Stage ensured")
    for file in sorted(Path(csv_dir).glob("*.csv")):
        uri = file.resolve().as_uri()
        cur.execute(f"PUT '{uri}' @{stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    print(f"[PUT] Uploaded CSVs from {csv_dir}")

    run(cur, "DROP TABLE IF EXISTS purchases_raw")
    cur.execute(
        "CREATE TABLE purchases_raw ("
        "po_id STRING, "
        "line_id NUMBER, "
        "qty_received NUMBER, "
        "unit_price NUMBER, "
        "order_dt DATE)"
    )

    cur.execute(
        f"COPY INTO purchases_raw "
        f"FROM ("
        f"    SELECT $1::STRING, $13::NUMBER, $17::NUMBER, $19::NUMBER, TO_DATE($3,'MM/DD/YYYY') "
        f"    FROM @{stage}"
        f") "
        f"FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"') "
        f"ON_ERROR=CONTINUE"
    )
    print("[LOAD] purchases_raw populated")

# Step 2: Totals View

def build_purchase_totals(cur):
    cur.execute(
        "CREATE OR REPLACE VIEW purchase_totals AS "
        "SELECT po_id, MIN(order_dt) AS order_dt, "
        "SUM(COALESCE(qty_received,0) * COALESCE(unit_price,0)) AS total_amount "
        "FROM purchases_raw GROUP BY po_id"
    )
    print("[VIEW] purchase_totals created")

# Step 3: Invoices XML -> Snowflake

def load_invoices(cur, xml_file, stage="stg_invoices"):
    run(cur, "CREATE OR REPLACE FILE FORMAT ff_xml TYPE=XML")
    run(cur, f"CREATE OR REPLACE STAGE {stage}")
    cur.execute(f"PUT 'file://{xml_file}' @{stage} OVERWRITE=TRUE")

    run(cur, "CREATE OR REPLACE TABLE invoices_raw (doc VARIANT)")
    cur.execute(f"COPY INTO invoices_raw FROM @{stage} FILE_FORMAT=ff_xml")

    run(cur, "CREATE OR REPLACE TABLE invoices_xml AS SELECT PARSE_XML(TO_VARCHAR(doc)) AS xml_doc FROM invoices_raw")

    cur.execute(
        "CREATE OR REPLACE VIEW invoice_rows AS "
        "SELECT f.index AS idx, f.value AS row_xml "
        "FROM invoices_xml, LATERAL FLATTEN(input=>GET(xml_doc,'$')) f "
        "WHERE TYPEOF(f.value)='XML'"
    )

    run(cur, "DROP TABLE IF EXISTS invoices")
    cur.execute(
        "CREATE TABLE invoices AS "
        "SELECT "
        "GET(XMLGET(row_xml,'SupplierInvoiceNumber'),'$')::STRING AS invoice_id, "
        "GET(XMLGET(row_xml,'PurchaseOrderID'),'$')::STRING AS po_id, "
        "GET(XMLGET(row_xml,'SupplierID'),'$')::STRING AS supplier_id, "
        "TRY_TO_DATE(GET(XMLGET(row_xml,'TransactionDate'),'$')::STRING) AS invoice_date, "
        "TRY_TO_NUMBER(GET(XMLGET(row_xml,'AmountExcludingTax'),'$')::STRING) AS amount_ex_tax "
        "FROM invoice_rows"
    )
    print("[LOAD] invoices table created")

# Step 4: Join Purchases + Invoices

def join_purchases_invoices(cur):
    cur.execute(
        "CREATE OR REPLACE VIEW po_invoice_join AS "
        "SELECT i.invoice_id, i.po_id, i.supplier_id, i.invoice_date, i.amount_ex_tax, p.order_dt, p.total_amount "
        "FROM invoices i INNER JOIN purchase_totals p ON i.po_id = p.po_id"
    )
    print("[JOIN] po_invoice_join created")

# Step 5: Materialized View with Delta

def build_invoice_delta(cur):
    sql = (
        "SELECT j.invoice_id, j.po_id, j.supplier_id, j.invoice_date, "
        "j.amount_ex_tax, j.total_amount, "
        "(COALESCE(j.amount_ex_tax,0) - COALESCE(j.total_amount,0)) AS invoiced_vs_quoted "
        "FROM po_invoice_join j"
    )
    try:
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS purchase_orders_and_invoices")
        cur.execute(f"CREATE MATERIALIZED VIEW purchase_orders_and_invoices AS {sql}")
        print("[VIEW] purchase_orders_and_invoices created as MV")
    except:
        cur.execute("DROP TABLE IF EXISTS purchase_orders_and_invoices")
        cur.execute(f"CREATE TABLE purchase_orders_and_invoices AS {sql}")
        print("[TABLE] purchase_orders_and_invoices created")

# Step 6: Supplier_case from Postgres -> Snowflake

def looks_int(s):    return re.fullmatch(r"[+-]?\d+", s or "") is not None
def looks_float(s):  return re.fullmatch(r"[+-]?\d+\.\d+", s or "") is not None
def looks_date(s):
    return (re.fullmatch(r"\d{4}-\d{2}-\d{2}", s or "") or
            re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", s or "")) is not None

def infer_schema_from_csv(path: Path, sample_rows=200):
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        headers = r.fieldnames or []
        col_types = {h: "STRING" for h in headers}
        for i, row in enumerate(r):
            if i >= sample_rows: break
            for h in headers:
                v = (row.get(h) or "").strip()
                if v == "" or v.upper() == "NULL":
                    continue
                if col_types[h] == "STRING":
                    if looks_int(v):   col_types[h] = "NUMBER"
                    elif looks_float(v): col_types[h] = "NUMBER"
                    elif looks_date(v):  col_types[h] = "DATE"
                elif col_types[h] == "NUMBER":
                    if looks_date(v): col_types[h] = "STRING"
        cols = []
        for h in headers:
            name = re.sub(r"\W+", "_", h.strip()).upper().strip("_") or "COL"
            cols.append((name, col_types[h]))
        return cols

def step6_supplier_from_pg(cur):
    required = ["PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD"]
    if not psycopg2 or any(k not in os.environ for k in required):
        print("[Step6] Skipped (no Postgres settings)")
        return

    out_csv = Path("supplier_case.csv")
    print("[Step6] Exporting supplier_case from Postgres to", out_csv.resolve())
    pg_conn = psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=os.environ["PG_PORT"],
        dbname=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )
    with pg_conn, pg_conn.cursor() as pgc, out_csv.open("w", newline="", encoding="utf-8") as f:
        pgc.copy_expert("COPY supplier_case TO STDOUT WITH CSV HEADER", f)

    run(cur, "CREATE OR REPLACE STAGE STG_SUPPLIER")
    cur.execute(f"PUT '{out_csv.resolve().as_uri()}' @STG_SUPPLIER AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

    cols = infer_schema_from_csv(out_csv)
    col_defs = ", ".join(f"{qid(n)} {t}" for n, t in cols)

    run(cur, "DROP TABLE IF EXISTS SUPPLIER_CASE")
    run(cur, f"CREATE TABLE SUPPLIER_CASE ({col_defs})", "[Step6] SUPPLIER_CASE created")

    casts = []
    for i, (n, t) in enumerate(cols, start=1):
        if t == "NUMBER":
            casts.append(f"TRY_TO_NUMBER(${i}) AS {qid(n)}")
        elif t == "DATE":
            casts.append(
                f"COALESCE(TRY_TO_DATE(${i},'YYYY-MM-DD'),"
                f"         TRY_TO_DATE(${i},'MM/DD/YYYY'),"
                f"         TRY_TO_DATE(${i},'M/D/YYYY')) AS {qid(n)}"
            )
        else:
            casts.append(f"${i}::STRING AS {qid(n)}")
    projection = ", ".join(casts)

    run(cur, (
        "COPY INTO SUPPLIER_CASE "
        "FROM ( SELECT " + projection + " FROM @STG_SUPPLIER (FILE_FORMAT => FF_CSV) ) "
        "ON_ERROR=CONTINUE"
    ), "[Step6] SUPPLIER_CASE loaded")

    print("[Step6] Rows:", one(cur, "SELECT COUNT(*) FROM SUPPLIER_CASE")[0])

# Step 7: Weather Mapping

def build_weather_bridge(cur):
    cur.execute(
        "CREATE OR REPLACE VIEW supplier_zip AS "
        "SELECT DISTINCT LPAD(TO_VARCHAR(postalpostalcode),5,'0') AS zip5 "
        "FROM supplier_case WHERE postalpostalcode IS NOT NULL"
    )

    cur.execute(
        "CREATE OR REPLACE VIEW supplier_zip_geo AS "
        "SELECT s.zip5, z.intptlat AS lat, z.intptlong AS lon "
        "FROM supplier_zip s JOIN zcta_data z ON s.zip5 = z.geoid"
    )

    cur.execute(
        "CREATE OR REPLACE TABLE zip_station AS "
        "SELECT g.zip5, idx.noaa_weather_station_id AS station_id, "
        "ST_DISTANCE(TO_GEOGRAPHY('POINT('||g.lon||' '||g.lat||')'), "
        "TO_GEOGRAPHY('POINT('||idx.longitude||' '||idx.latitude||')')) AS dist "
        "FROM supplier_zip_geo g "
        "JOIN WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_STATION_INDEX idx "
        "ON idx.country_name='United States' "
        "QUALIFY ROW_NUMBER() OVER(PARTITION BY g.zip5 ORDER BY dist ASC)=1"
    )

    cur.execute(
        "CREATE OR REPLACE TABLE station_max_temp AS "
        "SELECT noaa_weather_station_id AS station_id, date, MAX(value) AS max_temp "
        "FROM WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_METRICS_TIMESERIES "
        "GROUP BY noaa_weather_station_id, date"
    )

    cur.execute(
        "CREATE OR REPLACE VIEW supplier_zip_weather AS "
        "SELECT z.zip5, t.date, t.max_temp FROM zip_station z JOIN station_max_temp t USING(station_id)"
    )
    print("[VIEW] supplier_zip_weather created")

# Step 8: Join Everything with Weather

def join_all(cur):
    cur.execute(
        "CREATE OR REPLACE VIEW po_weather AS "
        "SELECT p.invoice_id, p.po_id, p.supplier_id, p.invoice_date, p.amount_ex_tax, p.total_amount, "
        "p.invoiced_vs_quoted, s.postalpostalcode AS supplier_zip, w.max_temp "
        "FROM purchase_orders_and_invoices p "
        "JOIN supplier_case s ON p.supplier_id = s.supplierid "
        "JOIN supplier_zip_weather w ON LPAD(TO_VARCHAR(s.postalpostalcode),5,'0')=w.zip5 "
        "AND p.invoice_date=w.date"
    )
    print("[JOIN] po_weather view created")

# Main

def main():
    conn = connect_sf("user","pwd","account","WH","DB","SCHEMA")
    cur = conn.cursor()
    try:
        load_purchases(cur, "./po_csv")
        build_purchase_totals(cur)
        load_invoices(cur, "./Supplier_Transactions_XML.xml")
        join_purchases_invoices(cur)
        build_invoice_delta(cur)

        # Postgres connection
        # If you prefer env vars, set PG_* and call step6_supplier_from_pg(cur)
        step6_supplier_from_pg(cur)
        build_weather_bridge(cur)
        join_all(cur)
        print("✅ Full pipeline executed")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
