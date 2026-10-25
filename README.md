# MSBA Data Pipeline & Cost Analytics — Snowflake ETL/ELT

> By Andy Liu · Summer 2025
> Python + Snowflake pipeline that consolidates 12 months of purchase-order,
> invoice, supplier, and weather data into validated tables, and computes
> invoice-vs-quoted variances for over/under-billing review.
> Raw data files are not included in this repo (see `data/README.md`).

## The problem: four sources, one question

A buyer wants to know a simple thing: **did we get billed what we were quoted?**
Answering it means joining four sources that were never designed to fit
together:

- **41 monthly purchase-order CSVs** — line-item level, one file per month
- **One supplier-invoice XML file** — semi-structured, one document per invoice
- **A supplier master table in Postgres** — names, ids, and zip codes
- **NOAA weather data from the Snowflake Marketplace (Cybersyn)** — daily
  station readings, joined by geography to test whether temperature had
  anything to do with billing differences

The pipeline's job is to land all four in Snowflake, standardize types and
definitions as data lands, and produce one joined, audit-ready view — with as
much of the work as possible done inside Snowflake SQL rather than in Python.

## The pipeline

All steps live in `snowflake_pipeline.py`. Python orchestrates; Snowflake
transforms.

### 1. Purchase orders: 41 CSVs → one table

Files are staged with `PUT` (globbed automatically, not one by one) and loaded
with `COPY INTO`, selecting only the relevant columns and casting types during
the load — dates stored as dates, not varchars. Line items roll up into
`purchase_totals`: one row per purchase order, with `total_amount` as the sum
of `qty_received × unit_price` across lines. That total is the "quoted" side
of the comparison.

### 2. Invoices: XML → one row per invoice

The XML file is staged and loaded as `VARIANT`, parsed with `PARSE_XML`, and
shredded with `LATERAL FLATTEN` into a flat `invoices` table: invoice id,
purchase-order id, supplier id, transaction date, and `amount_ex_tax`.

### 3. The variance: invoiced vs quoted

Joining invoices to purchase totals on purchase-order id (inner join — only
matching rows) gives `po_invoice_join`, and from it a materialized view
`purchase_orders_and_invoices` with the key calculated field:

```sql
(COALESCE(amount_ex_tax, 0) - COALESCE(total_amount, 0)) AS invoiced_vs_quoted
```

Positive means billed above quote, negative below — the field an auditor
actually reviews for over- and under-billing trends. (If a warehouse does not
support materialized views, the same query lands as a table; the code handles
both.)

### 4. Supplier master: Postgres → Snowflake, schema inferred

The supplier table is exported from Postgres with `COPY ... TO STDOUT` via
psycopg2, moved straight into a Snowflake stage, and loaded into
`SUPPLIER_CASE`. The table definition is not hand-written: a helper samples
the CSV, infers each column as NUMBER, DATE, or STRING from its values, and
generates the `CREATE TABLE` plus the typed `COPY INTO` projection. New or
changed source columns do not require rewriting the loader by hand.

### 5. Weather: nearest station per supplier zip

Weather has no zip code in the station data, so the join is geographic.
Supplier zips are mapped to lat/lon with the Census ZCTA file, then each zip
is matched to its **nearest** NOAA station by geodesic distance
(`ST_DISTANCE` on geography points, one station per zip). Daily maximum
temperatures come from the Cybersyn time series, giving
`supplier_zip_weather`: one row per zip per day.

### 6. One joined view

`po_weather` joins the invoice variance view to the supplier master and to
daily weather on supplier zip and invoice date — only transactions with a
matching temperature reading are kept. The result is the analysis-ready table:
invoice, quote, variance, supplier, and that day's high temperature side by
side.

## What this enables — and what it does not claim

- **Validated, standardized tables**: one definition of a purchase-order
  total, one definition of variance, types fixed at load — the basis for
  consistent reporting and audit readiness.
- **Over/under-billing review**: `invoiced_vs_quoted` per order, sliceable by
  supplier and date, instead of eyeballing invoices against orders.
- **A fair test of the weather question**: with temperature joined at the
  transaction level, any temperature effect on billing differences can be
  checked rather than assumed. The pipeline builds that test; it does not
  claim a finding the data did not show.

## How to run

1. Place the 41 monthly PO CSVs in `./po_csv` and the invoice XML at
   `./Supplier_Transactions_XML.xml` (see `data/README.md` for the expected
   formats).
2. Set the Postgres connection via environment variables
   (`PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`) for the
   supplier-master step.
3. Fill in your Snowflake connection in `main()` — the checked-in placeholders
   (`user`, `pwd`, `account`, `WH`, `DB`, `SCHEMA`) are not real credentials.
4. Run `snowflake_pipeline.py`. Steps 1–8 execute in order; the weather
   steps assume the Cybersyn Weather & Environment data share is available
   in your Snowflake account.

## Stack

Python · Snowflake (SQL, stages, `COPY INTO`, materialized views, geospatial
functions) · snowflake-connector-python · psycopg2 · NOAA/Cybersyn weather
data via Snowflake Marketplace.
