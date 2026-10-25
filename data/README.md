# Data

Raw data files are **not included in this repo**. The pipeline expects:

- `./po_csv/` — 41 monthly purchase-order CSVs, one file per month,
  line-item level (the loader reads columns by position: PO id, order date,
  line id, quantity received, unit price)
- `./Supplier_Transactions_XML.xml` — supplier invoice XML, one document
  per invoice (fields used: SupplierInvoiceNumber, PurchaseOrderID,
  SupplierID, TransactionDate, AmountExcludingTax)
- A Postgres table `supplier_case` — supplier master data (supplier id,
  postal code, and related attributes), exported by the pipeline itself
- Census ZCTA gazetteer file — zip code to latitude/longitude mapping,
  used to match each supplier zip to its nearest NOAA weather station
- Cybersyn Weather & Environment data in Snowflake Marketplace
  (`NOAA_WEATHER_STATION_INDEX`, `NOAA_WEATHER_METRICS_TIMESERIES`)

Place the local files as listed above and the pipeline runs end to end;
everything else it needs it creates inside Snowflake.
