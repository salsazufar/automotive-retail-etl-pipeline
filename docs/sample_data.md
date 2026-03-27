# Sample Data Reference — Maju Jaya Data Warehouse

> Note: Data is sample only — for schema and cleaning logic reference.

---

## MySQL Tables (Existing)

### `customers_raw`

| id | name         | dob        | created_at                  |
|----|--------------|------------|-----------------------------|
| 1  | Antonio      | 1998-08-04 | 2025-03-01 14:24:40.012     |
| 2  | Brandon      | 2001-04-21 | 2025-03-02 08:12:54.003     |
| 3  | Charlie      | 1980/11/15 | 2025-03-02 11:20:02.391     |
| 4  | Dominikus    | 14/01/1995 | 2025-03-03 09:50:41.852     |
| 5  | Erik         | 1900-01-01 | 2025-03-03 17:22:03.198     |
| 6  | PT Black Bird| NULL       | 2025-03-04 12:52:16.122     |

**Known data quality issues:**
- `dob` has 3 inconsistent formats: `YYYY-MM-DD`, `YYYY/MM/DD`, `DD/MM/YYYY`
- `dob = 1900-01-01` on row 5 → sentinel/default value, treat as NULL
- Row 6 (`PT Black Bird`) is a corporate entity → `dob` is NULL by nature

---

### `sales_raw`

| vin         | customer_id | model  | invoice_date | price       | created_at                  |
|-------------|-------------|--------|--------------|-------------|-----------------------------|
| JIS8135SAD  | 1           | RAIZA  | 2025-03-01   | 350.000.000 | 2025-03-01 14:24:40.012     |
| MAS8160POE  | 3           | RANGGO | 2025-05-19   | 430.000.000 | 2025-05-19 14:29:21.003     |
| JLK1368KDE  | 4           | INNAVO | 2025-05-22   | 600.000.000 | 2025-05-22 16:10:28.12      |
| JLK1869KDF  | 6           | VELOS  | 2025-08-02   | 390.000.000 | 2025-08-02 14:04:31.021     |
| JLK1962KOP  | 6           | VELOS  | 2025-08-02   | 390.000.000 | 2025-08-02 15:21:04.201     |

**Known data quality issues:**
- `price` stored as string with dot-thousands separator (`350.000.000`) → must strip dots and cast to `BIGINT`
- Rows 4 & 5 (JLK1869KDF, JLK1962KOP): same `customer_id=6`, `model=VELOS`, `invoice_date`, and `price` → likely duplicates; deduplicate by keeping earliest `created_at`

---

### `after_sales_raw`

| service_ticket | vin        | customer_id | model  | service_date | service_type | created_at                  |
|----------------|------------|-------------|--------|--------------|--------------|-----------------------------|
| T124-kgu1      | MAS8160POE | 3           | RANGGO | 2025-07-11   | BP           | 2025-07-11 09:24:40.012     |
| T560-jga1      | JLK1368KDE | 4           | INNAVO | 2025-08-04   | PM           | 2025-08-04 10:12:54.003     |
| T521-oai8      | POI1059IIK | 5           | RAIZA  | 2026-09-10   | GR           | 2026-09-10 12:45:02.391     |

**Known data quality issues:**
- Row 3 (`POI1059IIK`): VIN does not exist in `sales_raw` → orphan record; flag as `is_orphan = TRUE` in silver layer

**Service type codes:**
| Code | Meaning              |
|------|----------------------|
| BP   | Body Paint           |
| PM   | Preventive Maintenance |
| GR   | General Repair       |

---

## CSV File (Daily File Share)

### `customer_addresses_yyyymmdd.csv`

| id | customer_id | address                    | city            | province    | created_at                  |
|----|-------------|----------------------------|-----------------|-------------|-----------------------------|
| 1  | 1           | Jalan Mawar V, RT 1/RW 2   | Bekasi          | Jawa Barat  | 2026-03-01 14:24:40.012     |
| 2  | 3           | Jl Ababil Indah            | Tangerang Selatan | Jawa Barat | 2026-03-01 14:24:40.012     |
| 3  | 4           | Jl. Kemang Raya 1 No 3     | JAKARTA PUSAT   | DKI JAKARTA | 2026-03-01 14:24:40.012     |
| 4  | 6           | Astra Tower Jalan Yos Sudarso 12 | Jakarta Utara | DKI Jakarta | 2026-03-01 14:24:40.012 |

**Known data quality issues:**
- `city` and `province` casing is inconsistent: `JAKARTA PUSAT` vs `Jakarta Utara` vs `Bekasi` → normalize to title case in silver
- `province` inconsistency: `DKI JAKARTA` vs `DKI Jakarta` → normalize

**File naming convention:** `customer_addresses_YYYYMMDD.csv`  
**Drop location:** `data/input/`  
**Frequency:** Daily

---

## Cross-Table Relationships

```
customers_raw (id) ──────────── sales_raw (customer_id)
customers_raw (id) ──────────── after_sales_raw (customer_id)
customers_raw (id) ──────────── customer_addresses (customer_id)
sales_raw (vin)    ──────────── after_sales_raw (vin)  ← NOT enforced, orphans exist
```

---

## Price Classification (for Gold Layer — `datamart_sales_monthly`)

| Class  | Price Range          |
|--------|----------------------|
| LOW    | 100,000,000 – 250,000,000 |
| MEDIUM | 250,000,001 – 400,000,000 |
| HIGH   | > 400,000,000        |

Based on sample data:
- RAIZA  → 350,000,000 → **MEDIUM**
- RANGGO → 430,000,000 → **HIGH**
- INNAVO → 600,000,000 → **HIGH**
- VELOS  → 390,000,000 → **MEDIUM**

---

## Service Priority Classification (for Gold Layer — `datamart_service_priority`)

| Priority | Count of Services |
|----------|-------------------|
| HIGH     | > 10x per year    |
| MED      | 5–10x per year    |
| LOW      | < 5x per year     |