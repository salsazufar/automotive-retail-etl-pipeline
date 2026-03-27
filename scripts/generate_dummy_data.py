#!/usr/bin/env python3
"""Generate expanded seed_raw_tables.sql rows and customer_addresses CSV files.

Re-run to refresh dummy data. Keeps schema aligned with sql/seed_raw_tables.sql
and docs/sample_data.md.
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_OUT = ROOT / "sql" / "seed_raw_tables.sql"
INPUT_DIR = ROOT / "data" / "input"

# Silver layer rejects invoice/service dates after this (matches pipeline validation).
CUTOFF = date(2026, 3, 27)

random.seed(20260327)

MODELS = ("RAIZA", "RANGGO", "INNAVO", "VELOS")
MODEL_PRICE_RANGE = {
    "RAIZA": (210_000_000, 355_000_000),
    "RANGGO": (410_000_000, 465_000_000),
    "INNAVO": (585_000_000, 635_000_000),
    "VELOS": (368_000_000, 402_000_000),
}

SERVICE_TYPES = ("BP", "PM", "GR")

FIRST_NAMES = (
    "Ahmad", "Budi", "Citra", "Dewi", "Eko", "Fitri", "Gunawan", "Hana", "Indra",
    "Joko", "Kartika", "Lina", "Maya", "Nadia", "Omar", "Putri", "Rizki", "Sari",
    "Tono", "Udin", "Vera", "Wawan", "Yoga", "Zaki", "Adi", "Bayu", "Cici", "Dian",
)

CORP_NAMES = (
    "PT Sinar Motor", "PT Delta Trans", "CV Mandiri Jaya", "PT Armada Nusantara",
    "PT Kencana Mobil", "PT Citra Perkasa", "UD Sumber Rejeki", "PT Mitra Logistik",
)


def _fmt_idr(v: int) -> str:
    return f"{v:,}".replace(",", ".")


def _rand_vin() -> str:
    letters = "ABCDEFGHJKLMNPRSTUVWXYZ"
    digits = "0123456789"
    return (
        "".join(random.choices(letters, k=3))
        + "".join(random.choices(digits, k=4))
        + "".join(random.choices(letters, k=3))
    )


def _ticket_seq(n: int) -> str:
    suf = f"{random.choice('abcdefghijklmnopqrstuvwxyz')}{random.choice('abcdefghijklmnopqrstuvwxyz')}{random.randint(0, 9)}"
    return f"T{n:03d}-{suf}"


def _dob_variant(i: int) -> str | None:
    """Mix formats like the technical-test sample."""
    y = 1965 + (i % 45)
    m = 1 + (i % 12)
    d = 1 + (i % 28)
    fmt = i % 7
    if fmt == 0:
        return f"{y:04d}-{m:02d}-{d:02d}"
    if fmt == 1:
        return f"{y:04d}/{m:02d}/{d:02d}"
    if fmt == 2:
        return f"{d:02d}/{m:02d}/{y}"
    if fmt == 3:
        return "1900-01-01"
    if fmt == 4:
        return f"{y:04d}-{m:02d}-{d:02d}"
    if fmt == 5:
        return f"{d:02d}/{m:02d}/{y}"
    return f"{y:04d}/{m:02d}/{d:02d}"


def build_customers(max_id: int = 120) -> list[tuple]:
    rows: list[tuple] = []
    # Original 1–15 from technical test (unchanged)
    base = [
        (1, "Antonio", "1998-08-04", "2025-03-01 14:24:40.012"),
        (2, "Brandon", "2001-04-21", "2025-03-02 08:12:54.003"),
        (3, "Charlie", "1980/11/15", "2025-03-02 11:20:02.391"),
        (4, "Dominikus", "14/01/1995", "2025-03-03 09:50:41.852"),
        (5, "Erik", "1900-01-01", "2025-03-03 17:22:03.198"),
        (6, "PT Black Bird", None, "2025-03-04 12:52:16.122"),
        (7, "Ferdinand", "1994-10-01", "2025-03-05 08:01:11.100"),
        (8, "Gerald", "1992/06/19", "2025-03-05 08:22:11.450"),
        (9, "Hendra", "23/11/1988", "2025-03-06 10:12:01.231"),
        (10, "Irene", "1997-03-09", "2025-03-06 12:45:20.777"),
        (11, "PT Nusantara Karya", None, "2025-03-07 07:30:55.010"),
        (12, "Johan", "1985/01/31", "2025-03-07 14:09:09.910"),
        (13, "Kevin", "1900-01-01", "2025-03-08 09:20:08.200"),
        (14, "Lukman", "12/12/1990", "2025-03-08 11:21:39.301"),
        (15, "PT Mitra Armada", None, "2025-03-09 15:44:48.500"),
    ]
    rows.extend(base)
    base_dt = datetime(2025, 3, 10, 8, 0, 0)
    for cid in range(16, max_id + 1):
        dt = base_dt + timedelta(days=(cid - 16) * 2, seconds=(cid * 17) % 3600)
        ca = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        if cid % 11 == 0:
            name = CORP_NAMES[(cid // 11) % len(CORP_NAMES)]
            dob = None
        else:
            name = FIRST_NAMES[cid % len(FIRST_NAMES)] + f" {('Wijaya', 'Santoso', 'Kusuma', 'Pratama')[cid % 4]}"
            dob = _dob_variant(cid)
        rows.append((cid, name, dob, ca))
    return rows


def sql_escape(s: str | None) -> str:
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def build_sales(customers: list[tuple], n_sales: int) -> list[tuple]:
    """Returns (vin, customer_id, model, invoice_date, price, created_at)."""
    ids = [r[0] for r in customers]
    rows: list[tuple] = []
    seen_vins: set[str] = set()
    # Preserve original sales from seed (same VINs / rows)
    original = [
        ("JIS8135SAD", 1, "RAIZA", "2025-03-01", "350.000.000", "2025-03-01 14:24:40.012"),
        ("MAS8160POE", 3, "RANGGO", "2025-05-19", "430.000.000", "2025-05-19 14:29:21.003"),
        ("JLK1368KDE", 4, "INNAVO", "2025-05-22", "600.000.000", "2025-05-22 16:10:28.120"),
        ("JLK1869KDF", 6, "VELOS", "2025-08-02", "390.000.000", "2025-08-02 14:04:31.021"),
        ("JLK1962KOP", 6, "VELOS", "2025-08-02", "390.000.000", "2025-08-02 15:21:04.201"),
        ("QWE2034ZXC", 7, "RAIZA", "2025-03-15", "240.000.000", "2025-03-15 09:11:00.120"),
        ("ASD9234RTY", 8, "RANGGO", "2025-04-02", "420.000.000", "2025-04-02 10:20:33.031"),
        ("ZXC8821VBN", 9, "INNAVO", "2025-04-15", "610.000.000", "2025-04-15 13:42:19.777"),
        ("POI1192LKJ", 10, "VELOS", "2025-06-10", "390.000.000", "2025-06-10 11:00:00.000"),
        ("MNB7788GHJ", 11, "RAIZA", "2025-07-05", "210.000.000", "2025-07-05 14:12:47.201"),
        ("LKJ0911QAZ", 12, "RANGGO", "2025-07-19", "450.000.000", "2025-07-19 16:02:21.932"),
        ("WSX5500EDC", 13, "INNAVO", "2025-09-01", "620.000.000", "2025-09-01 08:20:54.432"),
        ("RFV7712TGB", 14, "VELOS", "2025-09-11", "380.000.000", "2025-09-11 11:11:11.111"),
        ("YHN2299UJM", 15, "RAIZA", "2025-10-03", "245.000.000", "2025-10-03 09:09:09.009"),
        ("TGB6677IKM", 3, "RANGGO", "2025-10-21", "430.000.000", "2025-10-21 10:10:10.100"),
        ("POI9933LOK", 4, "INNAVO", "2025-11-05", "600.000.000", "2025-11-05 12:50:40.500"),
        ("UJM8812HNB", 6, "VELOS", "2025-11-22", "390.000.000", "2025-11-22 17:33:28.017"),
        ("PLM0099QWE", 10, "VELOS", "2025-06-10", "390.000.000", "2025-06-10 13:30:00.500"),
    ]
    for r in original:
        seen_vins.add(r[0])
    rows.extend(original)

    start = date(2025, 1, 5)
    for i in range(len(rows), n_sales):
        vin = _rand_vin()
        while vin in seen_vins:
            vin = _rand_vin()
        seen_vins.add(vin)
        cid = random.choice(ids)
        model = random.choice(MODELS)
        lo, hi = MODEL_PRICE_RANGE[model]
        price = (random.randint(lo // 1_000_000, hi // 1_000_000)) * 1_000_000
        inv = start + timedelta(days=(i * 7 + random.randint(0, 4)) % 420)
        if inv > CUTOFF:
            inv = CUTOFF - timedelta(days=random.randint(1, 60))
        cr = datetime.combine(inv, datetime.min.time()).replace(
            hour=random.randint(8, 17), minute=random.randint(0, 59), second=random.randint(0, 59)
        )
        created = cr.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        rows.append(
            (vin, cid, model, inv.isoformat(), _fmt_idr(price), created)
        )
    return rows


def build_after_sales(
    sales: list[tuple], customer_ids: list[int], extra_orphans: int = 3
) -> list[tuple]:
    """service_date <= CUTOFF and >= invoice_date for linked VINs."""
    # vin -> invoice_date
    vin_invoice: dict[str, date] = {}
    for vin, cid, model, inv_s, _price, _ca in sales:
        invd = date.fromisoformat(inv_s) if isinstance(inv_s, str) else inv_s
        vin_invoice[vin] = invd

    rows: list[tuple] = []
    ticket_n = 100

    def add_row(ticket: str, vin: str, cid: int, model: str, sd: date, st: str) -> None:
        ca = datetime.combine(sd, datetime.min.time()).replace(
            hour=random.randint(8, 17), minute=random.randint(0, 59)
        ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        rows.append((ticket, vin, cid, model, sd.isoformat(), st, ca))

    # Original block (adjust future service_date + orphan per PDF)
    legacy = [
        ("T124-kgu1", "MAS8160POE", 3, "RANGGO", "2025-07-11", "BP", "2025-07-11 09:24:40.012"),
        ("T560-jga1", "JLK1368KDE", 4, "INNAVO", "2025-08-04", "PM", "2025-08-04 10:12:54.003"),
        # Orphan VIN (not in sales) — PDF example; keep past service_date
        ("T521-oai8", "POI1059IIK", 5, "RAIZA", "2025-11-10", "GR", "2025-11-10 12:45:02.391"),
        ("T601-kpl3", "QWE2034ZXC", 7, "RAIZA", "2025-05-01", "PM", "2025-05-01 08:10:11.001"),
        ("T602-kpl4", "QWE2034ZXC", 7, "RAIZA", "2025-07-01", "GR", "2025-07-01 08:11:11.001"),
        ("T603-kpl5", "ASD9234RTY", 8, "RANGGO", "2025-08-21", "PM", "2025-08-21 13:01:01.111"),
        ("T604-kpl6", "ZXC8821VBN", 9, "INNAVO", "2025-09-12", "BP", "2025-09-12 10:31:55.321"),
        ("T605-kpl7", "POI1192LKJ", 10, "VELOS", "2025-10-02", "GR", "2025-10-02 10:10:10.010"),
        ("T606-kpl8", "POI1192LKJ", 10, "VELOS", "2025-12-02", "PM", "2025-12-02 10:10:10.120"),
        ("T607-kpl9", "MNB7788GHJ", 11, "RAIZA", "2025-12-20", "BP", "2025-12-20 15:22:01.222"),
        ("T608-kpm0", "LKJ0911QAZ", 12, "RANGGO", "2026-01-11", "PM", "2026-01-11 09:44:34.900"),
        ("T609-kpm1", "WSX5500EDC", 13, "INNAVO", "2026-01-15", "GR", "2026-01-15 14:44:34.900"),
        ("T610-kpm2", "RFV7712TGB", 14, "VELOS", "2026-02-08", "PM", "2026-02-08 17:40:20.452"),
        ("T611-kpm3", "YHN2299UJM", 15, "RAIZA", "2026-03-03", "BP", "2026-03-03 10:21:49.340"),
        ("T612-kpm4", "AAA0000XXX", 2, "RAIZA", "2026-03-10", "GR", "2026-03-10 11:35:54.612"),
    ]
    for t, v, c, m, sd, st, ca in legacy:
        rows.append((t, v, c, m, sd, st, ca))

    # datamart_service_priority: COUNT per (year, vin, customer, address) with
    # MED = 5–10 services, HIGH = >10 (see gold datamart_service_priority).
    used_tickets = {r[0] for r in rows}

    def _ticket(prefix: str, n: int) -> str:
        tix = f"{prefix}{n:02d}"
        while tix in used_tickets:
            n += 1
            tix = f"{prefix}{n:02d}"
        used_tickets.add(tix)
        return tix

    def _append_row(
        ticket: str,
        vin: str,
        cid: int,
        model: str,
        sd: date,
        st: str,
    ) -> None:
        ca = datetime.combine(sd, datetime.min.time()).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        rows.append((ticket, vin, cid, model, sd.isoformat(), st, ca))

    # MED: 7 services in 2025 for JIS8135SAD (invoice 2025-03-01) → count 7 ∈ [5,10].
    vin_m, cid_m, model_m = "JIS8135SAD", 1, "RAIZA"
    inv_m = vin_invoice[vin_m]
    med_dates = [
        date(2025, 3, 18),
        date(2025, 4, 9),
        date(2025, 5, 14),
        date(2025, 6, 22),
        date(2025, 8, 5),
        date(2025, 10, 12),
        date(2025, 12, 3),
    ]
    for i, sd in enumerate(med_dates):
        assert sd >= inv_m and sd <= CUTOFF
        _append_row(_ticket("T9DM-", i), vin_m, cid_m, model_m, sd, SERVICE_TYPES[i % 3])

    # HIGH: legacy T124 (2025-07-11) + 11 more in 2025 for MAS8160POE → 12 > 10.
    vin_h, cid_h, model_h = "MAS8160POE", 3, "RANGGO"
    inv_h = vin_invoice[vin_h]
    high_dates = [
        date(2025, 6, 8),
        date(2025, 6, 28),
        date(2025, 7, 25),
        date(2025, 8, 7),
        date(2025, 8, 26),
        date(2025, 9, 9),
        date(2025, 9, 30),
        date(2025, 10, 14),
        date(2025, 11, 2),
        date(2025, 11, 19),
        date(2025, 12, 18),
    ]
    for i, sd in enumerate(high_dates):
        assert sd >= inv_h and sd <= CUTOFF
        _append_row(_ticket("T9HI-", i), vin_h, cid_h, model_h, sd, SERVICE_TYPES[(i + 1) % 3])

    # Extra services for generated VINs (skip original list keys)
    for vin, cid, model, inv_s, _p, _c in sales:
        if vin in {
            "JIS8135SAD", "MAS8160POE", "JLK1368KDE", "JLK1869KDF", "JLK1962KOP",
            "QWE2034ZXC", "ASD9234RTY", "ZXC8821VBN", "POI1192LKJ", "MNB7788GHJ",
            "LKJ0911QAZ", "WSX5500EDC", "RFV7712TGB", "YHN2299UJM", "TGB6677IKM",
            "POI9933LOK", "UJM8812HNB", "PLM0099QWE",
        }:
            continue
        if random.random() > 0.55:
            continue
        invd = date.fromisoformat(inv_s)
        span = (CUTOFF - invd).days
        if span < 14:
            continue
        n_sv = random.randint(1, 3)
        for _ in range(n_sv):
            ticket_n += 1
            ticket = _ticket_seq(ticket_n)
            while ticket in used_tickets:
                ticket_n += 1
                ticket = _ticket_seq(ticket_n)
            used_tickets.add(ticket)
            offset = random.randint(10, min(span, 400))
            sd = invd + timedelta(days=offset)
            if sd > CUTOFF:
                sd = CUTOFF - timedelta(days=random.randint(0, 5))
            if sd < invd:
                sd = invd + timedelta(days=1)
            add_row(ticket, vin, cid, model, sd, random.choice(SERVICE_TYPES))

    used_extra_vins = set(vin_invoice.keys())
    for j in range(extra_orphans):
        ticket_n += 1
        ticket = _ticket_seq(900 + j)
        while ticket in used_tickets:
            ticket_n += 1
            ticket = _ticket_seq(ticket_n)
        used_tickets.add(ticket)
        vin = _rand_vin()
        while vin in used_extra_vins:
            vin = _rand_vin()
        used_extra_vins.add(vin)
        cid = random.choice(customer_ids)
        sd = date(2025, 6, 1) + timedelta(days=j * 40)
        if sd > CUTOFF:
            sd = CUTOFF - timedelta(days=1)
        add_row(
            ticket,
            vin,
            cid,
            random.choice(MODELS),
            sd,
            random.choice(SERVICE_TYPES),
        )

    return rows


ADDRESSES = [
    ("Jl. Merdeka Selatan No {n}", "Bandung", "Jawa Barat"),
    ("Komplek Griya Asri Blok {n}", "Depok", "jawa barat"),
    ("Jl. Sudirman Kav {n}", "JAKARTA SELATAN", "DKI JAKARTA"),
    ("Perumahan Taman {n}", "BEKASI", "Jawa Barat"),
    ("Jl. Pahlawan {n}", "Semarang", "JAWA TENGAH"),
    ("Cluster Emerald No {n}", "Tangerang Selatan", "Jawa Barat"),
    ("Jl. Ahmad Yani {n}", "Surabaya", "Jawa Timur"),
    ("Kawasan Industri Sektor {n}", "Cikarang", "JAWA BARAT"),
    ("Apartemen City View Lt {n}", "Jakarta Barat", "DKI jakarta"),
    ("Jl. Gatot Subroto {n}", "Medan", "Sumatera Utara"),
]


def write_csv(path: Path, start_id: int, n_rows: int, customer_ids: list[int], stamp: str) -> int:
    """Returns next id."""
    lines = ["id,customer_id,address,city,province,created_at"]
    rid = start_id
    for i in range(n_rows):
        cid = random.choice(customer_ids)
        tpl = ADDRESSES[(rid + i) % len(ADDRESSES)]
        addr = tpl[0].format(n=(rid % 99) + 1)
        city = tpl[1]
        prov = tpl[2]
        if random.random() < 0.15:
            city = city.swapcase() if len(city) > 3 else city
        line = f'{rid},{cid},"{addr}",{city},{prov},{stamp}'
        lines.append(line)
        rid += 1
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return rid


def main() -> None:
    customers = build_customers(120)
    sales = build_sales(customers, n_sales=220)
    after_sales = build_after_sales(sales, [r[0] for r in customers], extra_orphans=5)

    header = """CREATE DATABASE IF NOT EXISTS dagster_internal;
CREATE DATABASE IF NOT EXISTS maju_jaya_dw;
CREATE USER IF NOT EXISTS 'maju_jaya'@'%' IDENTIFIED BY 'secret';
GRANT ALL PRIVILEGES ON maju_jaya_dw.* TO 'maju_jaya'@'%';
GRANT ALL PRIVILEGES ON dagster_internal.* TO 'maju_jaya'@'%';
FLUSH PRIVILEGES;
USE maju_jaya_dw;

CREATE TABLE IF NOT EXISTS customers_raw (
  id INT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  dob VARCHAR(20) NULL,
  created_at DATETIME(3) NOT NULL
);

CREATE TABLE IF NOT EXISTS sales_raw (
  vin VARCHAR(20) NOT NULL,
  customer_id INT NOT NULL,
  model VARCHAR(50) NOT NULL,
  invoice_date DATE NOT NULL,
  price VARCHAR(20) NOT NULL,
  created_at DATETIME(3) NOT NULL
);

CREATE TABLE IF NOT EXISTS after_sales_raw (
  service_ticket VARCHAR(20) PRIMARY KEY,
  vin VARCHAR(20) NOT NULL,
  customer_id INT NOT NULL,
  model VARCHAR(50) NOT NULL,
  service_date DATE NOT NULL,
  service_type VARCHAR(10) NOT NULL,
  created_at DATETIME(3) NOT NULL
);

"""

    cust_vals = ",\n  ".join(
        f"({r[0]}, {sql_escape(r[1])}, {sql_escape(r[2])}, '{r[3]}')" for r in customers
    )
    sales_vals = ",\n  ".join(
        f"('{r[0]}', {r[1]}, '{r[2]}', '{r[3]}', '{r[4]}', '{r[5]}')" for r in sales
    )
    after_vals = ",\n  ".join(
        f"('{r[0]}', '{r[1]}', {r[2]}, '{r[3]}', '{r[4]}', '{r[5]}', '{r[6]}')" for r in after_sales
    )

    sql = (
        header
        + f"INSERT INTO customers_raw (id, name, dob, created_at)\nVALUES\n  {cust_vals}\n"
        + "ON DUPLICATE KEY UPDATE\n"
        + "  name = VALUES(name),\n"
        + "  dob = VALUES(dob),\n"
        + "  created_at = VALUES(created_at);\n\n"
        + f"INSERT INTO sales_raw (vin, customer_id, model, invoice_date, price, created_at)\nVALUES\n  {sales_vals};\n\n"
        + f"INSERT INTO after_sales_raw (service_ticket, vin, customer_id, model, service_date, service_type, created_at)\nVALUES\n  {after_vals}\n"
        + "ON DUPLICATE KEY UPDATE\n"
        + "  vin = VALUES(vin),\n"
        + "  customer_id = VALUES(customer_id),\n"
        + "  model = VALUES(model),\n"
        + "  service_date = VALUES(service_date),\n"
        + "  service_type = VALUES(service_type),\n"
        + "  created_at = VALUES(created_at);\n"
    )
    SQL_OUT.write_text(sql, encoding="utf-8")

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    cids = [r[0] for r in customers]
    next_id = 19
    for day, stamp in [
        ("20260328", "2026-03-28 08:00:00.000"),
        ("20260329", "2026-03-29 08:00:00.000"),
        ("20260330", "2026-03-30 08:00:00.000"),
    ]:
        p = INPUT_DIR / f"customer_addresses_{day}.csv"
        next_id = write_csv(p, next_id, 180, cids, stamp)

    print(f"Wrote {SQL_OUT} ({len(customers)} customers, {len(sales)} sales, {len(after_sales)} after_sales)")
    print(f"Wrote 3 CSV files in {INPUT_DIR} (~180 rows each, ids starting 19)")


if __name__ == "__main__":
    main()
