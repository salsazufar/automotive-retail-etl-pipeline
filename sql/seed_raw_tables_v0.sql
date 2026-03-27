CREATE DATABASE IF NOT EXISTS dagster_internal;
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

INSERT INTO customers_raw (id, name, dob, created_at)
VALUES
  (1, 'Antonio', '1998-08-04', '2025-03-01 14:24:40.012'),
  (2, 'Brandon', '2001-04-21', '2025-03-02 08:12:54.003'),
  (3, 'Charlie', '1980/11/15', '2025-03-02 11:20:02.391'),
  (4, 'Dominikus', '14/01/1995', '2025-03-03 09:50:41.852'),
  (5, 'Erik', '1900-01-01', '2025-03-03 17:22:03.198'),
  (6, 'PT Black Bird', NULL, '2025-03-04 12:52:16.122'),
  (7, 'Ferdinand', '1994-10-01', '2025-03-05 08:01:11.100'),
  (8, 'Gerald', '1992/06/19', '2025-03-05 08:22:11.450'),
  (9, 'Hendra', '23/11/1988', '2025-03-06 10:12:01.231'),
  (10, 'Irene', '1997-03-09', '2025-03-06 12:45:20.777'),
  (11, 'PT Nusantara Karya', NULL, '2025-03-07 07:30:55.010'),
  (12, 'Johan', '1985/01/31', '2025-03-07 14:09:09.910'),
  (13, 'Kevin', '1900-01-01', '2025-03-08 09:20:08.200'),
  (14, 'Lukman', '12/12/1990', '2025-03-08 11:21:39.301'),
  (15, 'PT Mitra Armada', NULL, '2025-03-09 15:44:48.500')
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  dob = VALUES(dob),
  created_at = VALUES(created_at);

INSERT INTO sales_raw (vin, customer_id, model, invoice_date, price, created_at)
VALUES
  ('JIS8135SAD', 1, 'RAIZA', '2025-03-01', '350.000.000', '2025-03-01 14:24:40.012'),
  ('MAS8160POE', 3, 'RANGGO', '2025-05-19', '430.000.000', '2025-05-19 14:29:21.003'),
  ('JLK1368KDE', 4, 'INNAVO', '2025-05-22', '600.000.000', '2025-05-22 16:10:28.120'),
  ('JLK1869KDF', 6, 'VELOS', '2025-08-02', '390.000.000', '2025-08-02 14:04:31.021'),
  ('JLK1962KOP', 6, 'VELOS', '2025-08-02', '390.000.000', '2025-08-02 15:21:04.201'),
  ('QWE2034ZXC', 7, 'RAIZA', '2025-03-15', '240.000.000', '2025-03-15 09:11:00.120'),
  ('ASD9234RTY', 8, 'RANGGO', '2025-04-02', '420.000.000', '2025-04-02 10:20:33.031'),
  ('ZXC8821VBN', 9, 'INNAVO', '2025-04-15', '610.000.000', '2025-04-15 13:42:19.777'),
  ('POI1192LKJ', 10, 'VELOS', '2025-06-10', '390.000.000', '2025-06-10 11:00:00.000'),
  ('MNB7788GHJ', 11, 'RAIZA', '2025-07-05', '210.000.000', '2025-07-05 14:12:47.201'),
  ('LKJ0911QAZ', 12, 'RANGGO', '2025-07-19', '450.000.000', '2025-07-19 16:02:21.932'),
  ('WSX5500EDC', 13, 'INNAVO', '2025-09-01', '620.000.000', '2025-09-01 08:20:54.432'),
  ('RFV7712TGB', 14, 'VELOS', '2025-09-11', '380.000.000', '2025-09-11 11:11:11.111'),
  ('YHN2299UJM', 15, 'RAIZA', '2025-10-03', '245.000.000', '2025-10-03 09:09:09.009'),
  ('TGB6677IKM', 3, 'RANGGO', '2025-10-21', '430.000.000', '2025-10-21 10:10:10.100'),
  ('POI9933LOK', 4, 'INNAVO', '2025-11-05', '600.000.000', '2025-11-05 12:50:40.500'),
  ('UJM8812HNB', 6, 'VELOS', '2025-11-22', '390.000.000', '2025-11-22 17:33:28.017'),
  ('PLM0099QWE', 10, 'VELOS', '2025-06-10', '390.000.000', '2025-06-10 13:30:00.500');

INSERT INTO after_sales_raw (service_ticket, vin, customer_id, model, service_date, service_type, created_at)
VALUES
  ('T124-kgu1', 'MAS8160POE', 3, 'RANGGO', '2025-07-11', 'BP', '2025-07-11 09:24:40.012'),
  ('T560-jga1', 'JLK1368KDE', 4, 'INNAVO', '2025-08-04', 'PM', '2025-08-04 10:12:54.003'),
  ('T521-oai8', 'POI1059IIK', 5, 'RAIZA', '2026-09-10', 'GR', '2026-09-10 12:45:02.391'),
  ('T601-kpl3', 'QWE2034ZXC', 7, 'RAIZA', '2025-05-01', 'PM', '2025-05-01 08:10:11.001'),
  ('T602-kpl4', 'QWE2034ZXC', 7, 'RAIZA', '2025-07-01', 'GR', '2025-07-01 08:11:11.001'),
  ('T603-kpl5', 'ASD9234RTY', 8, 'RANGGO', '2025-08-21', 'PM', '2025-08-21 13:01:01.111'),
  ('T604-kpl6', 'ZXC8821VBN', 9, 'INNAVO', '2025-09-12', 'BP', '2025-09-12 10:31:55.321'),
  ('T605-kpl7', 'POI1192LKJ', 10, 'VELOS', '2025-10-02', 'GR', '2025-10-02 10:10:10.010'),
  ('T606-kpl8', 'POI1192LKJ', 10, 'VELOS', '2025-12-02', 'PM', '2025-12-02 10:10:10.120'),
  ('T607-kpl9', 'MNB7788GHJ', 11, 'RAIZA', '2025-12-20', 'BP', '2025-12-20 15:22:01.222'),
  ('T608-kpm0', 'LKJ0911QAZ', 12, 'RANGGO', '2026-01-11', 'PM', '2026-01-11 09:44:34.900'),
  ('T609-kpm1', 'WSX5500EDC', 13, 'INNAVO', '2026-01-15', 'GR', '2026-01-15 14:44:34.900'),
  ('T610-kpm2', 'RFV7712TGB', 14, 'VELOS', '2026-02-08', 'PM', '2026-02-08 17:40:20.452'),
  ('T611-kpm3', 'YHN2299UJM', 15, 'RAIZA', '2026-03-03', 'BP', '2026-03-03 10:21:49.340'),
  ('T612-kpm4', 'AAA0000XXX', 2, 'RAIZA', '2026-03-10', 'GR', '2026-03-10 11:35:54.612')
ON DUPLICATE KEY UPDATE
  vin = VALUES(vin),
  customer_id = VALUES(customer_id),
  model = VALUES(model),
  service_date = VALUES(service_date),
  service_type = VALUES(service_type),
  created_at = VALUES(created_at);
