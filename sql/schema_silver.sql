USE maju_jaya_dw;

CREATE TABLE IF NOT EXISTS customers_silver (
  id INT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  dob DATE NULL,
  customer_type ENUM('CORPORATE', 'INDIVIDUAL') NOT NULL,
  created_at DATETIME(3) NOT NULL
);

CREATE TABLE IF NOT EXISTS sales_silver (
  vin VARCHAR(20) NOT NULL,
  customer_id INT NOT NULL,
  model VARCHAR(50) NOT NULL,
  invoice_date DATE NOT NULL,
  price BIGINT NOT NULL,
  created_at DATETIME(3) NOT NULL,
  PRIMARY KEY (vin),
  INDEX idx_sales_silver_customer (customer_id),
  INDEX idx_sales_silver_invoice_date (invoice_date)
);

CREATE TABLE IF NOT EXISTS after_sales_silver (
  service_ticket VARCHAR(20) PRIMARY KEY,
  vin VARCHAR(20) NOT NULL,
  customer_id INT NOT NULL,
  model VARCHAR(50) NOT NULL,
  service_date DATE NOT NULL,
  service_type VARCHAR(10) NOT NULL,
  is_orphan BOOLEAN NOT NULL DEFAULT FALSE,
  created_at DATETIME(3) NOT NULL,
  INDEX idx_after_sales_silver_customer (customer_id),
  INDEX idx_after_sales_silver_vin (vin)
);

CREATE TABLE IF NOT EXISTS customer_addresses_silver (
  id INT NOT NULL,
  customer_id INT NOT NULL,
  address VARCHAR(255) NOT NULL,
  city VARCHAR(100) NOT NULL,
  province VARCHAR(100) NOT NULL,
  created_at DATETIME(3) NULL,
  INDEX idx_customer_addresses_silver_customer (customer_id)
);

CREATE TABLE IF NOT EXISTS cleaning_quarantine (
  id INT AUTO_INCREMENT PRIMARY KEY,
  source_table VARCHAR(50) NOT NULL,
  record_key VARCHAR(100) NOT NULL,
  reason VARCHAR(255) NOT NULL,
  raw_payload JSON,
  quarantined_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_quarantine_source_table (source_table),
  INDEX idx_quarantine_quarantined_at (quarantined_at)
);
