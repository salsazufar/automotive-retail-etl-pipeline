USE maju_jaya_dw;

CREATE TABLE IF NOT EXISTS customer_addresses_bronze (
  id INT NOT NULL,
  customer_id INT NOT NULL,
  address VARCHAR(255) NOT NULL,
  city VARCHAR(100) NOT NULL,
  province VARCHAR(100) NOT NULL,
  created_at DATETIME(3) NULL,
  source_file VARCHAR(255) NOT NULL,
  ingested_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_customer_addresses_bronze_customer (customer_id),
  INDEX idx_customer_addresses_bronze_source_file (source_file)
);

CREATE TABLE IF NOT EXISTS processed_files_log (
  id INT AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  file_hash CHAR(64) NOT NULL,
  row_count INT NOT NULL,
  ingested_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
