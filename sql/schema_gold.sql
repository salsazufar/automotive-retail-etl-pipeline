USE maju_jaya_dw;

CREATE TABLE IF NOT EXISTS datamart_sales_monthly (
  periode VARCHAR(7) NOT NULL,
  class ENUM('LOW', 'MEDIUM', 'HIGH') NOT NULL,
  model VARCHAR(50) NOT NULL,
  total_sales BIGINT NOT NULL,
  INDEX idx_datamart_sales_monthly_periode (periode),
  INDEX idx_datamart_sales_monthly_class (class)
);

CREATE TABLE IF NOT EXISTS datamart_service_priority (
  periode VARCHAR(4) NOT NULL,
  vin VARCHAR(20) NOT NULL,
  customer_name VARCHAR(100) NOT NULL,
  address TEXT NOT NULL,
  count_service INT NOT NULL,
  priority ENUM('HIGH', 'MED', 'LOW') NOT NULL,
  INDEX idx_datamart_service_priority_periode (periode),
  INDEX idx_datamart_service_priority_vin (vin),
  INDEX idx_datamart_service_priority_priority (priority)
);
