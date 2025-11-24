-- =====================================================
--  DATABASE SETUP
-- =====================================================
CREATE DATABASE IF NOT EXISTS iot_medical_waste_tracker;
USE iot_medical_waste_tracker;

-- =====================================================
-- 1. DEPARTMENTS TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS departments (
    dept_id INT AUTO_INCREMENT PRIMARY KEY,
    dept_name VARCHAR(100) NOT NULL
);

-- =====================================================
-- 2. EMPLOYEES TABLE
-- =====================================================
-- Referenced in:
--   INSERT INTO employees (emp_name, emp_pin, dept_id)
--   SELECT emp_id, emp_name, emp_pin FROM employees;
--   SELECT * FROM Departments;   (so departments must exist)

CREATE TABLE IF NOT EXISTS employees (
    emp_id INT AUTO_INCREMENT PRIMARY KEY,
    emp_name VARCHAR(100) NOT NULL,
    emp_pin VARCHAR(20) NOT NULL UNIQUE,
    dept_id INT,
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL
);

-- =====================================================
-- 3. WASTE STREAMS TABLE
-- =====================================================
-- Used heavily in inserts and updates.

CREATE TABLE IF NOT EXISTS waste_streams (
    waste_stream_id INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    hazardous TINYINT(1) DEFAULT 0,
    description TEXT,
    disposal_guidance TEXT
);

-- =====================================================
-- 4. BINS TABLE
-- =====================================================
-- NOTE: Your script shows:
--   ALTER TABLE bins DROP FOREIGN KEY fk_bin_dept;
--   ALTER TABLE bins DROP COLUMN dept_id;
--
-- So dept_id should NOT exist in final schema.

CREATE TABLE IF NOT EXISTS bins (
    bin_id INT AUTO_INCREMENT PRIMARY KEY,
    bin_name VARCHAR(100) NOT NULL,
    waste_stream_id INT,
    esp32_mac VARCHAR(50),
    location VARCHAR(150),

    FOREIGN KEY (waste_stream_id) REFERENCES waste_streams(waste_stream_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL
);

-- =====================================================
-- 5. UUID LOGS TABLE
-- =====================================================
-- Must support:
--   SELECT * FROM uuid_logs ORDER BY generated_at DESC;
--   SELECT * FROM uuid_logs ORDER BY uuid_id DESC LIMIT 3;

CREATE TABLE IF NOT EXISTS uuid_logs (
    uuid_id INT AUTO_INCREMENT PRIMARY KEY,
    uuid_value VARCHAR(100) NOT NULL,
    emp_id INT,
    bin_id INT,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (emp_id) REFERENCES employees(emp_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL,

    FOREIGN KEY (bin_id) REFERENCES bins(bin_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL
);

-- =====================================================
-- OPTIONAL SEED DATA (SAFE EMPTY INSERTS)
-- =====================================================
INSERT INTO departments (dept_name) VALUES
("General"), ("Emergency"), ("Surgery"), ("Pharmacy"), ("Nuclear Medicine");

