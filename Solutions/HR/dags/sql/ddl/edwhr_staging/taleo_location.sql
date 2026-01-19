CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_location
(
  file_date DATE,
  number STRING,
  abbreviation STRING,
  code STRING,
  complete STRING,
  customerid STRING,
  lastmodifiedon DATETIME,
  level STRING,
  name STRING,
  zipcode STRING,
  networklocation_number STRING,
  parent_number STRING,
  sequence STRING,
  status_number STRING,
  worklocation_address1 STRING,
  worklocation_address2 STRING,
  worklocation_city STRING,
  worklocation_code STRING,
  dw_last_update_date_time DATETIME
);