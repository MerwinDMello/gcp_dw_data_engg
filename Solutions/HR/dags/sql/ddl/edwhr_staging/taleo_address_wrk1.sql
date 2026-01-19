CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_address_wrk1
(
  addr_type_code STRING NOT NULL,
  addr_line_1_text STRING NOT NULL,
  addr_line_2_text STRING,
  addr_line_3_text STRING,
  addr_line_4_text STRING,
  city_name STRING NOT NULL,
  zip_code STRING NOT NULL,
  county_name STRING NOT NULL,
  country_code STRING,
  state_code STRING NOT NULL,
  location_code STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;