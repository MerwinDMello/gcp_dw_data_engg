CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_holiday_calendar
(
  holiday_calendar_id INT64 NOT NULL OPTIONS(description='A unique identifier for holiday calendar'),
  holiday_calendar_date DATE OPTIONS(description='Date for holiday Calendar'),
  holiday_calendar_desc STRING OPTIONS(description='Description for holiday calendar'),
  created_date_time DATETIME OPTIONS(description='Date Time record is created in discover database'),
  created_by_3_4_id STRING OPTIONS(description='3-4 ID who created the record in discover database'),
  source_last_update_date_time DATETIME OPTIONS(description='Date Time record last updated in discover database'),
  updated_by_3_4_id STRING OPTIONS(description='3-4 ID who updated the record in discover database'),
  active_ind STRING OPTIONS(description='Indicates if data is active or not'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY holiday_calendar_id
OPTIONS(
  description='Reference holding data for holiday calendar from discover database'
);
