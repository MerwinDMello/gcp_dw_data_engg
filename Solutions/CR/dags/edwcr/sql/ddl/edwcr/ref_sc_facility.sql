CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_sc_facility
(
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  coid_name STRING NOT NULL OPTIONS(description='This column stores the COID name.As per Sarah Canon business requirement this column name is changed to Facility Name.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY coid
OPTIONS(
  description='This tables stores all the markets of Sarah Cannon'
);
