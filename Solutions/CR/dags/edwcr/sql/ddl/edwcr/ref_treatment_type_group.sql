CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_treatment_type_group
(
  treatment_type_group_id INT64 NOT NULL OPTIONS(description='A unique identifier for type of treatment group'),
  treatment_type_group_code STRING OPTIONS(description='Alpha character representation of the treatment group performed'),
  treatment_type_group_desc STRING OPTIONS(description='Description of Treatment type group performed'),
  nav_treatment_type_group_desc STRING OPTIONS(description='Description of the Treatment type grouped into Biopsy, Surgery, Rad Onc, Med Onc, Other, Palliative Only'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY treatment_type_group_id
OPTIONS(
  description='This table contains different codes and descriptions for treatment type groups'
);
