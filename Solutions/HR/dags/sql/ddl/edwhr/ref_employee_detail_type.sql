create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_employee_detail_type`
(
  employee_detail_type_code STRING NOT NULL OPTIONS(description="indicates if the employee detail type code is an alphanumeric, numeric, or date detail  field."),
  employee_detail_type_desc STRING OPTIONS(description="this field maintains description of employee detail type code."),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY employee_detail_type_code
OPTIONS(
  description="this field maintains unique employee details types like alphanumeric, numeric, date & amount."
);