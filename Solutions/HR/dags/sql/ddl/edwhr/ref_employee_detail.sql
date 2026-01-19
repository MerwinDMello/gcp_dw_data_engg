create table if not exists `{{ params.param_hr_core_dataset_name }}.ref_employee_detail`
(
  employee_detail_code STRING NOT NULL OPTIONS(description="unique list of employee detail codes applicable to an employee of an hr company maintained in this field."),
  employee_detail_type_code STRING NOT NULL OPTIONS(description="unique list of employee detail type codes are maintained in this field. "),
  employee_detail_desc STRING NOT NULL OPTIONS(description="description of employee detail code maintained in this field"),
  source_system_code STRING NOT NULL OPTIONS(description="a one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="datetime of update or load of this record to the enterprise data warehouse.")
)
CLUSTER BY employee_detail_code
OPTIONS(
  description="unique list of employee details are maintained in this table. employee details are like previous experience, auxilliary flag etc."
);