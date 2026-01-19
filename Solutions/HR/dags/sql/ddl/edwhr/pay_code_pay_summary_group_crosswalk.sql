CREATE TABLE IF NOT EXISTS `{{ params.param_hr_core_dataset_name }}.pay_code_pay_summary_group_crosswalk`
(
  clock_library_code STRING NOT NULL OPTIONS(description="Code for each clock library a clock code can belong to."),
  kronos_pay_code STRING NOT NULL OPTIONS(description="Code that defines each type of pay (e.g. productive, bereavement, etc.)"),
  kronos_pay_code_desc STRING OPTIONS(description="The description of the pay code."),
  lawson_pay_summary_group_code STRING OPTIONS(description="Code that groups pay codes together."),
  lawson_pay_code STRING OPTIONS(description="The interface code that is used by payroll."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY clock_library_code, kronos_pay_code
OPTIONS(
  description="Maps the kronos pay codes to the lawson pay summary groups."
);