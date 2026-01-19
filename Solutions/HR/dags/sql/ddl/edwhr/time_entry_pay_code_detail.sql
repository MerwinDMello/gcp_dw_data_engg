CREATE TABLE IF NOT EXISTS `{{ params.param_hr_core_dataset_name }}.time_entry_pay_code_detail`
(
  employee_num INT64 NOT NULL OPTIONS(description="This is the lawson employee number"),
  kronos_num NUMERIC NOT NULL OPTIONS(description="Number for each personnel assigned in Kronos."),
  clock_library_code STRING NOT NULL OPTIONS(description="Code for each clock library."),
  kronos_pay_code_seq_num INT64 NOT NULL OPTIONS(description="This denotes each type of pay code and hours that can be attributed to each empoyees shift."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically."),
  valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated."),
  kronos_pay_code STRING OPTIONS(description="Code that defines each type of pay (e.g. productive, bereavement, etc.)"),
  rounded_clocked_hour_num NUMERIC OPTIONS(description="Contains the total amount of all productive time charged to each pay code."),
  pay_summary_group_code STRING OPTIONS(description="Code that groups pay codes together."),
  lawson_company_num INT64 NOT NULL OPTIONS(description="The number that identifies a company.A company represents a business or legal entity of an organization"),
  process_level_code STRING NOT NULL OPTIONS(description="Unique process level code of an HR company value mainatined in this field."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_num, kronos_num, clock_library_code, kronos_pay_code_seq_num
OPTIONS(
  description="Contains the breakdown of each employees pay summary group codes per time record."
);