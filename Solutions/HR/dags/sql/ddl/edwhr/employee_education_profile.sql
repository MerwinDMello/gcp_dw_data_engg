CREATE TABLE IF NOT EXISTS  {{ params.param_hr_core_dataset_name }}.employee_education_profile
(
  employee_education_profile_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each education record associated with an employee."),
  employee_education_type_code STRING NOT NULL OPTIONS(description="This field identifies whether employee detail type code is numeric, alphanumeric or date field."),
  valid_from_date DATETIME NOT NULL OPTIONS(description="Record effective start date"),
  employee_talent_profile_sid INT64 OPTIONS(description="This field maintains unique sequnce number generated for each employee talent profile"),
  employee_sid INT64 OPTIONS(description="Unique numeric number generated for unique combination of Employee Identifier and HR Company Identifier"),
  detail_value_alpahnumeric_text STRING OPTIONS(description="This field maintains alpahnumeric value"),
  detail_value_num NUMERIC OPTIONS(description="this field is responsible to maintain all numeric information of an employee. Example age, numbe of years etc"),
  detail_value_date DATE OPTIONS(description="This field maintains date value"),
  employee_num INT64 OPTIONS(description="This is an lawson employee number"),
  lawson_company_num INT64 OPTIONS(description="The number that identifies a company.A company represents a business or legal entity of an organization"),
  process_level_code STRING OPTIONS(description="Unique process level code of an HR company value mainatined in this field."),
  valid_to_date DATETIME OPTIONS(description="The date the record was no longer active."),
  source_system_key STRING OPTIONS(description="A unique record identifier coming from the source."),
  source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated."),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(valid_from_date)
CLUSTER BY employee_education_profile_sid, employee_education_type_code
OPTIONS(description="This table maintain employee education, certification and training details.");
