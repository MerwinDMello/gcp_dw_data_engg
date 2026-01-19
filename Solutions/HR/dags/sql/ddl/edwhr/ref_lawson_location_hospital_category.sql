CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_lawson_location_hospital_category
(
lawson_location_code STRING NOT NULL OPTIONS(description="This field maintains lawson location code")
, hospital_category_code_year INT64 NOT NULL OPTIONS(description="The year of the level code.")
, hospital_category_code STRING OPTIONS(description="It captures current level of Facility")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY lawson_location_code, hospital_category_code_year
OPTIONS(description="This table captures Facility level information comprised of Location code and facility level. This is determined based on the revenue and rules set by the business.");