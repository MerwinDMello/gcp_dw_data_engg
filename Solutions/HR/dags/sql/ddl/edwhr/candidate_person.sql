CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.candidate_person
(
candidate_sid INT64 NOT NULL OPTIONS(description="Unique identifier for each candidate.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded.")
, first_name STRING OPTIONS(description="First name of the candidate.")
, middle_name STRING OPTIONS(description="Middle name of the candidate.")
, last_name STRING OPTIONS(description="Last name of the candidate.")
, maiden_name STRING OPTIONS(description="Maiden name of a candidate.")
, social_security_num STRING OPTIONS(description="Domain to mask the Social Security Number during the Security View generation.")
, email_address STRING OPTIONS(description="Email address of the candidate.")
, birth_date DATE OPTIONS(description="The date of birth of the candidate.")
, driver_license_num STRING OPTIONS(description="The driver license number of the candidate.")
, driver_license_state_code STRING OPTIONS(description="Issuing state for the driver license.")
, valid_to_date DATETIME OPTIONS(description="Date the record is valid to based on when it was loaded.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY candidate_sid
OPTIONS(description="Table contains the demographic data of the candidate.");