CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.recruitment_user (
recruitment_user_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each user.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically.")
, recruitment_user_num INT64 OPTIONS(description="A unique number for each user coming from the source.")
, valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated.")
, employee_num INT64 OPTIONS(description="This is an lawson employee number")
, employee_34_login_code STRING OPTIONS(description="The 3-4 id of the employee.")
, first_name STRING OPTIONS(description="First name of the user.")
, last_name STRING OPTIONS(description="Last name of the user.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Recruitment_User_SID
OPTIONS(description="Will contain details associated with the users that take an active role in the recruitment of a candidate.");

