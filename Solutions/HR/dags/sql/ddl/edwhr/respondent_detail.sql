
CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.respondent_detail (
respondent_id NUMERIC NOT NULL OPTIONS(description="This is the respondent identifier that is assigned by the vendor.")
, survey_receive_date DATE NOT NULL OPTIONS(description="The date the survey was received")
, respondent_type_code STRING NOT NULL OPTIONS(description="A one character code indicating the type of the respondent.")
, survey_sid INT64 NOT NULL OPTIONS(description="It is the ETL generated unique sequence number for each survey.")
, respondent_3_4_id STRING OPTIONS(description="The respondents 3-4 if its an employee engagement.")
, employee_num STRING OPTIONS(description="This is a lawson employee number")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY respondent_id, respondent_type_code, survey_sid
OPTIONS(description="This table contains the details of the respondents who responded to the survey.");
