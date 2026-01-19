/*edwhr.survey_response_master*/
CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.survey_response_master
(
  survey_response_sid INT64 NOT NULL OPTIONS(description="It is the ETL generated unique sequence number for each response to a survey question.")
, eff_from_date DATE NOT NULL OPTIONS(description="Effective start date of a record")
, survey_question_sid INT64 NOT NULL OPTIONS(description="It is the ETL generated unique sequence number for each survey question.")
, response_value_text STRING NOT NULL OPTIONS(description="This is the response value to a survey question")
, response_label_text STRING OPTIONS(description="This is the description for the response to a survey question.")
, response_label_extended_text STRING OPTIONS(description="This is the extended description for the response to a survey question.")
, eff_to_date DATE NOT NULL OPTIONS(description="Record effective end date")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY eff_from_date
CLUSTER BY survey_response_sid
OPTIONS(description="This table records all the valid responses to a survey question.");
