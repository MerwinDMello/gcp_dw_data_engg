CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.employee_engagement_question_index (
survey_question_sid INT64 NOT NULL OPTIONS(description="It is the ETL generated unique sequence number for each survey question.")
, eff_from_date DATE NOT NULL OPTIONS(description="Effective start date of a record.")
, index_question_id INT64 OPTIONS(description="The index identifier that each question will roll up to if they are the same.")
, survey_sid INT64 OPTIONS(description="It is the ETL generated unique sequence number for each survey.")
, question_id STRING OPTIONS(description="This is the identifier for the question from the source.")
, survey_category_code STRING OPTIONS(description="This is the unique survey identifier from the source.")
, survey_year_num INT64 OPTIONS(description="The year of the survey.")
, eff_to_date DATE OPTIONS(description="Record effective end date")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY Eff_From_Date
CLUSTER BY Survey_Question_SID, Eff_From_Date
OPTIONS(description="This table will hold the question identifiers for each years survey that represent the same question. It will be used to compare answers to the same question over different years.");
