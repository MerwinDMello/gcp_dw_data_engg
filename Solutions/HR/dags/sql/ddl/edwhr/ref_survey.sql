
CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_survey (
survey_sid INT64 NOT NULL OPTIONS(description="It is the ETL generated unique sequence number for each survey.")
, eff_from_date DATE NOT NULL OPTIONS(description="Effective start date of a record")
, survey_category_num INT64 NOT NULL OPTIONS(description="This is the category number for the survey. Category number, code and category text define a unique Survey,")
, survey_category_code STRING NOT NULL OPTIONS(description="Is the Category Code for the survey. Category number, code and category text define a unique Survey,")
, survey_category_text STRING NOT NULL OPTIONS(description="Is the Category text for the Survey. Category number, code and category text define a unique Survey,")
, eff_to_date DATE NOT NULL OPTIONS(description="Effective end date of a record")
, survey_group_text STRING NOT NULL OPTIONS(description="This is the grouping for the surveys.")
, survey_date DATE OPTIONS(description="The date of the survey.")
, survey_start_date DATE OPTIONS(description="The date the survey was open to the participants.")
, survey_end_date DATE OPTIONS(description="The date the survey was closed to the participants.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY eff_from_date
CLUSTER BY survey_sid
OPTIONS(description="This table defines the details about the survey.");
