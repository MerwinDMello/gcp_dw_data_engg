CREATE  TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.question_legacy_survey_xwalk
(
question_id STRING NOT NULL OPTIONS(description="This is the identifier for the question.")
, measure_id_text STRING OPTIONS(description="This is a grouping for some of the questions; This is a static list maintained by the CSG.")
, legacy_question_id INT64 OPTIONS(description="This is the question ID from the legacy patient satisfaction process.")
, domain_id INT64 OPTIONS(description="This is the domain identifier assigned by the vendor.")
, domain_group_id INT64 OPTIONS(description="This is the domain identifier assigned by the vendor.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Question_Id
OPTIONS(description="This table is a one time load to cross walk questions from new vendor to ones from the previous vendor");