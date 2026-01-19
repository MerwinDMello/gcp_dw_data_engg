CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.candidate_onboarding_resource
(
resource_screening_package_num INT64 NOT NULL OPTIONS(description="Unique identifier from the source given to each candidate.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date the record is valid from based on when it was loaded.")
, candidate_sid INT64 OPTIONS(description="Unique identifier for each candidate.")
, recruitment_requisition_sid INT64 OPTIONS(description="A unique identifier for each requisition coming from the recruitment system.")
, valid_to_date DATETIME NOT NULL OPTIONS(description="Date on which the record was invalidated.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY resource_screening_package_num
OPTIONS(description="This table contains information about a candidate that any screening (background, drug) is ordered for.");