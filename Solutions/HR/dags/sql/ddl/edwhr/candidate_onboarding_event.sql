CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.candidate_onboarding_event
(
candidate_onboarding_event_sid INT64 NOT NULL OPTIONS(description="It  is the unique System generated identifier for each candidate obboarding event, This is the combination of Requisition Number , Process Level Code and Event Type Code")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically.")
, event_type_id STRING NOT NULL OPTIONS(description="Unique identifier for each event type ")
, recruitment_requisition_num_text STRING OPTIONS(description="A unique value coming from the source that combines the location code and lawson requisition number.")
, valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated.")
, completed_date DATETIME OPTIONS(description="This contains candidates event completion date for the events like tour, drug screening")
, candidate_sid INT64 OPTIONS(description="Unique identifier for each candidate.")
, resource_screening_package_num INT64 OPTIONS(description="Unique identifier from the source given to each candidate.")
, sequence_num INT64 OPTIONS(description="If multiple records are created in Lawson for the  same effective date field will be incremented for each  record for the date.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Datetime of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY candidate_onboarding_event_sid
OPTIONS(description="This table contains candidates onboarding event  information like candidate tour and drug screening completion.");