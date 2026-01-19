CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.recruitment_requisition_history (
recruitment_requisition_sid INT64 NOT NULL OPTIONS(description="A unique identifier for each requisition coming from the recruitment system.")
, creation_date_time DATETIME NOT NULL OPTIONS(description="Date the requisition was created.")
, requisition_status_id INT64 NOT NULL OPTIONS(description="The status of the requisition")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically.")
, valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated.")
, closed_date_time DATETIME OPTIONS(description="The date the requisition was closed.")
, requisition_creator_user_sid INT64 OPTIONS(description="A link to Recruitment_User to identify who created the requisition.")
, recruiter_owner_user_sid INT64 OPTIONS(description="A link to Recruitment_User to identify who is the owner of the requisition.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY recruitment_requisition_sid
OPTIONS(description="Contains the history of the requisition coming out of the recruitment source.");