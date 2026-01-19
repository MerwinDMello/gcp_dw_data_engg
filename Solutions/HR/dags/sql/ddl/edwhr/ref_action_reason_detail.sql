CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_action_reason_detail (
action_reason_text STRING NOT NULL OPTIONS(description="Reason description from Lawson or Mission of an action taken on an employee.")
, mapped_action_reason_text STRING OPTIONS(description="These are the reason descriptions either straight from Lawson or if originally from Mission they have been mapped to Lawson values.")
, action_reason_group_text STRING OPTIONS(description="A subcategory of action reasons that describes the category the reason falls into.")
, action_reason_sub_group_text STRING OPTIONS(description="A more granular category to define the action reason.")
, action_reason_desc STRING OPTIONS(description="Description of the action reason code from Lawson")
, governance_group_desc STRING OPTIONS(description="Describes whether the action counts towards certain standardized metrics.")
, action_reason_type_desc STRING OPTIONS(description="Specifies whether it was an internal or external action.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Action_Reason_Text
OPTIONS(description="This table is loaded and maintained by the business and contains several mapped fields with detail about each action reason from Lawson.");
