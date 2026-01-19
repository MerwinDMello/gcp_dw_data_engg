CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_hr_credential (
credential_code STRING NOT NULL OPTIONS(description="This field maintains credential code")
, credential_type_code STRING NOT NULL OPTIONS(description="This field captures credential type code")
, credential_group_desc STRING OPTIONS(description="This field maintain credential code group description")
, credential_desc STRING OPTIONS(description="This field maintains credential code description")
, credential_report_desc STRING OPTIONS(description="This field maintains description of credential report")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Credential_Code, Credential_Type_Code
OPTIONS(description="This table maintains credential information of physician based on a custom logic.");
