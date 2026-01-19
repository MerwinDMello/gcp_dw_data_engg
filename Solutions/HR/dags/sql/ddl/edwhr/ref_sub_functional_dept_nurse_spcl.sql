CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_sub_functional_dept_nurse_spcl (
sub_functional_dept_num STRING NOT NULL OPTIONS(description="Six character number that represents each sub-functional department.")
, functional_dept_num STRING NOT NULL OPTIONS(description="Six character number that represents each functional department.")
, nurse_specialty_desc STRING OPTIONS(description="The nursing specialty for nurses in the sub-functional/functional department.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY Sub_Functional_Dept_Num, Functional_Dept_Num
OPTIONS(description="Contains the nursing specialty for each functional/sub-functional department. This is manually maintained by the business.");
