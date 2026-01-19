CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.hr_workunit_variable (
workunit_sid NUMERIC(18,0) NOT NULL OPTIONS(description="Unique identifier generated of the combination of Workunit and Source_System_Code.")
, variable_name STRING NOT NULL OPTIONS(description="Cotains the variable name.")
, variable_seq_num INT64 NOT NULL OPTIONS(description="Contains the variable sequence number.")
, valid_from_date DATETIME NOT NULL OPTIONS(description="Date on which the record became valid. Load date typically.")
, valid_to_date DATETIME OPTIONS(description="Date on which the record was invalidated.")
, workunit_num NUMERIC(12,0) NOT NULL OPTIONS(description="Categories each activity number into a separate group to help understand the workflow.")
, variable_type_num INT64 OPTIONS(description="Contains the variable type.")
, variable_value_text STRING OPTIONS(description="Contains the value for each variable.")
, lawson_company_num INT64 NOT NULL OPTIONS(description="The number that identifies a company.A company represents a business or legal entity of an organization")
, process_level_code STRING NOT NULL OPTIONS(description="Unique process level code of an HR company value maintained in this field.")
, active_dw_ind STRING NOT NULL OPTIONS(description="Y/N character to indicate this record as active in the EDW.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY DATE(Valid_From_Date)
CLUSTER BY Workunit_SID, Variable_Name, Variable_Seq_Num, Valid_From_Date
OPTIONS(description="This table cotains the variable data associated to the work unit.");
