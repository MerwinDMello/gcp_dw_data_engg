CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_hr_parameter (
source_parameter_code STRING NOT NULL OPTIONS(description="The code or value coming from the source.")
, source_code STRING NOT NULL OPTIONS(description="A code that identifies the source or the source instant,")
, parameter_type_text STRING NOT NULL OPTIONS(description="The type of parameter.")
, parameter_category_text STRING NOT NULL OPTIONS(description="The category for each parameter.")
, eff_from_date DATE NOT NULL OPTIONS(description="The date the parameter mapping is effective from.")
, eff_to_date DATE OPTIONS(description="The date the parameter mapping is effective to.")
, source_parameter_desc STRING OPTIONS(description="The description of the parameter coming from the source.")
, standardized_alias_name STRING NOT NULL OPTIONS(description="The standardized value that the parameter has been mapped to.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
PARTITION BY Eff_From_Date
CLUSTER BY Source_Parameter_Code, Source_Code, Parameter_Type_Text, Parameter_Category_Text
OPTIONS(description="Used to take different values from each individual survey and standardize them to a given value.");
