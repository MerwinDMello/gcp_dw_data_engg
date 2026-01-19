/*edwhr.ref_integrated_lob*/


CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_integrated_lob (
integrated_lob_id INT64 NOT NULL OPTIONS(description="This field captures ETL generated unique sequence number for each LOB description")
, category_desc STRING NOT NULL OPTIONS(description="This field maintains integrated Line of business category description")
, sub_category_desc STRING NOT NULL OPTIONS(description="This field maintains integrated Line of business sub category description")
, process_level_code STRING OPTIONS(description="This field maintains process level code of an facility")
, dept_code STRING OPTIONS(description="This field maintains department code of process level.")
, lob_code STRING OPTIONS(description="This field maintains line of business code")
, sub_lob_code STRING OPTIONS(description="This field maintains sub line of business code")
, functional_dept_desc STRING OPTIONS(description="This field maintains function description of an line of business")
, sub_functional_dept_desc STRING OPTIONS(description="This field maintains sub function description of an line of business")
, match_level_num INT64 OPTIONS(description="Match level is defined based on number of department have same kind of code")
, match_level_desc STRING OPTIONS(description="This field maintains match level description.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")
)
CLUSTER BY integrated_lob_id
OPTIONS(description="This table maintains all integrated line of business details .");
