/*edwhr.ref_coid_hospital_category*/
CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_coid_hospital_category
(
  coid STRING NOT NULL OPTIONS(description="The company identifier which uniquely identifies a facility to corporate and all other facilities.")
, company_code STRING NOT NULL OPTIONS(description="Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes")
, year_num INT64 NOT NULL OPTIONS(description="The year of the level code.")
, hospital_level_code STRING OPTIONS(description="It captures current level of Facility. A to G is a net revenue based level/size of a given hospital. A being the smallest and G the largest")
, prev_year_change_ind STRING OPTIONS(description="A column to indicate if a Facility Hospital Category (Hospital_Level) changed from previous year. Values Y/N.")
, manual_hold_ind STRING OPTIONS(description="Indicates (Y/N) if the Hospital_Level is a Manual Hold for any given year")
, quartile_rank_num INT64 OPTIONS(description="Denotes quartile the hospital falls in based on EBITDA.")
, total_rank_num INT64 OPTIONS(description="Denotes rank of the hospital based on EBITDA.")
, source_system_code STRING NOT NULL OPTIONS(description="A one character code indicating the specific source system from which the data originated.")
, dw_last_update_date_time DATETIME NOT NULL OPTIONS(description="Timestamp of update or load of this record to the Enterprise Data Warehouse.")

)
CLUSTER BY coid, company_code, year_num
OPTIONS(description="This table captures the COID level information comprised of Year and Hospital level. This is determined based on the revenue and rules set by the business.");