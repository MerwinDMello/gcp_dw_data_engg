CREATE TABLE IF NOT EXISTS {{ params.param_fs_core_dataset_name }}.fact_financial_analytics_rptg_result (
org_code STRING NOT NULL
, scenario_code STRING NOT NULL
, year_id INT64 NOT NULL
, time_code STRING NOT NULL
, lob_code STRING NOT NULL
, fa_measure_code STRING NOT NULL
, company_code STRING NOT NULL
, coid STRING
, fa_value_amt NUMERIC(18,5)
, parallon_usage_ind STRING NOT NULL
, asd_usage_ind STRING NOT NULL
, hr_usage_ind STRING NOT NULL
, spa_usage_ind STRING NOT NULL
, supply_chain_usage_ind STRING NOT NULL
, financial_services_usage_ind STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
 CLUSTER BY Org_Code, Scenario_Code, Year_Id, Time_Code
;
