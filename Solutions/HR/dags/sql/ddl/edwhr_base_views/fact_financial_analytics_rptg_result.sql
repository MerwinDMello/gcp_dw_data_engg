-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_Base_Views/fact_financial_analytics_rptg_result.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.fact_financial_analytics_rptg_result AS SELECT
    fact_financial_analytics_rptg_result.org_code,
    fact_financial_analytics_rptg_result.scenario_code,
    fact_financial_analytics_rptg_result.year_id,
    fact_financial_analytics_rptg_result.time_code,
    fact_financial_analytics_rptg_result.lob_code,
    fact_financial_analytics_rptg_result.fa_measure_code,
    fact_financial_analytics_rptg_result.company_code,
    fact_financial_analytics_rptg_result.coid,
    fact_financial_analytics_rptg_result.fa_value_amt,
    fact_financial_analytics_rptg_result.parallon_usage_ind,
    fact_financial_analytics_rptg_result.asd_usage_ind,
    fact_financial_analytics_rptg_result.hr_usage_ind,
    fact_financial_analytics_rptg_result.spa_usage_ind,
    fact_financial_analytics_rptg_result.supply_chain_usage_ind,
    fact_financial_analytics_rptg_result.financial_services_usage_ind,
    fact_financial_analytics_rptg_result.source_system_code,
    fact_financial_analytics_rptg_result.dw_last_update_date_time
  FROM
    {{ params.param_fs_base_views_dataset_name }}.fact_financial_analytics_rptg_result
  WHERE upper(fact_financial_analytics_rptg_result.company_code) = 'H'
;
