/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.fact_fa_rptg_result_hr AS SELECT
      a.org_code,
      a.scenario_code,
      a.year_id,
      a.time_code,
      a.lob_code,
      a.fa_measure_code,
      a.company_code,
      a.coid,
      a.fa_value_amt,
      a.parallon_usage_ind,
      a.asd_usage_ind,
      a.hr_usage_ind,
      a.spa_usage_ind,
      a.supply_chain_usage_ind,
      a.financial_services_usage_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.fact_financial_analytics_rptg_result AS a
    WHERE upper(a.hr_usage_ind) = 'Y'
  ;

/*
INNER JOIN EDWCP_BASE_VIEWS.SecRef_Facility B 
ON A.Company_Code = B.Company_Code 
AND A.Coid = B.Co_id 
AND B.User_Id = CURRENT_USER*/