-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_financial_analytics_rptg_result.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_financial_analytics_rptg_result AS SELECT
    fact_financial_analytics_rptg_result.org_code,
    fact_financial_analytics_rptg_result.scenario_code,
    fact_financial_analytics_rptg_result.year_id,
    fact_financial_analytics_rptg_result.time_code,
    fact_financial_analytics_rptg_result.lob_code,
    fact_financial_analytics_rptg_result.fa_measure_code,
    fact_financial_analytics_rptg_result.company_code,
    fact_financial_analytics_rptg_result.coid,
    ROUND(fact_financial_analytics_rptg_result.fa_value_amt, 5, 'ROUND_HALF_EVEN') AS fa_value_amt,
    fact_financial_analytics_rptg_result.parallon_usage_ind,
    fact_financial_analytics_rptg_result.asd_usage_ind,
    fact_financial_analytics_rptg_result.hr_usage_ind,
    fact_financial_analytics_rptg_result.spa_usage_ind,
    fact_financial_analytics_rptg_result.supply_chain_usage_ind,
    fact_financial_analytics_rptg_result.financial_services_usage_ind,
    fact_financial_analytics_rptg_result.source_system_code,
    fact_financial_analytics_rptg_result.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwfs_base_views.fact_financial_analytics_rptg_result
;
