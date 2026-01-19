-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_financial_analytics_rptg_result.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_financial_analytics_rptg_result AS SELECT
    a.org_code,
    a.scenario_code,
    a.year_id,
    a.time_code,
    a.lob_code,
    a.fa_measure_code,
    a.company_code,
    a.coid,
    ROUND(a.fa_value_amt, 5, 'ROUND_HALF_EVEN') AS fa_value_amt,
    a.parallon_usage_ind,
    a.asd_usage_ind,
    a.hr_usage_ind,
    a.spa_usage_ind,
    a.supply_chain_usage_ind,
    a.financial_services_usage_ind,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_financial_analytics_rptg_result AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS sf ON upper(a.coid) = upper(sf.co_id)
     AND sf.user_id = session_user()
;
