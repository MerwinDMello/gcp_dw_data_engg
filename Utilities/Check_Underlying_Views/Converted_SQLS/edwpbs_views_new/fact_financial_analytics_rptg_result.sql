-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_financial_analytics_rptg_result AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS sf ON a.coid = sf.co_id
     AND sf.user_id = session_user()
;
