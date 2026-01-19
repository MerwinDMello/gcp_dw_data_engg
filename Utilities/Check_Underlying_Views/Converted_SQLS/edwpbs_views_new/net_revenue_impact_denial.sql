-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/net_revenue_impact_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.net_revenue_impact_denial AS SELECT
    a.reporting_date,
    a.patient_dw_id,
    a.iplan_id,
    a.iplan_insurance_order_num,
    a.payor_dw_id,
    a.company_code,
    a.coid,
    a.unit_num,
    a.pat_acct_num,
    a.financial_class_code,
    a.denial_status_code,
    a.patient_type_code,
    a.write_off_denial_amt,
    a.overturned_denial_amt,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.net_revenue_impact_denial AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
