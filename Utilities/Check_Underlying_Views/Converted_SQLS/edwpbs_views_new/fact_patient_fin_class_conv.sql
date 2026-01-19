-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_patient_fin_class_conv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient_fin_class_conv AS SELECT
    a.company_code,
    a.coid,
    a.patient_dw_id,
    a.month_id,
    a.unit_num,
    a.pat_acct_num,
    a.patient_type_code,
    a.initial_financial_class_code,
    a.final_financial_class_code,
    a.financial_class_code_initial_date,
    a.financial_class_code_final_date,
    a.total_charge_amt,
    a.source_system_code,
    a.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient_fin_class_conv AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
