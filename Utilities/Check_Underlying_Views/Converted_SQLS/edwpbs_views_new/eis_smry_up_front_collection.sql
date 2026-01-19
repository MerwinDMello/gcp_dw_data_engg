-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_smry_up_front_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.eis_smry_up_front_collection AS SELECT
    a.unit_num_sid,
    a.patient_type_sid,
    a.patient_financial_class_sid,
    a.payor_sid,
    a.account_status_sid,
    a.time_id,
    a.up_front_msr_sid,
    a.company_code,
    a.coid,
    a.transaction_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_smry_up_front_collection AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.coid = b.co_id
     AND a.company_code = b.company_code
     AND b.user_id = session_user()
;
