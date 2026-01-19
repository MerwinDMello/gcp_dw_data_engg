-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payor_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payor_eom AS SELECT
    a.pe_date,
    a.payor_dw_id,
    a.coid,
    a.company_code,
    a.iplan_id,
    a.iplan_financial_class_code,
    a.sub_payor_group_id,
    a.major_payor_group_id,
    a.payor_sid,
    a.payor_id,
    a.unit_num,
    a.plan_desc,
    a.sub_payor_group_desc,
    a.major_payor_group_desc,
    a.payor_gen_02_code,
    a.bankrupt_payor_ind,
    a.auto_payor_ind,
    a.mcaid_pending_payor_ind,
    a.major_payor_group_unique_num,
    a.calc_rev_cat_financial_class_code,
    a.meditech_mnemonic,
    a.payer_type_code,
    a.eff_from_date,
    a.eff_to_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_eom AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
