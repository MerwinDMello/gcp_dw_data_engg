-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payor_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_eom AS SELECT
    payor_eom.pe_date,
    payor_eom.payor_dw_id,
    payor_eom.coid,
    payor_eom.company_code,
    payor_eom.iplan_id,
    payor_eom.iplan_financial_class_code,
    payor_eom.sub_payor_group_id,
    payor_eom.major_payor_group_id,
    payor_eom.payor_sid,
    payor_eom.payor_id,
    payor_eom.unit_num,
    payor_eom.plan_desc,
    payor_eom.sub_payor_group_desc,
    payor_eom.major_payor_group_desc,
    payor_eom.payor_gen_02_code,
    payor_eom.bankrupt_payor_ind,
    payor_eom.auto_payor_ind,
    payor_eom.mcaid_pending_payor_ind,
    payor_eom.major_payor_group_unique_num,
    payor_eom.calc_rev_cat_financial_class_code,
    payor_eom.meditech_mnemonic,
    payor_eom.payer_type_code,
    payor_eom.eff_from_date,
    payor_eom.eff_to_date,
    payor_eom.source_system_code,
    payor_eom.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.payor_eom
;
