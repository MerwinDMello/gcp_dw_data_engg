-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payor_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_eom
   OPTIONS(description='Payor end of month snap shot table which contains monthly changes to payor members and keeps history')
  AS SELECT
      payor_eom.pe_date,
      ROUND(payor_eom.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      payor_eom.coid,
      payor_eom.company_code,
      payor_eom.iplan_id,
      ROUND(payor_eom.iplan_financial_class_code, 0, 'ROUND_HALF_EVEN') AS iplan_financial_class_code,
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
      ROUND(payor_eom.calc_rev_cat_financial_class_code, 0, 'ROUND_HALF_EVEN') AS calc_rev_cat_financial_class_code,
      payor_eom.meditech_mnemonic,
      payor_eom.payer_type_code,
      payor_eom.eff_from_date,
      payor_eom.eff_to_date,
      payor_eom.source_system_code,
      payor_eom.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.payor_eom
  ;
