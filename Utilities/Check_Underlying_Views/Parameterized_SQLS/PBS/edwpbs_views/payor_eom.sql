-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payor_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.payor_eom
   OPTIONS(description='Payor end of month snap shot table which contains monthly changes to payor members and keeps history')
  AS SELECT
      a.pe_date,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.coid,
      a.company_code,
      a.iplan_id,
      ROUND(a.iplan_financial_class_code, 0, 'ROUND_HALF_EVEN') AS iplan_financial_class_code,
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
      ROUND(a.calc_rev_cat_financial_class_code, 0, 'ROUND_HALF_EVEN') AS calc_rev_cat_financial_class_code,
      a.meditech_mnemonic,
      a.payer_type_code,
      a.eff_from_date,
      a.eff_to_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.payor_eom AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
