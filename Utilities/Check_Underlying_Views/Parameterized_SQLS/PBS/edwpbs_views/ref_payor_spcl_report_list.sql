-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_payor_spcl_report_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.ref_payor_spcl_report_list
   OPTIONS(description='Reference table containing the list of reporting related instructions used in Payor Specialization Report List')
  AS SELECT
      a.report_list_sid,
      a.report_list_code,
      a.unit_num,
      a.report_parm_txt,
      a.business_owner_comment_txt,
      a.business_owner_name,
      a.iplan_id,
      a.group_id,
      a.procedure_code,
      a.revenue_code,
      a.federal_tax_id,
      a.ssc_coid,
      a.coid,
      a.company_code,
      a.diag_code,
      a.facility_state_code,
      a.eff_from_date,
      a.eff_to_date,
      a.source_system_code,
      a.dw_last_update_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.ref_payor_spcl_report_list AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
