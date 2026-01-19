-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_payor_spcl_report_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_payor_spcl_report_list
   OPTIONS(description='Reference table containing the list of reporting related instructions used in Payor Specialization Report List')
  AS SELECT
      ref_payor_spcl_report_list.report_list_sid,
      ref_payor_spcl_report_list.report_list_code,
      ref_payor_spcl_report_list.unit_num,
      ref_payor_spcl_report_list.report_parm_txt,
      ref_payor_spcl_report_list.business_owner_comment_txt,
      ref_payor_spcl_report_list.business_owner_name,
      ref_payor_spcl_report_list.iplan_id,
      ref_payor_spcl_report_list.group_id,
      ref_payor_spcl_report_list.procedure_code,
      ref_payor_spcl_report_list.revenue_code,
      ref_payor_spcl_report_list.federal_tax_id,
      ref_payor_spcl_report_list.ssc_coid,
      ref_payor_spcl_report_list.coid,
      ref_payor_spcl_report_list.company_code,
      ref_payor_spcl_report_list.diag_code,
      ref_payor_spcl_report_list.facility_state_code,
      ref_payor_spcl_report_list.eff_from_date,
      ref_payor_spcl_report_list.eff_to_date,
      ref_payor_spcl_report_list.source_system_code,
      ref_payor_spcl_report_list.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_payor_spcl_report_list
  ;
