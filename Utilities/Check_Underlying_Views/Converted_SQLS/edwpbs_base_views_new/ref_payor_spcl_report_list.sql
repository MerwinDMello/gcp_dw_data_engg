-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_payor_spcl_report_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_payor_spcl_report_list AS SELECT
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
    `hca-hin-dev-cur-parallon`.edwpbs.ref_payor_spcl_report_list
;
