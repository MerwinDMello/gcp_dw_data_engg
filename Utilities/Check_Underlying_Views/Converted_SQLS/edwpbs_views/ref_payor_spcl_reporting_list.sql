-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_payor_spcl_reporting_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_payor_spcl_reporting_list AS SELECT
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
    a.company_code,
    a.diag_code,
    a.facility_state_code,
    a.eff_from_date,
    a.eff_to_date,
    a.source_system_code,
    a.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_payor_spcl_report_list AS a
;
