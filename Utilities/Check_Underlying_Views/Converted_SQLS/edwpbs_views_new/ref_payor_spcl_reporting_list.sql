-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
