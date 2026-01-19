-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_ed_query_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_ed_query_cdm AS SELECT
    ed_qry_rslt_dtl.clnc_fnd_sk AS clinical_finding_sk,
    ed_qry_rslt_dtl.clnc_ordr_sk AS clinical_order_sk,
    ed_qry_rslt_dtl.rslt_cd_sk AS ed_query_sk,
    ed_qry_rslt_dtl.vld_fr_ts AS valid_from_date_time,
    ed_qry_rslt_dtl.patient_dw_id AS patient_dw_id,
    ed_qry_rslt_dtl.company_code,
    ed_qry_rslt_dtl.coid,
    ed_qry_rslt_dtl.qry_type_ref_cd AS ed_query_type_code,
    ed_qry_rslt_dtl.qry_id_txt AS ed_query_id_txt,
    ed_qry_rslt_dtl.qry_subid_txt AS ed_query_subid_txt,
    ed_qry_rslt_dtl.qry_occr_ts AS ed_query_occr_date_time,
    ed_qry_rslt_dtl.qry_rptd_ts AS ed_query_rpt_date_time,
    ed_qry_rslt_dtl.qry_rslt_sts_ref_cd AS ed_query_result_status_code,
    ed_qry_rslt_dtl.qry_val_unt_type_cd AS ed_query_value_unit_type_code,
    ed_qry_rslt_dtl.qry_val_txt AS ed_query_value_txt,
    ed_qry_rslt_dtl.src_sys_orgnl_cd AS source_system_original_code,
    ed_qry_rslt_dtl.effv_fr_dt AS effective_from_date_time,
    ed_qry_rslt_dtl.src_sys_ref_cd AS source_system_txt,
    ed_qry_rslt_dtl.dw_insrt_ts AS dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.ed_qry_rslt_dtl
  WHERE ed_qry_rslt_dtl.vld_to_ts = '9999-12-31 00:00:00'
;
