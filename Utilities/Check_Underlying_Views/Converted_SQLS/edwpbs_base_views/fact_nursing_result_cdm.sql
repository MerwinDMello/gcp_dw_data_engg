-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_nursing_result_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_nursing_result_cdm AS SELECT
    nur_qry_rslt_dtl.vld_to_ts,
    ROUND(nur_qry_rslt_dtl.clnc_fnd_sk, 0, 'ROUND_HALF_EVEN') AS clinical_finding_sk,
    ROUND(nur_qry_rslt_dtl.clnc_ordr_sk, 0, 'ROUND_HALF_EVEN') AS clinical_order_sk,
    nur_qry_rslt_dtl.rslt_cd_sk AS nursing_query_sk,
    nur_qry_rslt_dtl.vld_fr_ts AS valid_from_date_time,
    ROUND(nur_qry_rslt_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    nur_qry_rslt_dtl.company_code,
    nur_qry_rslt_dtl.coid,
    nur_qry_rslt_dtl.qry_id_txt AS nursing_query_id_txt,
    nur_qry_rslt_dtl.qry_subid_txt AS nursing_query_subid_txt,
    nur_qry_rslt_dtl.qry_occr_ts AS nursing_query_occr_date_time,
    nur_qry_rslt_dtl.qry_rptd_ts AS nursing_query_rpt_date_time,
    nur_qry_rslt_dtl.qry_rslt_sts_ref_cd AS nursing_result_status_code,
    nur_qry_rslt_dtl.qry_val_unt_type_cd AS nursing_result_unit_type_code,
    nur_qry_rslt_dtl.qry_val_txt AS nursing_result_val_txt,
    ROUND(nur_qry_rslt_dtl.clnc_instnc_sk, 0, 'ROUND_HALF_EVEN') AS clinical_instance_sk,
    nur_qry_rslt_dtl.src_sys_orgnl_cd AS source_system_original_code,
    nur_qry_rslt_dtl.effv_fr_dt AS effective_from_date,
    nur_qry_rslt_dtl.src_sys_ref_cd AS source_system_txt,
    nur_qry_rslt_dtl.msg_ctrl_id_txt AS message_control_id_text,
    nur_qry_rslt_dtl.dw_insrt_ts AS dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.nur_qry_rslt_dtl
;
