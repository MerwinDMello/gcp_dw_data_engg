-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_lab_test_result_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_lab_test_result_cdm AS SELECT
    ROUND(fnd.clnc_fnd_sk, 0, 'ROUND_HALF_EVEN') AS clinical_finding_sk,
    ROUND(fnd.clnc_ordr_sk, 0, 'ROUND_HALF_EVEN') AS clinical_order_sk,
    fnd.lab_tst_cd_sk AS lab_test_sk,
    fnd.vld_fr_ts AS valid_from_date_time,
    ROUND(fnd.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    fnd.company_code AS company_code,
    fnd.coid AS coid,
    fnd.lab_tst_type_ref_cd AS lab_test_result_type_code,
    fnd.lab_tst_id_txt AS lab_test_id_txt,
    fnd.lab_tst_subid_txt AS lab_test_subid_txt,
    fnd.lab_tst_coll_ts AS lab_test_coll_date_time,
    fnd.lab_tst_rptd_ts AS lab_test_rpt_date_time,
    fnd.lab_tst_rslt_sts_ref_cd AS lab_test_result_status_code,
    fnd.lab_tst_val_nmrc_ind AS lab_test_result_numeric_ind,
    fnd.lab_tst_val_unt_type_cd AS lab_test_result_unit_type_code,
    fnd.lab_tst_val_txt AS lab_test_result_val_txt,
    ROUND(fnd.lab_tst_val_num, 6, 'ROUND_HALF_EVEN') AS lab_test_result_val_num,
    fnd.ref_rng_low AS range_low,
    fnd.ref_rng_hi AS range_high,
    fnd.ref_rng_txt AS range_txt,
    fnd.ntrt_of_abnrml_tst_ref_cd AS nature_of_abnormal_test_code,
    fnd.abnrml_flg_nm AS abnormal_ind,
    fnd.effv_fr_dt AS effective_from_date,
    fnd.src_sys_orgnl_cd AS source_system_original_code,
    fnd.src_sys_ref_cd AS source_system_txt,
    fnd.dw_insrt_ts AS dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.lab_tst_rslt_dtl AS fnd
  WHERE fnd.vld_to_ts = datetime(TIMESTAMP '9999-12-31 00:00:00', 'US/Central')
;
