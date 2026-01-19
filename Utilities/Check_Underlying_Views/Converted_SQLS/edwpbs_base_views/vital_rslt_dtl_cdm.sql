-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/vital_rslt_dtl_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.vital_rslt_dtl_cdm AS SELECT
    ROUND(a.clnc_fnd_sk, 0, 'ROUND_HALF_EVEN') AS clnc_fnd_sk,
    a.vld_fr_ts,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    a.company_code,
    a.coid,
    a.vld_to_ts,
    ROUND(a.clnc_ordr_sk, 0, 'ROUND_HALF_EVEN') AS clnc_ordr_sk,
    a.vital_id_txt,
    a.vital_occr_ts,
    a.vital_rptd_ts,
    a.vital_rslt_sts_ts,
    a.vital_rslt_sts_ref_cd,
    a.vital_val_nmrc_ind,
    a.vital_val_unt_type_cd,
    a.vital_val_txt,
    ROUND(a.vital_val_num, 6, 'ROUND_HALF_EVEN') AS vital_val_num,
    a.ref_rng_low,
    a.ref_rng_hi,
    a.ref_rng_txt,
    a.ntrt_of_abnrml_tst_ref_cd,
    a.obsrv_methd_cd,
    a.src_sys_orgnl_cd,
    a.cnfd_cd_sk,
    a.vital_cd_sk,
    a.effv_fr_dt,
    a.effv_to_dt,
    a.type_ref_cd,
    a.src_sys_ref_cd,
    a.src_sys_unq_key_txt,
    a.crt_run_id,
    a.lst_updt_run_id,
    a.dw_insrt_ts
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.vital_rslt_dtl AS a
;
