-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_surgical_case_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_surgical_case_cdm AS SELECT
    srg_case_dtl.srg_case_sk AS surgical_case_sk,
    srg_case_dtl.case_sts_cd_sk AS case_status_sk,
    srg_case_dtl.hsptl_srvc_cd_sk AS hospital_service_sk,
    en.encounter_sk AS encounter_sk,
    en.patient_sk AS patient_sk,
    srg_case_dtl.patient_dw_id AS patient_dw_id,
    srg_case_dtl.company_code AS company_code,
    srg_case_dtl.coid AS coid,
    srg_case_dtl.vld_fr_ts AS valid_from_date_time,
    srg_case_dtl.srg_type_ref_cd AS surgical_type_code,
    srg_case_dtl.srg_case_id_txt AS surgical_case_id_txt,
    srg_case_dtl.srg_case_apntmt_id_txt AS surgical_case_appt_id_txt,
    srg_case_dtl.ansth_type_cd AS anesthesia_type_code,
    srg_case_dtl.outcm_cd AS surgical_outcome_code,
    srg_case_dtl.asa_scor_cd AS surgical_asa_score_code,
    srg_case_dtl.outptnt_case_ind AS surgical_outpatient_case_ind,
    srg_case_dtl.case_vrfctn_sts_txt AS surgical_case_status_txt,
    srg_case_dtl.src_sys_orgnl_cd AS source_system_original_code,
    srg_case_dtl.src_sys_ref_cd AS source_system_txt,
    srg_case_dtl.dw_insrt_ts AS dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.srg_case_dtl
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcdm_base_views.fact_encounter AS en ON srg_case_dtl.patient_dw_id = en.patient_dw_id
  WHERE srg_case_dtl.vld_to_ts = '9999-12-31 00:00:00'
;
