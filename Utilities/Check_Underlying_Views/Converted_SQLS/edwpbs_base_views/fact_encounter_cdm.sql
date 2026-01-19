-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_encounter_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_encounter_cdm AS SELECT
    ROUND(a.encounter_sk, 0, 'ROUND_HALF_EVEN') AS encounter_sk,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    a.coid,
    a.company_code,
    ROUND(a.patient_sk, 0, 'ROUND_HALF_EVEN') AS patient_sk,
    ROUND(a.facility_sk, 0, 'ROUND_HALF_EVEN') AS facility_sk,
    ROUND(a.attending_practitioner_sk, 0, 'ROUND_HALF_EVEN') AS attending_practitioner_sk,
    ROUND(a.consulting_practitioner_sk, 0, 'ROUND_HALF_EVEN') AS consulting_practitioner_sk,
    ROUND(a.admitting_practitioner_sk, 0, 'ROUND_HALF_EVEN') AS admitting_practitioner_sk,
    ROUND(a.referring_practitioner_sk, 0, 'ROUND_HALF_EVEN') AS referring_practitioner_sk,
    a.discharge_status_sk,
    a.arrival_mode_sk,
    a.admit_source_sk,
    a.medical_record_num,
    a.patient_market_urn,
    a.visit_type_sk,
    a.admit_type_code,
    a.patient_class_code,
    a.patient_status_code,
    a.priority_code,
    a.reason_for_visit_txt,
    a.emr_patient_id,
    a.encounter_id_txt,
    a.special_program_sk,
    a.vip_ind,
    a.encounter_date_time,
    a.admission_date_time,
    a.discharge_date_time,
    a.actl_los_cnt,
    a.accident_date_time,
    a.dbmotion_effective_date,
    a.expected_num_of_ins_plns_cnt,
    a.signature_date,
    a.readmission_ind,
    a.valid_from_date_time,
    a.eff_from_date,
    a.source_system_txt,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.fact_encounter AS a
;
