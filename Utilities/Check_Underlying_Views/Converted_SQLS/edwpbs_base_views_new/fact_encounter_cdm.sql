-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_encounter_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_encounter_cdm AS SELECT
    a.encounter_sk,
    a.patient_dw_id,
    a.pat_acct_num,
    a.coid,
    a.company_code,
    a.patient_sk,
    a.facility_sk,
    a.attending_practitioner_sk,
    a.consulting_practitioner_sk,
    a.admitting_practitioner_sk,
    a.referring_practitioner_sk,
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
