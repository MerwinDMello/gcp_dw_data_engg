-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/admission_patient_type_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.admission_patient_type_pf AS SELECT
    admission_patient_type.coid,
    admission_patient_type.company_code,
    admission_patient_type.patient_dw_id,
    admission_patient_type.eff_from_date,
    admission_patient_type.pat_acct_num,
    admission_patient_type.admission_patient_type_code,
    admission_patient_type.admission_prev_pat_type_code,
    admission_patient_type.eff_to_date,
    admission_patient_type.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.admission_patient_type
;
