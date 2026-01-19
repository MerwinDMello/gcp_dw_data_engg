-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/admission_patient_type_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.admission_patient_type_pf AS SELECT
    admission_patient_type_pf.coid,
    admission_patient_type_pf.company_code,
    ROUND(admission_patient_type_pf.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    admission_patient_type_pf.eff_from_date,
    ROUND(admission_patient_type_pf.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    admission_patient_type_pf.admission_patient_type_code,
    admission_patient_type_pf.admission_prev_pat_type_code,
    admission_patient_type_pf.eff_to_date,
    admission_patient_type_pf.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.admission_patient_type_pf
;
