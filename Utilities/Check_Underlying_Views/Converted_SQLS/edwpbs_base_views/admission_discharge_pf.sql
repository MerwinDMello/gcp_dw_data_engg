-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/admission_discharge_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.admission_discharge_pf AS SELECT
    admission_discharge.coid,
    admission_discharge.company_code,
    ROUND(admission_discharge.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(admission_discharge.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    admission_discharge.discharge_medical_code,
    admission_discharge.discharge_status_code,
    admission_discharge.discharge_date,
    admission_discharge.discharge_hour,
    admission_discharge.discharge_posted_date,
    admission_discharge.source_system_code,
    admission_discharge.final_bill_date,
    admission_discharge.final_bill_hold_ind,
    admission_discharge.hold_bill_reason_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.admission_discharge
;
