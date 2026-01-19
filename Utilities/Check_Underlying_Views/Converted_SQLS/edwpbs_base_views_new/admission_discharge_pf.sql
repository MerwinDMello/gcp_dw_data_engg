-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/admission_discharge_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.admission_discharge_pf AS SELECT
    admission_discharge.coid,
    admission_discharge.company_code,
    admission_discharge.patient_dw_id,
    admission_discharge.pat_acct_num,
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
