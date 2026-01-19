CREATE OR REPLACE VIEW {{ params.param_rmt_mirrored_base_views_dataset_name }}.fact_patient_qual
AS SELECT
    fact_patient_qual.qual_seq_num,
    fact_patient_qual.patient_remit_sid,
    fact_patient_qual.svc_line_seq_num,
    fact_patient_qual.qualifier_code,
    fact_patient_qual.pat_qual_id,
    fact_patient_qual.pat_qual_ind,
    fact_patient_qual.pat_qual_name,
    fact_patient_qual.pat_qual_amt,
    fact_patient_qual.pat_qual_code,
    fact_patient_qual.pat_qual_cnt,
    fact_patient_qual.dw_last_update_date_time,
    fact_patient_qual.source_system_code,
    fact_patient_qual.customer_cd
  FROM
    {{ params.param_mirroring_project_id }}.{{ params.param_rmt_mirrored_core_dataset_name }}.fact_patient_qual
;
