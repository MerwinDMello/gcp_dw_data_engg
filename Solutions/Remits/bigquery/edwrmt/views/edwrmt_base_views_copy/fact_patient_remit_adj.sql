CREATE OR REPLACE VIEW {{ params.param_rmt_mirrored_base_views_dataset_name }}.fact_patient_remit_adj
AS SELECT
    fact_patient_remit_adj.patient_remit_sid,
    fact_patient_remit_adj.adj_seq_num,
    fact_patient_remit_adj.claim_adj_group_cd,
    fact_patient_remit_adj.claim_adj_reason_cd,
    fact_patient_remit_adj.adjustment_amt,
    fact_patient_remit_adj.dw_last_update_date_time,
    fact_patient_remit_adj.source_system_code,
    fact_patient_remit_adj.adjustment_quantity,
    fact_patient_remit_adj.customer_cd
  FROM
    {{ params.param_mirroring_project_id }}.{{ params.param_rmt_mirrored_core_dataset_name }}.fact_patient_remit_adj
;
