CREATE OR REPLACE VIEW {{ params.param_rmt_views_dataset_name }}.fact_patient_remit_adj
AS SELECT
    a.patient_remit_sid,
    a.adj_seq_num,
    a.claim_adj_group_cd,
    a.claim_adj_reason_cd,
    a.adjustment_amt,
    a.dw_last_update_date_time,
    a.source_system_code,
    a.adjustment_quantity,
    a.customer_cd
  FROM
    {{ params.param_rmt_base_views_dataset_name }}.fact_patient_remit_adj AS a
  WHERE upper(rtrim(a.customer_cd, ' ')) IN(
    'HCA', 'HCAD'
  )
;
