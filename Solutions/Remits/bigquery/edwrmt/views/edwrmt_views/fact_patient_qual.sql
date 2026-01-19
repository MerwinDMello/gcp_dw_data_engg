CREATE OR REPLACE VIEW {{ params.param_rmt_views_dataset_name }}.fact_patient_qual
AS SELECT
    a.qual_seq_num,
    a.patient_remit_sid,
    a.svc_line_seq_num,
    a.qualifier_code,
    a.pat_qual_id,
    a.pat_qual_ind,
    a.pat_qual_name,
    a.pat_qual_amt,
    a.pat_qual_code,
    a.pat_qual_cnt,
    a.dw_last_update_date_time,
    a.source_system_code,
    a.customer_cd
  FROM
    {{ params.param_rmt_base_views_dataset_name }}.fact_patient_qual AS a
  WHERE upper(rtrim(a.customer_cd, ' ')) IN(
    'HCA', 'HCAD'
  )
;
