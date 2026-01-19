CREATE OR REPLACE VIEW {{ params.param_rmt_views_dataset_name }}.fact_patient_remit
AS SELECT
    a.patient_remit_sid,
    a.remit_id,
    a.remit_provider_id,
    a.remit_payer_id,
    a.patient_acct_num,
    a.unit_num,
    a.iplan,
    a.pas_coid,
    a.remit_file_path,
    a.remit_file_name,
    a.claim_file_name,
    a.remit_bill_dt,
    a.remit_claim_status_cd,
    a.remit_payment_amt,
    a.remit_claim_id,
    a.tot_claim_charge_amt,
    a.patient_resp_amt,
    a.claim_file_inc_cd,
    a.payer_claim_ctrll_num,
    a.facility_cd,
    a.claim_freq_type_cd,
    a.drg,
    a.drg_weight,
    a.discharge_fraction,
    a.dw_last_update_date_time,
    a.source_system_code,
    a.customer_cd,
    b.remit_effective_dt,
    b.remit_entered_dt,
    c.npi,
    c.provider_name,
    d.payer_name,
    d.payer_id_num
  FROM
    {{ params.param_rmt_base_views_dataset_name }}.fact_patient_remit AS a
    INNER JOIN (
      SELECT
          fact_master_remit.*
        FROM
          {{ params.param_rmt_base_views_dataset_name }}.fact_master_remit
        WHERE upper(rtrim(fact_master_remit.customer_cd, ' ')) IN(
          'HCA', 'HCAD'
        )
    ) AS b ON upper(trim(a.remit_id, ' ')) = upper(trim(b.remit_id, ' '))
     AND upper(rtrim(a.customer_cd, ' ')) = upper(rtrim(b.customer_cd, ' '))
     AND SAFE_CAST(coalesce(a.patient_acct_num, '0') AS NUMERIC) IS NOT NULL
     AND length(trim(a.patient_acct_num, ' ')) < 12
     AND rtrim(upper(to_hex(CAST(upper(a.patient_acct_num) as BYTES))), ' ') = rtrim(upper(to_hex(CAST(lower(a.patient_acct_num) as BYTES))), ' ')
     AND a.patient_acct_num NOT LIKE '%-%'
     AND a.patient_acct_num IS NOT NULL
    LEFT OUTER JOIN (
      SELECT
          lu_remit_provider.*
        FROM
          {{ params.param_rmt_base_views_dataset_name }}.lu_remit_provider
        WHERE upper(rtrim(lu_remit_provider.customer_cd, ' ')) IN(
          'HCA', 'HCAD'
        )
    ) AS c ON upper(rtrim(a.remit_provider_id, ' ')) = upper(rtrim(c.remit_provider_id, ' '))
    LEFT OUTER JOIN (
      SELECT
          lu_remit_payer.*
        FROM
          {{ params.param_rmt_base_views_dataset_name }}.lu_remit_payer
        WHERE upper(rtrim(lu_remit_payer.customer_cd, ' ')) IN(
          'HCA', 'HCAD'
        )
    ) AS d ON upper(rtrim(a.remit_payer_id, ' ')) = upper(rtrim(d.remit_payer_id, ' '))
;
