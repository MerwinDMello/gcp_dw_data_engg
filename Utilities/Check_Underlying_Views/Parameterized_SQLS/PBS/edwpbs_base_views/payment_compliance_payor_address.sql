-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_compliance_payor_address.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.payment_compliance_payor_address
   OPTIONS(description='Payor Address master table for appeals, underpayment, overpayments, and other non-claim related addresses from Payment Compliance')
  AS SELECT
      payment_compliance_payor_address.address_sid,
      payment_compliance_payor_address.major_payor_group_id,
      payment_compliance_payor_address.salutation_name,
      payment_compliance_payor_address.payor_name,
      payment_compliance_payor_address.address_1_text,
      payment_compliance_payor_address.address_2_text,
      payment_compliance_payor_address.city_name,
      payment_compliance_payor_address.state_code,
      payment_compliance_payor_address.zip_code,
      ROUND(payment_compliance_payor_address.fax_num, 0, 'ROUND_HALF_EVEN') AS fax_num,
      payment_compliance_payor_address.email_text,
      payment_compliance_payor_address.eff_start_date,
      payment_compliance_payor_address.eff_end_date,
      payment_compliance_payor_address.source_system_code,
      payment_compliance_payor_address.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.payment_compliance_payor_address
  ;
