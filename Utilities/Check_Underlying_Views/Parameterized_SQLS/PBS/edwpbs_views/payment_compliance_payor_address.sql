-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payment_compliance_payor_address.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.payment_compliance_payor_address
   OPTIONS(description='Payor Address master table for appeals, underpayment, overpayments, and other non-claim related addresses from Payment Compliance')
  AS SELECT
      a.address_sid,
      a.major_payor_group_id,
      a.salutation_name,
      a.payor_name,
      a.address_1_text,
      a.address_2_text,
      a.city_name,
      a.state_code,
      a.zip_code,
      ROUND(a.fax_num, 0, 'ROUND_HALF_EVEN') AS fax_num,
      a.email_text,
      a.eff_start_date,
      a.eff_end_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.payment_compliance_payor_address AS a
  ;
