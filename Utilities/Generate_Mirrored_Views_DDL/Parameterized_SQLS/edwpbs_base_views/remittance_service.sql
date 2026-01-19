-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.remittance_service
   OPTIONS(description='This table has all the services provided for the patient associated to a payment and claim.')
  AS SELECT
      remittance_service.service_guid,
      remittance_service.claim_guid,
      remittance_service.audit_date,
      remittance_service.coid,
      remittance_service.company_code,
      remittance_service.delete_ind,
      remittance_service.delete_date,
      ROUND(remittance_service.charge_amt, 3, 'ROUND_HALF_EVEN') AS charge_amt,
      ROUND(remittance_service.payment_amt, 3, 'ROUND_HALF_EVEN') AS payment_amt,
      ROUND(remittance_service.coinsurance_amt, 3, 'ROUND_HALF_EVEN') AS coinsurance_amt,
      ROUND(remittance_service.deductible_amt, 3, 'ROUND_HALF_EVEN') AS deductible_amt,
      remittance_service.adjudicated_hcpcs_code,
      remittance_service.submitted_hcpcs_code,
      remittance_service.submitted_hcpcs_code_desc,
      remittance_service.payor_sent_revenue_code,
      remittance_service.adjudicated_hipps_code,
      remittance_service.submitted_hipps_code,
      remittance_service.apc_code,
      ROUND(remittance_service.apc_amt, 3, 'ROUND_HALF_EVEN') AS apc_amt,
      ROUND(remittance_service.adjudicated_service_qty, 2, 'ROUND_HALF_EVEN') AS adjudicated_service_qty,
      ROUND(remittance_service.submitted_service_qty, 2, 'ROUND_HALF_EVEN') AS submitted_service_qty,
      remittance_service.service_category_code,
      remittance_service.date_time_qualifier_code_1,
      remittance_service.service_date_1,
      remittance_service.date_time_qualifier_code_2,
      remittance_service.service_date_2,
      remittance_service.dw_last_update_date_time,
      remittance_service.source_system_code
    FROM
      {{ params.param_pbs_core_dataset_name }}.remittance_service
  ;
