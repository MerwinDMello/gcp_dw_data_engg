-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/remittance_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.remittance_service
   OPTIONS(description='This table has all the services provided for the patient associated to a payment and claim.')
  AS SELECT
      a.service_guid,
      a.claim_guid,
      a.audit_date,
      a.coid,
      a.company_code,
      a.delete_ind,
      a.delete_date,
      ROUND(a.charge_amt, 3, 'ROUND_HALF_EVEN') AS charge_amt,
      ROUND(a.payment_amt, 3, 'ROUND_HALF_EVEN') AS payment_amt,
      ROUND(a.coinsurance_amt, 3, 'ROUND_HALF_EVEN') AS coinsurance_amt,
      ROUND(a.deductible_amt, 3, 'ROUND_HALF_EVEN') AS deductible_amt,
      a.adjudicated_hcpcs_code,
      a.submitted_hcpcs_code,
      a.submitted_hcpcs_code_desc,
      a.payor_sent_revenue_code,
      a.adjudicated_hipps_code,
      a.submitted_hipps_code,
      a.apc_code,
      ROUND(a.apc_amt, 3, 'ROUND_HALF_EVEN') AS apc_amt,
      ROUND(a.adjudicated_service_qty, 2, 'ROUND_HALF_EVEN') AS adjudicated_service_qty,
      ROUND(a.submitted_service_qty, 2, 'ROUND_HALF_EVEN') AS submitted_service_qty,
      a.service_category_code,
      a.date_time_qualifier_code_1,
      a.service_date_1,
      a.date_time_qualifier_code_2,
      a.service_date_2,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.remittance_service AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
