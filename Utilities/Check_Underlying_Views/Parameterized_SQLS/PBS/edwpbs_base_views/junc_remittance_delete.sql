-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_remittance_delete.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.junc_remittance_delete
   OPTIONS(description='Crosswalk table for Payment Records to be deleted.')
  AS SELECT
      junc_remittance_delete.check_num_an,
      junc_remittance_delete.check_date,
      ROUND(junc_remittance_delete.check_amt, 3, 'ROUND_HALF_EVEN') AS check_amt,
      junc_remittance_delete.interchange_sender_id,
      junc_remittance_delete.provider_adjustment_id,
      junc_remittance_delete.payment_guid,
      junc_remittance_delete.claim_guid,
      junc_remittance_delete.service_guid,
      junc_remittance_delete.delete_date,
      junc_remittance_delete.coid,
      junc_remittance_delete.unit_num,
      junc_remittance_delete.company_code,
      junc_remittance_delete.source_system_code,
      junc_remittance_delete.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.junc_remittance_delete
  ;
