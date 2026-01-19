-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_other_claim_related_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.ref_remittance_other_claim_related_info
   OPTIONS(description='Reference table to maintain the Other Claim Related Reference details of the Claims recevied.')
  AS SELECT
      ROUND(ref_remittance_other_claim_related_info.ref_sid, 0, 'ROUND_HALF_EVEN') AS ref_sid,
      ref_remittance_other_claim_related_info.reference_id_qualifier_code,
      ref_remittance_other_claim_related_info.reference_id,
      ref_remittance_other_claim_related_info.source_system_code,
      ref_remittance_other_claim_related_info.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.ref_remittance_other_claim_related_info
  ;
