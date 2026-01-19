-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_collection_claim_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.ref_collection_claim_status
   OPTIONS(description='Table contains descriptions for all the collection Claim Status Codes present in the Artiva system')
  AS SELECT
      a.claim_status_code,
      a.claim_status_code_desc,
      a.claim_status_relt_scat_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.ref_collection_claim_status AS a
  ;
