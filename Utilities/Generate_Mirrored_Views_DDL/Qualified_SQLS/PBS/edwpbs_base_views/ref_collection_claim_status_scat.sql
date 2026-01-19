-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_collection_claim_status_scat.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_collection_claim_status_scat
   OPTIONS(description='Table contains related Sub category  descriptions for all the collection Claim Status Codes present in the Artiva system')
  AS SELECT
      ref_collection_claim_status_scat.claim_status_scat_id,
      ref_collection_claim_status_scat.claim_status_scat_desc,
      ref_collection_claim_status_scat.source_system_code,
      ref_collection_claim_status_scat.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_collection_claim_status_scat
  ;
