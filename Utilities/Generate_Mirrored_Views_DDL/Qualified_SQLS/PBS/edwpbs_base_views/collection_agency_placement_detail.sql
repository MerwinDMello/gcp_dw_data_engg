-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_agency_placement_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.collection_agency_placement_detail
   OPTIONS(description='This table holds the agency placement details of all collections from Artiva  for any accounts')
  AS SELECT
      ROUND(collection_agency_placement_detail.placement_sid, 0, 'ROUND_HALF_EVEN') AS placement_sid,
      ROUND(collection_agency_placement_detail.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      collection_agency_placement_detail.company_code,
      collection_agency_placement_detail.coid,
      ROUND(collection_agency_placement_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      collection_agency_placement_detail.unit_num,
      collection_agency_placement_detail.vendor_id,
      collection_agency_placement_detail.artiva_instance_code,
      collection_agency_placement_detail.placement_date,
      collection_agency_placement_detail.placement_time,
      collection_agency_placement_detail.recall_reason_code,
      collection_agency_placement_detail.recall_date,
      collection_agency_placement_detail.recall_time,
      ROUND(collection_agency_placement_detail.placement_amt, 3, 'ROUND_HALF_EVEN') AS placement_amt,
      ROUND(collection_agency_placement_detail.recall_amt, 3, 'ROUND_HALF_EVEN') AS recall_amt,
      collection_agency_placement_detail.source_system_code,
      collection_agency_placement_detail.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.collection_agency_placement_detail
  ;
