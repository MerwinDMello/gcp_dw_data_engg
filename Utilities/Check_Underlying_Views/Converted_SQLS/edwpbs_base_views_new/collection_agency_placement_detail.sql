-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_agency_placement_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_agency_placement_detail AS SELECT
    collection_agency_placement_detail.placement_sid,
    collection_agency_placement_detail.patient_dw_id,
    collection_agency_placement_detail.company_code,
    collection_agency_placement_detail.coid,
    collection_agency_placement_detail.pat_acct_num,
    collection_agency_placement_detail.unit_num,
    collection_agency_placement_detail.vendor_id,
    collection_agency_placement_detail.artiva_instance_code,
    collection_agency_placement_detail.placement_date,
    collection_agency_placement_detail.placement_time,
    collection_agency_placement_detail.recall_reason_code,
    collection_agency_placement_detail.recall_date,
    collection_agency_placement_detail.recall_time,
    collection_agency_placement_detail.placement_amt,
    collection_agency_placement_detail.recall_amt,
    collection_agency_placement_detail.source_system_code,
    collection_agency_placement_detail.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.collection_agency_placement_detail
;
