-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/collection_agency_placement_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.collection_agency_placement_detail AS SELECT
    a.placement_sid,
    a.patient_dw_id,
    a.company_code,
    a.coid,
    a.pat_acct_num,
    a.unit_num,
    a.vendor_id,
    a.artiva_instance_code,
    a.placement_date,
    a.placement_time,
    a.recall_reason_code,
    a.recall_date,
    a.recall_time,
    a.placement_amt,
    a.recall_amt,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_agency_placement_detail AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
