-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/collection_payer_bal_threshold.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.collection_payer_bal_threshold AS SELECT
    a.coid,
    a.company_code,
    a.iplan_id,
    a.artiva_instance_code,
    a.unit_num,
    a.top_threshold_amt,
    a.high_threshold_amt,
    a.low_threshold_amt,
    a.ultra_low_threshold_amt,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_payer_bal_threshold AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
