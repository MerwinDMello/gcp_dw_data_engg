-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/remittance_service_carc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.remittance_service_carc AS SELECT
    a.service_guid,
    a.adj_group_code,
    a.carc_code,
    a.audit_date,
    a.delete_ind,
    a.delete_date,
    a.coid,
    a.company_code,
    a.adj_amt,
    a.adj_qty,
    a.adj_category,
    a.cc_adj_group_code,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_service_carc AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
