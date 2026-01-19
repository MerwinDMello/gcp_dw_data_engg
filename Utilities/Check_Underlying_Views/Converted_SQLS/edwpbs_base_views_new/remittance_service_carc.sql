-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service_carc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_service_carc AS SELECT
    remittance_service_carc.service_guid,
    remittance_service_carc.adj_group_code,
    remittance_service_carc.carc_code,
    remittance_service_carc.audit_date,
    remittance_service_carc.delete_ind,
    remittance_service_carc.delete_date,
    remittance_service_carc.coid,
    remittance_service_carc.company_code,
    remittance_service_carc.adj_amt,
    remittance_service_carc.adj_qty,
    remittance_service_carc.adj_category,
    remittance_service_carc.cc_adj_group_code,
    remittance_service_carc.dw_last_update_date_time,
    remittance_service_carc.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.remittance_service_carc
;
