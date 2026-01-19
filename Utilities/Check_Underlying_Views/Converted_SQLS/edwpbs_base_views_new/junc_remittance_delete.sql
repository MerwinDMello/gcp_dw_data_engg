-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_remittance_delete.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_remittance_delete AS SELECT
    junc_remittance_delete.check_num_an,
    junc_remittance_delete.check_date,
    junc_remittance_delete.check_amt,
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
    `hca-hin-dev-cur-parallon`.edwpbs.junc_remittance_delete
;
