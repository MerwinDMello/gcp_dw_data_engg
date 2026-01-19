-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_remittance_rendering_provider.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_remittance_rendering_provider AS SELECT
    a.remittance_rendering_provider_sid,
    a.serv_provider_enty_type_qualifier_code,
    a.rendering_provider_last_org_name,
    a.rendering_provider_first_name,
    a.rendering_provider_middle_name,
    a.rendering_provider_name_suffix,
    a.serv_provider_id_qualifier_code,
    a.rendering_provider_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_remittance_rendering_provider AS a
;
