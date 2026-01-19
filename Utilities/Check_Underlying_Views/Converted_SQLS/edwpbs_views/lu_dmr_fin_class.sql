-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_dmr_fin_class.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_dmr_fin_class
   OPTIONS(description='Dimension table for maintaining Financial Class and will include more financial class than the regular 1-15 based on the iplans.')
  AS SELECT
      a.dmr_fin_class_code,
      a.dmr_fin_class_desc,
      a.fin_class_group_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_dmr_fin_class AS a
  ;
