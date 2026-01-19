-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_same_store.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store
   OPTIONS(description='Lookup table that is loaded monthly to determine if a facility is in Same Store before and after GL Close')
  AS SELECT
      lu_same_store.month_id,
      lu_same_store.company_code,
      lu_same_store.coid,
      lu_same_store.gl_close_ind,
      lu_same_store.same_store_ind,
      lu_same_store.source_system_code,
      lu_same_store.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.lu_same_store
  ;
