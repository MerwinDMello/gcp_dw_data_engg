-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_same_store.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.lu_same_store
   OPTIONS(description='Lookup table that is loaded monthly to determine if a facility is in Same Store before and after GL Close')
  AS SELECT
      a.month_id,
      a.company_code,
      a.coid,
      a.gl_close_ind,
      a.same_store_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.lu_same_store AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
