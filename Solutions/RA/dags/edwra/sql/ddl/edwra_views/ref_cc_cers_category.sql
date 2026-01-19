-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_cc_cers_category.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_cc_cers_category
   OPTIONS(description='Reference table to calculation engine rate schedule categories.')
  AS SELECT
      a.company_code,
      a.coid,
      a.cers_category_id,
      a.cers_category_name,
      a.cers_category_deleted_ind,
      a.cers_category_create_user_id,
      a.cers_category_create_date,
      a.cers_category_update_user_id,
      a.cers_category_update_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_cers_category AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
