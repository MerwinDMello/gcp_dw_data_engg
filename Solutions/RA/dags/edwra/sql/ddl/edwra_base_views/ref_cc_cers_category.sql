-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_cers_category.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_cers_category
   OPTIONS(description='Reference table to calculation engine rate schedule categories.')
  AS SELECT
      ref_cc_cers_category.company_code,
      ref_cc_cers_category.coid,
      ref_cc_cers_category.cers_category_id,
      ref_cc_cers_category.cers_category_name,
      ref_cc_cers_category.cers_category_deleted_ind,
      ref_cc_cers_category.cers_category_create_user_id,
      ref_cc_cers_category.cers_category_create_date,
      ref_cc_cers_category.cers_category_update_user_id,
      ref_cc_cers_category.cers_category_update_date,
      ref_cc_cers_category.source_system_code,
      ref_cc_cers_category.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_category
  ;
