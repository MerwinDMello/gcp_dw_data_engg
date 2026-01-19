-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_cers_profile.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_cers_profile
   OPTIONS(description='Reference table to calculation enginge rate schedule profiles.')
  AS SELECT
      ref_cc_cers_profile.company_code,
      ref_cc_cers_profile.coid,
      ref_cc_cers_profile.cers_profile_id,
      ref_cc_cers_profile.cers_profile_name,
      ref_cc_cers_profile.cers_profile_create_date,
      ref_cc_cers_profile.cers_profile_update_date,
      ref_cc_cers_profile.cers_profile_update_user_nm,
      ref_cc_cers_profile.cers_category_id,
      ref_cc_cers_profile.cers_model_ind,
      ref_cc_cers_profile.source_system_code,
      ref_cc_cers_profile.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_profile
  ;
