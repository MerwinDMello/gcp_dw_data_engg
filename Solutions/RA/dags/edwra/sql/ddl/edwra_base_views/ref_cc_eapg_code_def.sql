-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_eapg_code_def.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_eapg_code_def
   OPTIONS(description='Reference table to code definitions related to EAPG only.')
  AS SELECT
      ref_cc_eapg_code_def.company_code,
      ref_cc_eapg_code_def.coid,
      ref_cc_eapg_code_def.eapg_code,
      ref_cc_eapg_code_def.eapg_type_code,
      ref_cc_eapg_code_def.eapg_code_version_id,
      ref_cc_eapg_code_def.eapg_code_name,
      ref_cc_eapg_code_def.eapg_code_short_name,
      ref_cc_eapg_code_def.eapg_code_misc_desc,
      ref_cc_eapg_code_def.eapg_code_misc_char_desc,
      ref_cc_eapg_code_def.source_system_code,
      ref_cc_eapg_code_def.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_code_def
  ;
