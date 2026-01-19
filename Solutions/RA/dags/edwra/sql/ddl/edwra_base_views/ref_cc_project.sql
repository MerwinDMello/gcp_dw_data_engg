-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_project.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_project
   OPTIONS(description='Reference table for project related activities.')
  AS SELECT
      ref_cc_project.company_code,
      ref_cc_project.coid,
      ref_cc_project.project_id,
      ref_cc_project.project_name,
      ref_cc_project.project_desc,
      ref_cc_project.work_queue_exclusion_ind,
      ref_cc_project.source_system_code,
      ref_cc_project.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_project
  ;
