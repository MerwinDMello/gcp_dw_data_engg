-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_path_issue.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_path_issue
   OPTIONS(description='This table contains Issue information from Payer Analysis Trend Hub (PATH) application.')
  AS SELECT
      ref_path_issue.issue_information_id,
      ref_path_issue.issue_id,
      ref_path_issue.issue_summary_desc,
      ref_path_issue.issue_detail_desc,
      ref_path_issue.operational_guidance_desc,
      ref_path_issue.create_user_name,
      ref_path_issue.create_date,
      ref_path_issue.last_update_user_name,
      ref_path_issue.last_update_date,
      ref_path_issue.source_system_code,
      ref_path_issue.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_path_issue
  ;
