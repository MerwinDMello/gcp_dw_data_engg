-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_path_issue.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_path_issue
   OPTIONS(description='This table contains Issue information from Payer Analysis Trend Hub (PATH) application.')
  AS SELECT
      a.issue_information_id,
      a.issue_id,
      a.issue_summary_desc,
      a.issue_detail_desc,
      a.operational_guidance_desc,
      a.create_user_name,
      a.create_date,
      a.last_update_user_name,
      a.last_update_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.ref_path_issue AS a
  ;
