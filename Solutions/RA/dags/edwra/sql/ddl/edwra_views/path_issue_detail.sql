-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/path_issue_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.path_issue_detail AS SELECT
    path_issue_detail.account_payor_id,
    path_issue_detail.issue_1_id,
    path_issue_detail.issue_1_create_date,
    path_issue_detail.issue_1_summary,
    path_issue_detail.issue_1_detail,
    path_issue_detail.issue_1_operational_guidance,
    path_issue_detail.issue_2_id,
    path_issue_detail.issue_2_create_date,
    path_issue_detail.issue_2_summary,
    path_issue_detail.issue_2_detail,
    path_issue_detail.issue_2_operational_guidance,
    path_issue_detail.dw_last_update_date_time
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.path_issue_detail
;
