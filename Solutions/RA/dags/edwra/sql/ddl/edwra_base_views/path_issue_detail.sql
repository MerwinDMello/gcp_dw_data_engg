-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/path_issue_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.path_issue_detail AS SELECT
    b.account_payor_id,
    max(CASE
      WHEN b.issue_rank_num = 1 THEN b.issue_id
      ELSE CAST(NULL as BIGNUMERIC)
    END) AS issue_1_id,
    max(CASE
      WHEN b.issue_rank_num = 1 THEN b.create_date
      ELSE CAST(NULL as DATETIME)
    END) AS issue_1_create_date,
    max(CASE
      WHEN b.issue_rank_num = 1 THEN b.issue_summary_desc
      ELSE CAST(NULL as STRING)
    END) AS issue_1_summary,
    max(CASE
      WHEN b.issue_rank_num = 1 THEN b.issue_detail_desc
      ELSE CAST(NULL as STRING)
    END) AS issue_1_detail,
    max(CASE
      WHEN b.issue_rank_num = 1 THEN b.operational_guidance_desc
      ELSE CAST(NULL as STRING)
    END) AS issue_1_operational_guidance,
    max(CASE
      WHEN b.issue_rank_num = 2 THEN b.issue_id
      ELSE CAST(NULL as BIGNUMERIC)
    END) AS issue_2_id,
    max(CASE
      WHEN b.issue_rank_num = 2 THEN b.create_date
      ELSE CAST(NULL as DATETIME)
    END) AS issue_2_create_date,
    max(CASE
      WHEN b.issue_rank_num = 2 THEN b.issue_summary_desc
      ELSE CAST(NULL as STRING)
    END) AS issue_2_summary,
    max(CASE
      WHEN b.issue_rank_num = 2 THEN b.issue_detail_desc
      ELSE CAST(NULL as STRING)
    END) AS issue_2_detail,
    max(CASE
      WHEN b.issue_rank_num = 2 THEN b.operational_guidance_desc
      ELSE CAST(NULL as STRING)
    END) AS issue_2_operational_guidance,
    max(b.dw_last_update_date_time) AS dw_last_update_date_time
  FROM
    (
      SELECT
          ap.account_payor_id,
          api.issue_rank_num,
          api.issue_id,
          api.create_date,
          i.issue_summary_desc,
          i.issue_detail_desc,
          i.operational_guidance_desc,
          api.dw_last_update_date_time
        FROM
          {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor AS ap
          INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor_issue AS api ON ap.account_payor_id = api.acct_payer_id
          INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_path_issue AS i ON api.issue_id = i.issue_id
        WHERE api.issue_rank_num = 1
      UNION DISTINCT
      SELECT
          ap.account_payor_id,
          api.issue_rank_num,
          api.issue_id,
          api.create_date,
          i.issue_summary_desc,
          i.issue_detail_desc,
          i.operational_guidance_desc,
          api.dw_last_update_date_time
        FROM
          {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor AS ap
          INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor_issue AS api ON ap.account_payor_id = api.acct_payer_id
          INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_path_issue AS i ON api.issue_id = i.issue_id
        WHERE api.issue_rank_num = 2
    ) AS b
  GROUP BY 1
;
