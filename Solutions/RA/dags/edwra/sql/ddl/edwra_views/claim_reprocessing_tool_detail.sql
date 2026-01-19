-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/claim_reprocessing_tool_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.claim_reprocessing_tool_detail
   OPTIONS(description='This table contains all detail related information derived from the claims reprocessing tool.  Data from this table is used to drive discrepancies in expected versus actual payment for a specific account.')
  AS SELECT
      a.patient_dw_id,
      a.crt_log_id,
      a.coid,
      a.company_code,
      a.unit_num,
      a.pat_acct_num,
      a.request_type_desc,
      a.request_date_time,
      a.financial_class_code,
      a.last_activity_date_time,
      a.status_desc,
      a.discrepancy_date_time,
      a.discrepancy_source_desc,
      a.reimbursement_impact_desc,
      a.reprocess_reason_text,
      a.queue_name,
      a.claim_category_type,
      a.extract_date_time,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.claim_reprocessing_tool_detail AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
