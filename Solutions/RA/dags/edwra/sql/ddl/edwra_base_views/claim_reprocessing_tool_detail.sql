-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/claim_reprocessing_tool_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.claim_reprocessing_tool_detail
   OPTIONS(description='This table contains all detail related information derived from the claims reprocessing tool.  Data from this table is used to drive discrepancies in expected versus actual payment for a specific account.')
  AS SELECT
      claim_reprocessing_tool_detail.patient_dw_id,
      claim_reprocessing_tool_detail.crt_log_id,
      claim_reprocessing_tool_detail.coid,
      claim_reprocessing_tool_detail.company_code,
      claim_reprocessing_tool_detail.unit_num,
      claim_reprocessing_tool_detail.pat_acct_num,
      claim_reprocessing_tool_detail.request_type_desc,
      claim_reprocessing_tool_detail.request_date_time,
      claim_reprocessing_tool_detail.financial_class_code,
      claim_reprocessing_tool_detail.last_activity_date_time,
      claim_reprocessing_tool_detail.status_desc,
      claim_reprocessing_tool_detail.discrepancy_date_time,
      claim_reprocessing_tool_detail.discrepancy_source_desc,
      claim_reprocessing_tool_detail.reimbursement_impact_desc,
      claim_reprocessing_tool_detail.reprocess_reason_text,
      claim_reprocessing_tool_detail.queue_name,
      claim_reprocessing_tool_detail.claim_category_type,
      claim_reprocessing_tool_detail.extract_date_time,
      claim_reprocessing_tool_detail.dw_last_update_date_time,
      claim_reprocessing_tool_detail.source_system_code
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.claim_reprocessing_tool_detail
  ;
