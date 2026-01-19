-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/process_run_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.process_run_detail
   OPTIONS(description='This table has the information of start and completion of any EDWPBS ETL  Process. Used for Business facing for effective User Analysis')
  AS SELECT
      process_run_detail.process_name,
      process_run_detail.reporting_date,
      process_run_detail.table_name,
      process_run_detail.start_date_time,
      process_run_detail.end_date_time,
      process_run_detail.source_system_code,
      process_run_detail.dw_last_update_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.process_run_detail
  ;
