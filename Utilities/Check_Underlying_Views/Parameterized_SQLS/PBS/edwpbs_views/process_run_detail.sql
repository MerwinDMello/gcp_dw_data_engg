-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/process_run_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.process_run_detail AS SELECT
    a.process_name,
    a.reporting_date,
    a.table_name,
    a.start_date_time,
    a.end_date_time,
    a.source_system_code,
    a.dw_last_update_time
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.process_run_detail AS a
;
