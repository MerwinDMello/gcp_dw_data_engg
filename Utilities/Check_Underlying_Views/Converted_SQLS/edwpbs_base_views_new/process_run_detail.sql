-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/process_run_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.process_run_detail AS SELECT
    process_run_detail.process_name,
    process_run_detail.reporting_date,
    process_run_detail.table_name,
    process_run_detail.start_date_time,
    process_run_detail.end_date_time,
    process_run_detail.source_system_code,
    process_run_detail.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.process_run_detail
;
