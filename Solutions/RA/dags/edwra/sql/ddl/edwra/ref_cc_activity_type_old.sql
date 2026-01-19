-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_activity_type_old.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_activity_type_old
(
  company_code STRING NOT NULL,
  coid STRING NOT NULL,
  activity_type_id NUMERIC(29) NOT NULL,
  activity_type_desc STRING,
  activity_type_name STRING,
  default_days_num INT64,
  pay_days_num INT64,
  auto_complete_ind STRING,
  incl_rev_recov_rpt_ind STRING,
  incl_notes_ind STRING,
  create_appeal_collect_ind STRING,
  follow_up_activity_type_id NUMERIC(29),
  dw_last_update_date_time DATETIME,
  source_system_code STRING
)
CLUSTER BY company_code, coid, activity_type_id;
