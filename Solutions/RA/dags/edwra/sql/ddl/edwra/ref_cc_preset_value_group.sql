-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_preset_value_group.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_preset_value_group
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  preset_value_id NUMERIC(29) NOT NULL,
  preset_value_group_type STRING NOT NULL,
  preset_value_display_text STRING NOT NULL,
  dw_last_update_date_time DATETIME,
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.')
)
CLUSTER BY company_code, coid, preset_value_id
OPTIONS(
  description='Reference Ids and text values from Clear Contracts with their related groupings'
);
