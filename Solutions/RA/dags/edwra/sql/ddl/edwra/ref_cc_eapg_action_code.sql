-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_eapg_action_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_action_code
(
  eapg_action_code STRING NOT NULL OPTIONS(description='Enhanced all payer grouping action code which ties to an APG calculation output or APG grouper output table row.'),
  eapg_action_code_description STRING NOT NULL OPTIONS(description='Enhanced all payer grouping action description for the associated related action code.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY eapg_action_code
OPTIONS(
  description='Reference table for descriptions related to enhanced all payer grouping action codes.  This table will contain static values.'
);
