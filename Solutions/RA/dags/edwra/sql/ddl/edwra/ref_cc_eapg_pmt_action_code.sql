-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_eapg_pmt_action_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_pmt_action_code
(
  eapg_payment_action_code STRING NOT NULL OPTIONS(description='The concuity enhanced all payer grouping payment action code that relates to a specific payment action code from all payer grouping output and all payer grouping calculation output tables.'),
  eapg_payment_action_code_desc STRING NOT NULL OPTIONS(description='The concuity enhanced all payer grouping payment action code description that relates to a specific payment action code from all payer grouping output and all payer grouping calculation output tables.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY eapg_payment_action_code
OPTIONS(
  description='Reference table for descriptions related to enhanced all payer grouping payment action codes.  This table will contain static values.'
);
