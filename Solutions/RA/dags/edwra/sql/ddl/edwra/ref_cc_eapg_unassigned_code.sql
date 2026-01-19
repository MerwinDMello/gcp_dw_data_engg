-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_eapg_unassigned_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_unassigned_code
(
  eapg_unassigned_code STRING NOT NULL OPTIONS(description='Identifies the unassigned code for enhanced all payer grouping.'),
  eapg_unassigned_desc STRING NOT NULL OPTIONS(description='Description for the unassigned code for enhanced all payer grouping.'),
  source_system_code STRING NOT NULL OPTIONS(description='Identifies the source system.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Data Warehouse last update date time.')
)
CLUSTER BY eapg_unassigned_code
OPTIONS(
  description='Reference table for descriptions related to enhanced all payer grouping unassigned codes.  This table will contain static values.'
);
