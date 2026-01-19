-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_eapg_visit_type_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_eapg_visit_type_code
(
  eapg_visit_type_code STRING NOT NULL OPTIONS(description='The enhanced all payer grouping visit type code foregin key used within all payer grouping calculation output and all payer grouping output tables.'),
  eapg_visit_type_code_desc STRING NOT NULL OPTIONS(description='The description for a given enhanced all payer grouping visit type related code.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY eapg_visit_type_code
OPTIONS(
  description='Reference table for descriptions related to enhanced all payer grouping visit type codes.  This table will contain static values.'
);
