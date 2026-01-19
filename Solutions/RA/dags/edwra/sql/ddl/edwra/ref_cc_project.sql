-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_cc_project.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_project
(
  company_code STRING NOT NULL OPTIONS(description='One character that represents the company code for a given facility.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  project_id NUMERIC(29) NOT NULL OPTIONS(description='Foregin primary key to identifiy a specific project record.'),
  project_name STRING OPTIONS(description='Project name presented from the Concuity UI.'),
  project_desc STRING OPTIONS(description='Project description as stated on the Concuity UI.  Describes what the project is.'),
  work_queue_exclusion_ind STRING OPTIONS(description='Indicates if a work queue is excluded from a project within Concuity.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY company_code, coid, project_id
OPTIONS(
  description='Reference table for project related activities.'
);
