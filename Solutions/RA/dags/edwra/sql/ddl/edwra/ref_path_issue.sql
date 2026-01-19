-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/ref_path_issue.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.ref_path_issue
(
  issue_information_id BIGNUMERIC(38) NOT NULL OPTIONS(description='ID-generated primary-key column for the Issue_IssueInformation table.'),
  issue_id BIGNUMERIC(38) NOT NULL OPTIONS(description='Issue Identifier is the unique identifier assigned to an issue.'),
  issue_summary_desc STRING OPTIONS(description='Issue Summary is the descriptive label (i.e. name) given to an issue.'),
  issue_detail_desc STRING OPTIONS(description='A detailed description of the issue.'),
  operational_guidance_desc STRING OPTIONS(description='Operational guidance for the issue.'),
  create_user_name STRING OPTIONS(description='The 3-4 ID of the user who created the record.'),
  create_date DATETIME OPTIONS(description='The date when the record was created.'),
  last_update_user_name STRING OPTIONS(description='The 3-4 ID of the user who last modified the record.'),
  last_update_date DATETIME OPTIONS(description='The latest date when the record was modified.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY issue_information_id, issue_id
OPTIONS(
  description='This table contains Issue information from Payer Analysis Trend Hub (PATH) application.'
);
