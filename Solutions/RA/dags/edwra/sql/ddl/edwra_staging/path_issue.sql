-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_staging/path_issue.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_stage_dataset_name }}.path_issue
(
  schema_id NUMERIC(29) NOT NULL,
  issue_issueinformation_id BIGNUMERIC(38) NOT NULL,
  issueid BIGNUMERIC(38) NOT NULL,
  issuesummary STRING NOT NULL,
  issuedetail STRING NOT NULL,
  operationalguidance STRING,
  createdby STRING NOT NULL,
  createddate DATETIME,
  lastmodifiedby STRING NOT NULL,
  lastmodifieddate DATETIME,
  extract_date DATETIME
)
CLUSTER BY schema_id, issue_issueinformation_id;
