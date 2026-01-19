-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/claim_reprocessing_tool_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.claim_reprocessing_tool_detail
(
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Data warehouse account identifier.'),
  crt_log_id INT64 NOT NULL OPTIONS(description='Primary key used within the claims reprocessing tool. This number is generated when a new record is added within the application, much like an identity column.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  company_code STRING NOT NULL OPTIONS(description='Indicates the originating company code used by the Payroll system for reporting Payroll Taxes.  Valid values are:  \'H\' = HCA, \'M\'= Quorum Managed, \'N\' = Non-Affiliated, \'A\' = Lifepoint, and \'P\' = Triad. '),
  unit_num STRING NOT NULL OPTIONS(description='Like company idenifier, used to identify a facility.'),
  pat_acct_num NUMERIC(29) OPTIONS(description='The unique patient account number for a given facility.  Identifies a patient.'),
  request_type_desc STRING OPTIONS(description='The description displayed within the claims reprocessing tool for a given request type.'),
  request_date_time DATETIME OPTIONS(description='Date with timestamp when the request was created.'),
  financial_class_code STRING OPTIONS(description='A specific code that identifies a patient as Medicare, Medicaid or Managed Care.'),
  last_activity_date_time DATETIME OPTIONS(description='The date with timestamp of latest recorded activity for a given claim.'),
  status_desc STRING OPTIONS(description='Description of the current status of a claim.'),
  discrepancy_date_time DATETIME OPTIONS(description='Date with timestamp for a given claim discrepancy.'),
  discrepancy_source_desc STRING OPTIONS(description='A description for a given source of a discrepancy.'),
  reimbursement_impact_desc STRING OPTIONS(description='The description of the reimbursement impact. Values are No Original Pament Received,  No Reimbursement Impact, Overpayment, Underpayment, Unknown or NULL.'),
  reprocess_reason_text STRING OPTIONS(description='The underlying reason to reprocess a claim.'),
  queue_name STRING OPTIONS(description='A given name for the current queue with a claim.'),
  claim_category_type STRING OPTIONS(description='A one character prefix used to identify accounts as D, H, or R for respective DSH, IME, or RAC associated claims.'),
  extract_date_time DATETIME OPTIONS(description='The date with timestamp of the initial extract into the claims reprocessing tool application.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Last update date and time for the insert into the table.'),
  source_system_code STRING NOT NULL OPTIONS(description='A character that identifies the source system.')
)
CLUSTER BY patient_dw_id, crt_log_id
OPTIONS(
  description='This table contains all detail related information derived from the claims reprocessing tool.  Data from this table is used to drive discrepancies in expected versus actual payment for a specific account.'
);
