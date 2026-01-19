-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/cc_appeal_sequence.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Data warehouse key that identifies a patient.'),
  payor_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Data warehouse key that identifies an insurance payer.'),
  iplan_insurance_order_num INT64 NOT NULL OPTIONS(description='Indicates the precedence of payment liability for insurance plans associated with a patient:  primary, secondary, etc.'),
  appeal_num NUMERIC(29) NOT NULL OPTIONS(description='Indicates the appeal number for a given appeal record associated with an appeal sequence.'),
  appeal_seq_num INT64 NOT NULL OPTIONS(description='A given appeal sequence number associated with an appeal record.'),
  unit_num STRING NOT NULL OPTIONS(description='4-digit corporate assigned number which uniquely identifies each facility.'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description='A patient account number which is unique for a specific facility, identifies a given patient.'),
  iplan_id INT64 OPTIONS(description='An identification code assigned to an insurance plan.  The first three digits identify the payor.  The last two uniquely identify a specific plan belonging to that payor.'),
  appeal_seq_begin_bal_amt NUMERIC(32, 3) OPTIONS(description='Appeal balance at the beginning of the sequence activity.'),
  appeal_seq_current_bal_amt NUMERIC(32, 3) OPTIONS(description='Appeal balance currently associated with a sequence activity.'),
  appeal_seq_end_bal_amt NUMERIC(32, 3) OPTIONS(description='Appeal balance at the end of the sequence activity.'),
  appeal_seq_deadline_date DATE OPTIONS(description='Indicates the deadline to resolve an open appeal.'),
  appeal_seq_close_date_time DATETIME OPTIONS(description='The date when the appeal or appeal sequence record was closed.'),
  appeal_seq_root_cause_id NUMERIC(29) OPTIONS(description='Identifer for the appeal sequence root cause.'),
  appeal_seq_root_cause_dtl_text STRING OPTIONS(description='Appeal sequence root cause detailed description.'),
  appeal_disp_code_id NUMERIC(29) OPTIONS(description='Identifies an appeal sequence disposition code.'),
  appeal_code_id NUMERIC(29) OPTIONS(description='Identifies an appeal sequence code.'),
  appeal_seq_owner_user_id STRING OPTIONS(description='The identifier of an end-user owner of an appeal sequence.'),
  appeal_seq_create_user_id STRING OPTIONS(description='The identifier of an end-user creator of an appeal sequence.'),
  appeal_seq_create_date_time DATETIME OPTIONS(description='The appeal sequence creation date.'),
  appeal_seq_update_user_id STRING OPTIONS(description='The appeal sequence update user id.'),
  appeal_seq_update_date_time DATETIME OPTIONS(description='The appeal sequence update date.'),
  appeal_disp_id_update_user_id STRING OPTIONS(description='The appeal sequence disposition identifier update end-user.'),
  appeal_disp_id_date_time DATETIME OPTIONS(description='The appeal sequence disposition identifier update date time.'),
  vendor_cd STRING OPTIONS(description='An appeal sequence related vendor code.'),
  appeal_seq_reopen_user_id STRING OPTIONS(description='Appeal sequence user that performed the reopen.'),
  appeal_seq_reopen_date_time DATETIME OPTIONS(description='Date when the appeal sequence was reopened.'),
  appeal_level_num INT64 OPTIONS(description='Appeal lvl is the count of appeal sequences excluding disposition of "Below Threshold" or "Inactive Not True Denial" or open date is the same as the close date & the appealed amt is 0 and appeal bal amt is 0 & appeal num is 1 & seq num is 1.'),
  appeal_sent_date DATE OPTIONS(description='Appeal Sent Date is the date upon which the appeal was mailed or uploaded to the payer for the given appeal sequence.'),
  prior_appeal_response_date DATE OPTIONS(description='Prior Appeal Response Date is the date upon which the response was received for the prior appeal sequence.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Last update date time of the record within the data warehouse.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  appeal_receipt_date  DATE OPTIONS(description='appeal receipt date is the date upon which the appeal was received.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Concuity appeal sequences related to an open appeal row.'
);
