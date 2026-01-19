-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra/gr_gl_recn.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE TABLE IF NOT EXISTS {{ params.param_parallon_ra_core_dataset_name }}.gr_gl_recn
(
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company which a facility is affiliated for application processing purposes'),
  coid STRING NOT NULL OPTIONS(description='The company identifier which uniquely identifies a facility to corporate and all other facilities.'),
  cost_report_year_begin DATE NOT NULL OPTIONS(description='The beginning cost reportng year to which this record falls under.  Each facility can have a different cost reporting period.'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description='The account number for a given patient per facility.'),
  gl_acct_num STRING NOT NULL OPTIONS(description='The general ledger account number.'),
  log_24_section_name STRING NOT NULL OPTIONS(description='The associated log 24 report section name.'),
  admission_date DATE OPTIONS(description='The date when a patient was admitted to a facility.'),
  cost_report_year_end DATE OPTIONS(description='The beginning cost reportng year to which this record falls under.  Each facility can have a different cost reporting period.'),
  discharge_date DATE OPTIONS(description='The date when a patient was discharged from a facility.'),
  fourth_day_delay_ind STRING OPTIONS(description='A one character field that indicates if a record was flagged as delayed on the fourth day of the month.'),
  interim_bill_ind STRING OPTIONS(description='A one character field that indicates if a record was flagged as interim bill.'),
  log_type STRING OPTIONS(description='Identifies the type of logging record used.'),
  pat_last_name STRING OPTIONS(description='The last name of a given patient.'),
  total_ca_log_24_adj_amt NUMERIC(32, 3) OPTIONS(description='The total contractual adjustment amount for government logging 24.'),
  total_ca_log_65_adj_amt NUMERIC(32, 3) OPTIONS(description='The total contractual adjustment amount for government logging 65.'),
  total_gl_posted_acct_amt NUMERIC(32, 3) OPTIONS(description='The total amount extracted from the general ledger.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='The last date and time of record refresh in the enterprise data warehouse.'),
  source_system_code STRING NOT NULL OPTIONS(description='A single character code that identifies the source system for data.')
)
PARTITION BY DATE_TRUNC(cost_report_year_begin, MONTH)
CLUSTER BY company_code, coid, pat_acct_num, gl_acct_num
OPTIONS(
  description='This table contains reconciliation data between Concuity and the General Ledger which serves as the backend data source for government reporting.'
);
