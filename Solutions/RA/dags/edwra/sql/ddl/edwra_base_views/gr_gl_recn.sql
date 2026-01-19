-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/gr_gl_recn.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.gr_gl_recn
   OPTIONS(description='This table contains reconciliation data between Concuity and the General Ledger which serves as the backend data source for government reporting.')
  AS SELECT
      gr_gl_recn.company_code,
      gr_gl_recn.coid,
      gr_gl_recn.cost_report_year_begin,
      gr_gl_recn.pat_acct_num,
      gr_gl_recn.gl_acct_num,
      gr_gl_recn.log_24_section_name,
      gr_gl_recn.admission_date,
      gr_gl_recn.cost_report_year_end,
      gr_gl_recn.discharge_date,
      gr_gl_recn.fourth_day_delay_ind,
      gr_gl_recn.interim_bill_ind,
      gr_gl_recn.log_type,
      gr_gl_recn.pat_last_name,
      gr_gl_recn.total_ca_log_24_adj_amt,
      gr_gl_recn.total_ca_log_65_adj_amt,
      gr_gl_recn.total_gl_posted_acct_amt,
      gr_gl_recn.dw_last_update_date_time,
      gr_gl_recn.source_system_code
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.gr_gl_recn
  ;
