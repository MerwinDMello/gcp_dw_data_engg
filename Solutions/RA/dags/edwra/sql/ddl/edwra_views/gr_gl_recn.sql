-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/gr_gl_recn.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.gr_gl_recn
   OPTIONS(description='This table contains reconciliation data between Concuity and the General Ledger which serves as the backend data source for government reporting.')
  AS SELECT
      a.company_code,
      a.coid,
      a.cost_report_year_begin,
      a.pat_acct_num,
      a.gl_acct_num,
      a.log_24_section_name,
      a.admission_date,
      a.cost_report_year_end,
      a.discharge_date,
      a.fourth_day_delay_ind,
      a.interim_bill_ind,
      a.log_type,
      a.pat_last_name,
      a.total_ca_log_24_adj_amt,
      a.total_ca_log_65_adj_amt,
      a.total_gl_posted_acct_amt,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.gr_gl_recn AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
