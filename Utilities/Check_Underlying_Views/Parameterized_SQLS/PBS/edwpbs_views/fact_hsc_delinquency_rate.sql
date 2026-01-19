-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_hsc_delinquency_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.fact_hsc_delinquency_rate
   OPTIONS(description='Health Information Management, Service Center Delinquency Rates rolled up to a facility and a month within a given year.')
  AS SELECT
      a.company_code,
      a.coid,
      a.month_id,
      a.patient_type_code_pos1,
      a.unit_num,
      a.charts_not_revw_thty_day_cnt,
      a.total_discharge_cnt,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.fact_hsc_delinquency_rate AS a
      INNER JOIN {{ params.param_pbs_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
