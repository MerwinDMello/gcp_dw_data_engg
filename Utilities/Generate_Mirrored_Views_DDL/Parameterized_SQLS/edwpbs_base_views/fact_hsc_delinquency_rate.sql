-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_hsc_delinquency_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.fact_hsc_delinquency_rate
   OPTIONS(description='Health Information Management, Service Center Delinquency Rates rolled up to a facility and a month within a given year.')
  AS SELECT
      fact_hsc_delinquency_rate.company_code,
      fact_hsc_delinquency_rate.coid,
      fact_hsc_delinquency_rate.month_id,
      fact_hsc_delinquency_rate.patient_type_code_pos1,
      fact_hsc_delinquency_rate.unit_num,
      fact_hsc_delinquency_rate.charts_not_revw_thty_day_cnt,
      fact_hsc_delinquency_rate.total_discharge_cnt,
      fact_hsc_delinquency_rate.source_system_code,
      fact_hsc_delinquency_rate.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.fact_hsc_delinquency_rate
  ;
