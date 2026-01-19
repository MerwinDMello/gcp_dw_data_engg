-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_patient_fin_class_conv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.fact_patient_fin_class_conv
   OPTIONS(description='This table captures tracks the financial class conversions on the patient account from initial registration to service/bill date within a monthly reporting period.')
  AS SELECT
      fact_patient_fin_class_conv.company_code,
      fact_patient_fin_class_conv.coid,
      ROUND(fact_patient_fin_class_conv.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_patient_fin_class_conv.month_id,
      fact_patient_fin_class_conv.unit_num,
      ROUND(fact_patient_fin_class_conv.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      fact_patient_fin_class_conv.patient_type_code,
      fact_patient_fin_class_conv.initial_financial_class_code,
      fact_patient_fin_class_conv.final_financial_class_code,
      fact_patient_fin_class_conv.financial_class_code_initial_date,
      fact_patient_fin_class_conv.financial_class_code_final_date,
      ROUND(fact_patient_fin_class_conv.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      fact_patient_fin_class_conv.source_system_code,
      fact_patient_fin_class_conv.dw_last_update_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.fact_patient_fin_class_conv
  ;
