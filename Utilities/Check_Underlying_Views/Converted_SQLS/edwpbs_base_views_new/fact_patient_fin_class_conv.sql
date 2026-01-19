-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_patient_fin_class_conv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient_fin_class_conv AS SELECT
    fact_patient_fin_class_conv.company_code,
    fact_patient_fin_class_conv.coid,
    fact_patient_fin_class_conv.patient_dw_id,
    fact_patient_fin_class_conv.month_id,
    fact_patient_fin_class_conv.unit_num,
    fact_patient_fin_class_conv.pat_acct_num,
    fact_patient_fin_class_conv.patient_type_code,
    fact_patient_fin_class_conv.initial_financial_class_code,
    fact_patient_fin_class_conv.final_financial_class_code,
    fact_patient_fin_class_conv.financial_class_code_initial_date,
    fact_patient_fin_class_conv.financial_class_code_final_date,
    fact_patient_fin_class_conv.total_charge_amt,
    fact_patient_fin_class_conv.source_system_code,
    fact_patient_fin_class_conv.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_patient_fin_class_conv
;
