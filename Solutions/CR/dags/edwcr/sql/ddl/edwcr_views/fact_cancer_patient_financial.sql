-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/fact_cancer_patient_financial.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/*==============================================================*/
/* Table: Fact_Cancer_Patient_Financial                         */
/*==============================================================*/
/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.fact_cancer_patient_financial AS SELECT
    a.encounter_id,
    a.encounter_source_system_code,
    a.empi_text,
    a.pat_acct_num,
    a.medical_record_num,
    a.patient_market_urn,
    a.encounter_start_date,
    a.encounter_end_date,
    a.coid,
    a.company_code,
    a.sub_unit_num,
    a.diagnosis_type_code,
    a.principal_diagnosis_code,
    a.procedure_type_code,
    a.principal_procedure_code,
    a.principal_procedure_date,
    a.total_billed_charge_amt,
    a.patient_age,
    a.patient_person_dw_id,
    a.financial_class_code,
    a.patient_type_code_pos1,
    a.emergency_ind,
    a.oncology_tumor_site_id,
    a.oncology_detail_tumor_site_id,
    a.robotic_ind3,
    a.op_product_line_desc,
    a.drg_code_hcfa,
    a.drg_medical_surgical_ind,
    a.calculated_length_of_stay_num,
    a.estimated_net_revenue_amt,
    a.direct_contribution_margin_amt,
    a.ebdita_amt,
    a.esl_level_1_desc,
    a.esl_level_2_desc,
    a.esl_level_3_desc,
    a.esl_level_4_desc,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.fact_cancer_patient_financial AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
