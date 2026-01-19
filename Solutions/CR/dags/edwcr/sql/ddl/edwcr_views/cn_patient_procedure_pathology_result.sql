-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cn_patient_procedure_pathology_result.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cn_patient_procedure_pathology_result AS SELECT
    a.cn_patient_proc_pathology_result_sid,
    a.cn_patient_procedure_sid,
    a.margin_result_id,
    a.nav_result_id,
    a.oncotype_diagnosis_result_id,
    a.navigation_procedure_type_code,
    a.pathology_result_date,
    a.pathology_result_name,
    a.pathology_grade_available_ind,
    a.pathology_grade_num,
    a.pathology_tumor_size_available_ind,
    a.tumor_size_num_text,
    a.margin_result_detail_text,
    a.sentinel_node_result_code,
    a.estrogen_receptor_sw,
    a.estrogen_receptor_strength_code,
    a.estrogen_receptor_pct_text,
    a.progesterone_receptor_sw,
    a.progesterone_receptor_strength_code,
    a.progesterone_receptor_pct_text,
    a.oncotype_diagnosis_score_num,
    a.oncotype_diagnosis_risk_text,
    a.comment_text,
    a.hashbite_ssk,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cn_patient_procedure_pathology_result AS a
;
