CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_brca_screening_assessment
   OPTIONS(description='Contains the details behind a patients Breast Cancer gene mutation assessment.')
  AS SELECT
      cn_patient_brca_screening_assessment.brca_screening_assessment_sid,
      cn_patient_brca_screening_assessment.nav_patient_id,
      cn_patient_brca_screening_assessment.tumor_type_id,
      cn_patient_brca_screening_assessment.navigator_id,
      cn_patient_brca_screening_assessment.coid,
      cn_patient_brca_screening_assessment.company_code,
      cn_patient_brca_screening_assessment.early_onset_breast_cancer_ind,
      cn_patient_brca_screening_assessment.ovarian_cancer_history_ind,
      cn_patient_brca_screening_assessment.two_primary_breast_cancer_ind,
      cn_patient_brca_screening_assessment.male_breast_cancer_ind,
      cn_patient_brca_screening_assessment.triple_negative_ind,
      cn_patient_brca_screening_assessment.ashkenazi_jewish_ind,
      cn_patient_brca_screening_assessment.two_plus_relative_cancer_ind,
      cn_patient_brca_screening_assessment.meeting_assessment_critieria_ind,
      cn_patient_brca_screening_assessment.hashbite_ssk,
      cn_patient_brca_screening_assessment.source_system_code,
      cn_patient_brca_screening_assessment.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_brca_screening_assessment
  ;
