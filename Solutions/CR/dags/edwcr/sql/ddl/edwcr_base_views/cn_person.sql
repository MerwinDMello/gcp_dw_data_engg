CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_person
   OPTIONS(description='Contains all the demographic data for each person navigated by Sarah Cannon')
  AS SELECT
      cn_person.nav_patient_id,
      cn_person.birth_date,
      cn_person.first_name,
      cn_person.last_name,
      cn_person.middle_name,
      cn_person.preferred_name,
      cn_person.gender_code,
      cn_person.preferred_language_text,
      cn_person.death_date,
      cn_person.patient_email_text,
      cn_person.source_system_code,
      cn_person.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_person
  ;
