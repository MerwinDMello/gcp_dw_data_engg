-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/person.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.person AS SELECT
    ROUND(person.person_dw_id, 0, 'ROUND_HALF_EVEN') AS person_dw_id,
    person.name_prefix_text,
    person.first_name,
    person.middle_name,
    person.last_name,
    person.name_suffix_text,
    person.gender_code,
    person.native_language_text,
    person.person_birth_date,
    ROUND(person.social_security_num, 0, 'ROUND_HALF_EVEN') AS social_security_num,
    person.person_full_name,
    person.employee_type_code,
    person.pa_race_code,
    person.pa_ethnicity_code,
    person.source_system_code
  FROM
    {{ params.param_auth_base_views_dataset_name }}.person
;
