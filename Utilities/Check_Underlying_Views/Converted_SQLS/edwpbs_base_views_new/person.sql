-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/person.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.person AS SELECT
    person.person_dw_id,
    person.name_prefix_text,
    person.first_name,
    person.middle_name,
    person.last_name,
    person.name_suffix_text,
    person.gender_code,
    person.native_language_text,
    person.person_birth_date,
    person.social_security_num,
    person.person_full_name,
    person.employee_type_code,
    person.pa_race_code,
    person.pa_ethnicity_code,
    person.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.person
;
