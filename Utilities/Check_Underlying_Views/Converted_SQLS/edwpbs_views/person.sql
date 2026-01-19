-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/person.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.person AS SELECT
    ROUND(person.person_dw_id, 0, 'ROUND_HALF_EVEN') AS person_dw_id,
    person.name_prefix_text,
    CASE
      WHEN session_user() = z.userid THEN '***'
      ELSE person.first_name
    END AS first_name,
    CASE
      WHEN session_user() = z.userid THEN '***'
      ELSE person.middle_name
    END AS middle_name,
    CASE
      WHEN session_user() = z.userid THEN '***'
      ELSE person.last_name
    END AS last_name,
    person.name_suffix_text,
    person.gender_code,
    person.native_language_text,
    person.person_birth_date,
    CASE
      WHEN session_user() = zz.userid THEN substr(substr(ltrim(format('%#13.0f', person.social_security_num)), 1, 20), length(substr(ltrim(format('%#13.0f', person.social_security_num)), 1, 20)) - 9, 9)
      ELSE '***'
    END AS social_security_num,
    CASE
      WHEN session_user() = z.userid THEN '***'
      ELSE person.person_full_name
    END AS person_full_name,
    person.employee_type_code,
    person.pa_race_code,
    person.pa_ethnicity_code,
    person.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.person
    LEFT OUTER JOIN (
      SELECT
          sme.userid,
          sme.masked_column_code
        FROM
          `hca-hin-dev-cur-parallon`.edw_sec.security_mask_and_exception AS sme
        WHERE sme.userid = session_user()
         AND upper(sme.masked_column_code) = 'PN'
    ) AS z ON session_user() = z.userid
    LEFT OUTER JOIN (
      SELECT
          sme.userid,
          sme.masked_column_code
        FROM
          `hca-hin-dev-cur-parallon`.edw_sec.security_mask_and_exception AS sme
        WHERE session_user() = sme.userid
         AND upper(sme.masked_column_code) = 'SSN'
    ) AS zz ON session_user() = zz.userid
;
