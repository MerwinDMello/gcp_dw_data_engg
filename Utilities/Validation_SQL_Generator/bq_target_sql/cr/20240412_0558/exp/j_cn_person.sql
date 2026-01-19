-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_person.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    concat(format('%20d', count(*)), ', ') AS source_string
  FROM
    (
      SELECT
          cn_person_stg.nav_patient_id,
          cn_person_stg.birth_date,
          cn_person_stg.first_name,
          cn_person_stg.last_name,
          cn_person_stg.middle_name,
          cn_person_stg.perferred_name,
          cn_person_stg.gender_code,
          cn_person_stg.preferred_langauage_text,
          cn_person_stg.death_date,
          cn_person_stg.patient_email_text,
          'N' AS source_system_code,
          cn_person_stg.dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-ops`.edwcr_staging.cn_person_stg
        WHERE cn_person_stg.nav_patient_id NOT IN(
          SELECT
              cn_person.nav_patient_id
            FROM
              `hca-hin-dev-cur-ops`.edwcr_base_views.cn_person
        )
    ) AS src
;
