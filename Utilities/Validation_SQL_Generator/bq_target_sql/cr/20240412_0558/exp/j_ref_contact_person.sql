-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_ref_contact_person.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    concat(format('%20d', count(*)), ',\t') AS source_string
  FROM
    (
      SELECT
          contact_person_stg.contact_person_desc,
          contact_person_stg.source_system_code
        FROM
          `hca-hin-dev-cur-ops`.edwcr_staging.contact_person_stg
        WHERE upper(trim(contact_person_stg.contact_person_desc)) NOT IN(
          SELECT
              upper(trim(ref_contact_person.contact_person_desc))
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_contact_person
        )
         AND contact_person_stg.dw_last_update_date_time < (
          SELECT
              max(etl_job_run.job_start_date_time) AS job_start_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_dmx_ac.etl_job_run
            WHERE upper(rtrim(etl_job_run.job_name)) = 'J_REF_CONTACT_PERSON'
        )
    ) AS a
;
