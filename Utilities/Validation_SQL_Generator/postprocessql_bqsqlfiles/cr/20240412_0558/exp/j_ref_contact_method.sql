-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_ref_contact_method.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY upper(trim(type_stg.contact_method_desc))) +
     (SELECT coalesce(max(ref_contact_method.contact_method_id), 0) AS id1
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_contact_method) AS contact_method_id,
                            trim(type_stg.contact_method_desc) AS contact_method_desc,
                            type_stg.source_system_code AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT ref_contact_method_stg.contact_method_desc,
             ref_contact_method_stg.source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_contact_method_stg
      WHERE upper(trim(ref_contact_method_stg.contact_method_desc)) NOT IN
          (SELECT upper(trim(ref_contact_method.contact_method_desc))
           FROM `hca-hin-dev-cur-ops`.edwcr.ref_contact_method
           WHERE ref_contact_method.contact_method_desc IS NOT NULL ) ) AS type_stg
   WHERE type_stg.contact_method_desc IS NOT NULL
     AND upper(trim(type_stg.contact_method_desc)) <> '' ) AS a