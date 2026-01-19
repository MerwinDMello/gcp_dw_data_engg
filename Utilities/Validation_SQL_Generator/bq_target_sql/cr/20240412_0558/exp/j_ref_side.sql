-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_ref_side.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    concat(format('%20d', count(*)), ',\t') AS source_string
  FROM
    (
      SELECT
          row_number() OVER (ORDER BY upper(trim(src.side_desc))) + (
            SELECT
                coalesce(max(ref_side.side_id), 0) AS id1
              FROM
                `hca-hin-dev-cur-ops`.edwcr.ref_side
          ) AS side_id,
          trim(src.side_desc) AS side_desc,
          src.source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          (
            SELECT
                ref_side_stg.side_desc,
                ref_side_stg.source_system_code
              FROM
                `hca-hin-dev-cur-ops`.edwcr_staging.ref_side_stg
              WHERE upper(trim(ref_side_stg.side_desc)) NOT IN(
                SELECT
                    upper(trim(ref_side.side_desc))
                  FROM
                    `hca-hin-dev-cur-ops`.edwcr.ref_side
                  WHERE ref_side.side_desc IS NOT NULL
              )
          ) AS src
        WHERE src.side_desc IS NOT NULL
         AND upper(rtrim(src.side_desc)) <> ''
    ) AS a
;
