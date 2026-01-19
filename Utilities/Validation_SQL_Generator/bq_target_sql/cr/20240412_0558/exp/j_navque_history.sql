-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_navque_history.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    concat(format('%20d', count(*)), ',\t') AS source_string
  FROM
    (
      SELECT
          stg.navque_history_id,
          stg.navque_action_id,
          stg.navque_reason_id,
          stg.tumor_type_id,
          stg.navigator_id,
          stg.coid,
          stg.company_code,
          stg.message_control_id_text,
          stg.message_date,
          stg.navque_insert_date,
          stg.navque_action_date,
          stg.medical_record_num,
          stg.patient_market_urn,
          stg.network_mnemonic_cs,
          stg.transition_of_care_score_num,
          stg.navigated_patient_ind,
          stg.message_source_flag,
          stg.hashbite_ssk,
          stg.source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-ops`.edwcr_staging.navque_history_stg AS stg
        WHERE upper(trim(stg.hashbite_ssk)) NOT IN(
          SELECT
              upper(trim(navque_history.hashbite_ssk))
            FROM
              `hca-hin-dev-cur-ops`.edwcr.navque_history
        )
    ) AS a
;
