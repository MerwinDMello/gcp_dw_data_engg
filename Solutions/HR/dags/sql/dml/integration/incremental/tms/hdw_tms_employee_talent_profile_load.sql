BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_dt DATETIME;

SET current_dt = datetime_trunc(current_datetime('US/Central'), SECOND);
  DELETE FROM {{ params.param_hr_stage_dataset_name }}.employee_info WHERE employee_info.employee_id IN(
    '106431631', '119455195', '20949828', '43827771', '55558949'
  );

  CALL `{{ params.param_hr_core_dataset_name }}.sk_gen`("{{ params.param_hr_stage_dataset_name }}","employee_info","Trim(employee_id)","Employee_Talent_Profile");

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_talent_profile_reject;

/* Inserting bad data from staging table into reject table*/

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_talent_profile_reject (login, employee_id, last_name, first_name, middle_name, job_family, job_code, position_title, organization_and_hierarchy, manager, manager_employee_id, willing_to_travel, willing_travel_percentage, employee_mobilty_preferences, willing_to_relocate, relocation_preferences, calibrate_overall_perf_rating, overall_performance_rating, employee_promote_interest, potential, future_role1_leadership_level, future_role1_r_function_area, future_role1_org_size_scope, future_role1_r_timeframe, future_role2_leadership_level, future_role2_r_function_area, future_role2_org_size_scope, future_role2_r_timeframe, flight_risk, flight_risk_timeframe, external_flight_risk_driver, secondary_flight_risk_driver, jobs_pooled_for_count, positions_talent_pooled_count, positions_slated_for_count, readiness_unknown, readiness_others, ready_now, ready_6_11_months, ready_12_18_months, ready_18_24_months, successors_count, talent_pool_count, calibration_session_created_date, calibration_session_name, calibration_session_last_mod_date_time, calibration_session_published_date, calibration_session_created_by_name, company_tenure_text, calibration_box_name, position_tenure_text, dw_last_update_date_time, reject_reason, reject_stg_tbl_nm)
    SELECT
        stg.login,
        stg.employee_id,
        stg.last_name,
        stg.first_name,
        stg.middle_name,
        stg.job_family,
        stg.job_code,
        stg.position_title,
        stg.organization_and_hierarchy,
        stg.manager,
        stg.manager_employee_id,
        stg.willing_to_travel,
        stg.willing_travel_percentage,
        stg.employee_mobilty_preferences,
        stg.willing_to_relocate,
        stg.relocation_preferences,
        stg.calibrate_overall_perf_rating,
        stg.overall_performance_rating,
        stg.employee_promote_interest,
        stg.potential,
        stg.future_role1_leadership_level,
        stg.future_role1_r_function_area,
        stg.future_role1_org_size_scope,
        stg.future_role1_r_timeframe,
        stg.future_role2_leadership_level,
        stg.future_role2_r_function_area,
        stg.future_role2_org_size_scope,
        stg.future_role2_r_timeframe,
        stg.flight_risk,
        stg.flight_risk_timeframe,
        stg.external_flight_risk_driver,
        stg.secondary_flight_risk_driver,
        stg.jobs_pooled_for_count,
        stg.positions_talent_pooled_count,
        stg.positions_slated_for_count,
        stg.readiness_unknown,
        stg.readiness_others,
        stg.ready_now,
        stg.ready_6_11_months,
        stg.ready_12_18_months,
        stg.ready_18_24_months,
        stg.successors_count,
        stg.talent_pool_count,
        parse_date('%m/%d/%Y', trim(stg.calibrtn_ses_created_dt)) AS calibration_session_created_date,
        stg.calib_sess_name AS calibration_session_name,
        parse_datetime('%x %I:%M %p',REGEXP_EXTRACT(stg.calibrtn_ses_last_modif_dt, r'[0-9]+/[0-9]+/[0-9]+[\x20][0-9]+:[0-9]+[\x20][A-Z|a-z]+'))
        AS calibration_session_last_mod_date_time,
        CASE
          WHEN trim(stg.clb_ses_publ_to_talnt_prfl_dt) = '' THEN CAST(NULL as DATE)
          ELSE parse_date('%m/%d/%Y',REGEXP_EXTRACT(trim(stg.clb_ses_publ_to_talnt_prfl_dt), r'[0-9]+/[0-9]+/[0-9]+'))
        END
        AS clb_ses_publ_to_talnt_prfl_dt,

        stg.calibrtn_ses_created_by AS calibration_session_created_by_name,
        stg.time_with_company AS company_tenure_text,
        stg.calibration_box_name AS calibration_box_name,
        stg.time_in_position AS position_tenure_text,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
        CASE
          WHEN trim(stg.employee_id) IS NULL THEN 'Employee_ID is NULL'
          WHEN substr(trim(stg.employee_id), 1, 1) NOT IN(
            '1', '2', '3', '4', '5', '6', '7', '8', '9'
          ) THEN 'Employee_Id is alpha_numeric'
          ELSE CAST(0 as STRING)
        END AS reject_reason,
        'Employee_Info' AS reject_stg_tbl_nm
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_info AS stg
      WHERE substr(trim(stg.employee_id), 1, 1) NOT IN(
        '1', '2', '3', '4', '5', '6', '7', '8', '9'
      )
       OR length(trim(stg.employee_id)) >= 13
  ;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_talent_profile_wrk;

/*  Load Work Table with working Data */

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_talent_profile_wrk (employee_talent_profile_sid, employee_sid, employee_first_name, employee_middle_name, employee_last_name, employee_num, employee_3_4_login_code, job_family_text, str1, job_code, job_code_sid, position_code_text, position_code, position_sid, position_title_text, org_hierarchy_text, manager_name, manager_employee_num, travel_willingness_id, travel_willingness_pct_range_text, travel_location_text, relocation_willingness_id, relocation_location_text, calibrated_performance_rating_id, overall_performance_rating_id, employee_promotability_interest_id, potential_performance_id, future_role_1_leadership_level_id, future_role_1_org_size_id, future_role_1_timeframe_id, future_role_1_role_id, future_role_2_leadership_level_id, future_role_2_org_size_id, future_role_2_timeframe_id, future_role_2_role_id, flight_risk_probability_id, flight_risk_timeframe_id, flight_risk_driver_text, flight_risk_secondary_driver_text, calibration_session_created_date, calibration_session_name, calibration_session_last_mod_date_time, calibration_session_published_date, calibration_session_created_by_name, calibration_box_id, company_tenure_text, position_tenure_text, lawson_company_num, process_level_code, last_pub_calibration_session, last_pub_calibration_box_rank, source_system_code, dw_last_update_date_time)
    SELECT
        iq.*
      FROM
        (
          SELECT
              CAST(
                CASE
                WHEN trim(stg.employee_id) IS NOT NULL
                 AND substr(trim(stg.employee_id), 1, 1) IN(
                  '1', '2', '3', '4', '5', '6', '7', '8', '9'
                ) THEN xwlk.sk
                ELSE 0
              END AS INT64) AS employee_talent_profile_sid,
              coalesce(emp.employee_sid, 0) AS employee_sid,
              stg.first_name AS employee_first_name,
              stg.middle_name AS employee_middle_name,
              stg.last_name AS employee_last_name,
              CAST(CASE
                WHEN trim(stg.employee_id) IS NOT NULL
                 AND substr(trim(stg.employee_id), 1, 1) IN(
                  '1', '2', '3', '4', '5', '6', '7', '8', '9'
                ) THEN coalesce(trim(stg.employee_id), CAST(0 as STRING))
                ELSE CAST(0 as STRING)
              END AS INT64) AS employee_num,
              emp1.employee_34_login_code AS employee_3_4_login_code,
              stg.job_family AS job_family_text,
              CASE
                WHEN strpos(stg.job_family, '-') <> 0 THEN substr(stg.job_family, 1, strpos(stg.job_family, '-') - 1)
                ELSE CAST(NULL as STRING)
              END AS str1,
              CASE
                WHEN length(CASE
                  WHEN strpos(stg.job_family, '-') <> 0 THEN substr(stg.job_family, 1, strpos(stg.job_family, '-') - 1)
                  ELSE CAST(NULL as STRING)
                END) >= 9 THEN substr(CASE
                  WHEN strpos(stg.job_family, '-') <> 0 THEN substr(stg.job_family, 1, strpos(stg.job_family, '-') - 1)
                  ELSE CAST(NULL as STRING)
                END, 5, 5)
                WHEN length(CASE
                  WHEN strpos(stg.job_family, '-') <> 0 THEN substr(stg.job_family, 1, strpos(stg.job_family, '-') - 1)
                  ELSE CAST(NULL as STRING)
                END) = 5 THEN substr(CASE
                  WHEN strpos(stg.job_family, '-') <> 0 THEN substr(stg.job_family, 1, strpos(stg.job_family, '-') - 1)
                  ELSE CAST(NULL as STRING)
                END, 1, 5)
                ELSE CAST(NULL as STRING)
              END AS j_code,
              coalesce(jbc.job_code_sid, 0) AS job_code_sid,
              stg.job_code AS position_code_text,
              CASE
                WHEN length(stg.job_code) - 4 > 0 THEN substr(stg.job_code, 5, length(stg.job_code) - 4)
                ELSE CAST(NULL as STRING)
              END AS position_code,
              coalesce(jbp.position_sid, 0) AS position_sid,
              -- JBP.Position_Sid AS Position_Sid,
              stg.position_title AS position_title_text,
              stg.organization_and_hierarchy AS org_hierarchy_text,
              stg.manager AS manager_name,
              CAST(CASE
                WHEN trim(stg.manager_employee_id) IS NOT NULL
                 AND substr(trim(stg.manager_employee_id), 1, 1) IN(
                  '1', '2', '3', '4', '5', '6', '7', '8', '9'
                ) THEN coalesce(trim(stg.manager_employee_id), CAST(0 as STRING))
                ELSE CAST(0 as STRING)
              END AS INT64) AS manager_employee_num,
              coalesce(rlw.location_willingness_id, 0) AS travel_willingness_id,
              stg.willing_travel_percentage AS travel_willingness_pct_range_text,
              stg.employee_mobilty_preferences AS travel_location_text,
              rlw2.location_willingness_id AS relocation_willingness_id,
              stg.relocation_preferences AS relocation_location_text,
              coalesce(rpr.performance_rating_id, 0) AS calibrated_performance_rating_id,
              coalesce(rpr2.performance_rating_id, 0) AS overall_performance_rating_id,
              coalesce(rpp.probability_potential_id, 0)  AS employee_promotability_interest_id,
              coalesce(rpp2.probability_potential_id, 0)  AS potential_performance_id,
              coalesce(rfr.future_role_attribute_id, 0) AS future_role_1_leadership_level_id,
              coalesce(rfr2.future_role_attribute_id, 0) AS future_role_1_org_size_id,
              coalesce(rfr3.future_role_attribute_id, 0)  AS future_role_1_timeframe_id,
              coalesce(rfr4.future_role_attribute_id, 0)  AS future_role_1_role_id,
              coalesce(rfr5.future_role_attribute_id, 0)  AS future_role_2_leadership_level_id,
              coalesce(rfr6.future_role_attribute_id, 0)  AS future_role_2_org_size_id,
              coalesce(rfr7.future_role_attribute_id, 0) AS future_role_2_timeframe_id,
              coalesce(rfr8.future_role_attribute_id, 0)  AS future_role_2_role_id,
              coalesce(rpp3.probability_potential_id, 0)  AS flight_risk_probability_id,
              coalesce(rt.timeframe_id, 0) AS flight_risk_timeframe_id,
              stg.external_flight_risk_driver AS flight_risk_driver_text,
              stg.secondary_flight_risk_driver AS flight_risk_secondary_driver_text,
              parse_date('%m/%d/%Y', trim(stg.calibrtn_ses_created_dt)) AS calibration_session_created_date,
              stg.calib_sess_name AS calibration_session_name,
              PARSE_DATETIME('%m/%d/%Y %I:%M %p', ( concat(CASE
                  WHEN length(substr(trim(stg.calibrtn_ses_last_modif_dt), 1, strpos(stg.calibrtn_ses_last_modif_dt, '/') - 1)) < 2 
                  THEN concat('0', substr(trim(stg.calibrtn_ses_last_modif_dt), 1, strpos(stg.calibrtn_ses_last_modif_dt, '/') - 1), '/')
                  ELSE concat(substr(trim(stg.calibrtn_ses_last_modif_dt), 1, strpos(stg.calibrtn_ses_last_modif_dt, '/') - 1), '/')
                  END, 
                  CASE
                  WHEN length(replace(substr(trim(stg.calibrtn_ses_last_modif_dt), strpos(stg.calibrtn_ses_last_modif_dt, '/') + 1, 2), '/', '')) < 2 
                  THEN concat('0', replace(substr(trim(stg.calibrtn_ses_last_modif_dt), strpos(stg.calibrtn_ses_last_modif_dt, '/') + 1, 2), '/', ''), '/')
                  ELSE concat(replace(substr(trim(stg.calibrtn_ses_last_modif_dt), strpos(stg.calibrtn_ses_last_modif_dt, '/') + 1, 2), '/', ''), '/')
                  END, trim(CAST(extract(YEAR from datetime_trunc(current_datetime('US/Central'), SECOND)) as STRING)), ' ',
                  CASE
                  WHEN length(trim(substr(stg.calibrtn_ses_last_modif_dt, strpos(stg.calibrtn_ses_last_modif_dt, ' ') + 1, 8))) = 7 
                  THEN concat('0', trim(substr(stg.calibrtn_ses_last_modif_dt, strpos(stg.calibrtn_ses_last_modif_dt, ' ') + 1, 8)))

                  ELSE trim(substr(stg.calibrtn_ses_last_modif_dt, strpos(stg.calibrtn_ses_last_modif_dt, ' ') + 1, 8))
                  END))) AS calibration_session_last_mod_date_time,
              CASE
                WHEN trim(stg.clb_ses_publ_to_talnt_prfl_dt) = '' THEN CAST(NULL as DATE)
                ELSE parse_date('%m/%d/%Y', trim(stg.clb_ses_publ_to_talnt_prfl_dt))
              END AS clb_ses_publ_to_talnt_prfl_dt,
              stg.calibrtn_ses_created_by AS calibration_session_created_by_name,
              CASE
                WHEN trim(stg.calibration_box_name) = 'Not Specified' THEN 100
                WHEN trim(stg.calibration_box_name) = 'Too Soon To Tell' THEN 101
                ELSE CASE
                   substr(stg.calibration_box_name, 1, 1)
                  WHEN '' THEN 0
                  ELSE CAST(substr(stg.calibration_box_name, 1, 1) as INT64)
                END
              END AS calibration_box_id,
              stg.time_with_company AS company_tenure_text,
              stg.time_in_position AS position_tenure_text,
              coalesce(CASE
                 substr(stg.job_code, 1, 4)
                WHEN '' THEN 0
                ELSE CAST(substr(stg.job_code, 1, 4) as INT64)
              END, 0) AS lawson_company_num,
              '00000' AS process_level_code,
              stg.last_pub_calibration_session AS last_pub_calibration_session,
              stg.last_pub_calibration_box_rank AS last_pub_calibration_box_rank,
              'M' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee_info AS stg
              INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON trim(stg.employee_id) = xwlk.sk_source_txt
               AND upper(xwlk.sk_type) = 'EMPLOYEE_TALENT_PROFILE'
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON coalesce(trim(stg.employee_id), CAST(0 as STRING)) = coalesce(cast(emp.employee_num as STRING), CAST(0 as STRING))
               AND CASE
                 substr(stg.job_code, 1, 4)
                WHEN '' THEN 0
                ELSE CAST(substr(stg.job_code, 1, 4) as INT64)
              END = emp.lawson_company_num
               AND emp.valid_to_date = DATETIME("9999-12-31 23:59:59")
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp1 ON coalesce(trim(stg.employee_id), CAST(0 as STRING)) = coalesce(cast(emp1.employee_num as STRING), CAST(0 as STRING))
               AND emp1.valid_to_date = DATETIME("9999-12-31 23:59:59")
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jbc ON CASE
                WHEN length(CASE
                  WHEN strpos(stg.job_family, '-') <> 0 THEN substr(stg.job_family, 1, strpos(stg.job_family, '-') - 1)
                  ELSE CAST(NULL as STRING)
                END) >= 9 THEN substr(CASE
                  WHEN strpos(stg.job_family, '-') <> 0 THEN substr(stg.job_family, 1, strpos(stg.job_family, '-') - 1)
                  ELSE CAST(NULL as STRING)
                END, 5, 5)
                WHEN length(CASE
                  WHEN strpos(stg.job_family, '-') <> 0 THEN substr(stg.job_family, 1, strpos(stg.job_family, '-') - 1)
                  ELSE CAST(NULL as STRING)
                END) = 5 THEN substr(CASE
                  WHEN strpos(stg.job_family, '-') <> 0 THEN substr(stg.job_family, 1, strpos(stg.job_family, '-') - 1)
                  ELSE CAST(NULL as STRING)
                END, 1, 5)
                ELSE CAST(NULL as STRING)
              END = jbc.job_code
               AND CASE
                 substr(stg.job_code, 1, 4)
                WHEN '' THEN 0
                ELSE CAST(substr(stg.job_code, 1, 4) as INT64)
              END = jbc.lawson_company_num
               AND jbc.valid_to_date = DATETIME("9999-12-31 23:59:59")
              LEFT OUTER JOIN (
                SELECT DISTINCT
                    job_position.lawson_company_num,
                    job_position.position_code,
                    job_position.position_sid,
                    job_position.valid_to_date
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.job_position
              ) AS jbp ON CASE
                WHEN length(stg.job_code) - 4 > 0 THEN substr(stg.job_code, 5, length(stg.job_code) - 4)
              END = jbp.position_code
               AND CASE
                 substr(stg.job_code, 1, 4)
                WHEN '' THEN 0
                ELSE CAST(substr(stg.job_code, 1, 4) as INT64)
              END = jbp.lawson_company_num
               AND jbp.valid_to_date = DATETIME("9999-12-31 23:59:59")
              LEFT OUTER JOIN 
              {{ params.param_hr_base_views_dataset_name }}.ref_location_willingness AS rlw 
              ON Upper(Trim(rlw.location_willingness_desc)) = Upper(Trim(stg.willing_to_travel))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_location_willingness AS rlw2 
              ON Upper(Trim(rlw2.location_willingness_desc)) = Upper(Trim(stg.willing_to_relocate))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_performance_rating AS rpr 
              ON Upper(Trim(rpr.performance_rating_desc)) = Upper(Trim(stg.calibrate_overall_perf_rating))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_performance_rating AS rpr2 
              ON Upper(Trim(rpr2.performance_rating_desc)) = Upper(Trim(stg.overall_performance_rating))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_probability_potential AS rpp 
              ON Upper(Trim(rpp.probability_potential_desc)) = Upper(Trim(stg.employee_promote_interest))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_probability_potential AS rpp2 
              ON Upper(Trim(rpp2.probability_potential_desc)) = Upper(Trim(stg.potential))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_future_role_attribute AS rfr 
              ON Upper(Trim(rfr.future_role_attribute_desc)) = Upper(Trim(stg.future_role1_leadership_level))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_future_role_attribute AS rfr2 
              ON Upper(Trim(rfr2.future_role_attribute_desc)) = Upper(Trim(stg.future_role1_org_size_scope))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_future_role_attribute AS rfr3 
              ON Upper(Trim(rfr3.future_role_attribute_desc)) = Upper(Trim(stg.future_role1_r_timeframe))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_future_role_attribute AS rfr4 
              ON Upper(Trim(rfr4.future_role_attribute_desc)) = Upper(Trim(stg.future_role1_r_function_area))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_future_role_attribute AS rfr5 
              ON Upper(Trim(rfr5.future_role_attribute_desc)) = Upper(Trim(stg.future_role2_leadership_level))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_future_role_attribute AS rfr6 
              ON Upper(Trim(rfr6.future_role_attribute_desc)) = Upper(Trim(stg.future_role2_org_size_scope))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_future_role_attribute AS rfr7 
              ON Upper(Trim(rfr7.future_role_attribute_desc)) = Upper(Trim(stg.future_role2_r_timeframe))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_future_role_attribute AS rfr8 
              ON Upper(Trim(rfr8.future_role_attribute_desc)) = Upper(Trim(stg.future_role2_r_function_area))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_probability_potential AS rpp3 
              ON Upper(Trim(rpp3.probability_potential_desc)) = Upper(Trim(stg.flight_risk))
              LEFT OUTER JOIN
              {{ params.param_hr_base_views_dataset_name }}.ref_timeframe AS rt ON trim(rt.timeframe_desc) = trim(stg.flight_risk_timeframe)
            WHERE length(trim(stg.employee_id)) < 13
             AND substr(trim(stg.employee_id), 1, 1) IN(
              '1', '2', '3', '4', '5', '6', '7', '8', '9'
            )
             AND safe_cast(stg.calibrtn_ses_created_dt AS DATE) IS NULL
        ) AS iq
      WHERE iq.last_pub_calibration_session = iq.calibration_session_name
      QUALIFY row_number() OVER (PARTITION BY iq.employee_num ORDER BY iq.calibration_session_last_mod_date_time DESC) = 1
  ;



BEGIN TRANSACTION;
  UPDATE {{ params.param_hr_core_dataset_name }}.employee_talent_profile AS tgt 
  SET valid_to_date = (current_dt - interval 1 second), 
  dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) 
  FROM {{ params.param_hr_stage_dataset_name }}.employee_talent_profile_wrk AS wrk 
  WHERE tgt.employee_talent_profile_sid = wrk.employee_talent_profile_sid
   AND (coalesce(tgt.employee_sid, 0) <> coalesce(wrk.employee_sid, 0)
          OR Upper(trim(coalesce(tgt.employee_first_name, ''))) <> Upper(trim(coalesce(wrk.employee_first_name, '')))
          OR Upper(trim(coalesce(tgt.employee_middle_name, ''))) <> Upper(trim(coalesce(wrk.employee_middle_name, '')))
          OR Upper(trim(coalesce(tgt.employee_last_name, ''))) <> Upper(trim(coalesce(wrk.employee_last_name, '')))
          OR coalesce(tgt.employee_num, 0) <> coalesce(wrk.employee_num, 0)
          OR Upper(trim(coalesce(tgt.employee_3_4_login_code, ''))) <> Upper(trim(coalesce(wrk.employee_3_4_login_code, '')))
          OR Upper(trim(coalesce(tgt.job_family_text, ''))) <> Upper(trim(coalesce(wrk.job_family_text, '')))
          OR Upper(trim(coalesce(tgt.job_code, ''))) <> Upper(trim(coalesce(wrk.job_code, '')))
          OR coalesce(tgt.job_code_sid, 0) <> coalesce(wrk.job_code_sid, 0)
          OR Upper(trim(coalesce(tgt.position_code_text, ''))) <> Upper(trim(coalesce(wrk.position_code_text, '')))
          OR Upper(trim(coalesce(tgt.position_code, ''))) <> Upper(trim(coalesce(wrk.position_code, '')))
          OR coalesce(tgt.position_sid,0) <> coalesce(wrk.position_sid, 0)
          OR Upper(trim(coalesce(tgt.position_title_text, ''))) <> Upper(trim(coalesce(wrk.position_title_text, '')))
          OR Upper(trim(coalesce(tgt.org_hierarchy_text, ''))) <> Upper(trim(coalesce(wrk.org_hierarchy_text, '')))
          OR Upper(trim(coalesce(tgt.manager_name, ''))) <> Upper(trim(coalesce(wrk.manager_name, '')))
          OR coalesce(tgt.manager_employee_num,0) <> coalesce(wrk.manager_employee_num,0)
          OR coalesce(tgt.travel_willingness_id,0 ) <> coalesce(wrk.travel_willingness_id, 0)
          OR Upper(trim(coalesce(tgt.travel_willingness_pct_range_text, ''))) <> Upper(trim(coalesce(wrk.travel_willingness_pct_range_text, '')))
          OR Upper(trim(coalesce(tgt.travel_location_text, ''))) <> Upper(trim(coalesce(wrk.travel_location_text, '')))
          OR CAST(coalesce(tgt.relocation_willingness_id, 0) as STRING) <> CAST(coalesce(wrk.relocation_willingness_id, 0) as STRING)
          OR Upper(trim(coalesce(tgt.relocation_location_text, ''))) <> Upper(trim(coalesce(wrk.relocation_location_text, '')))
          OR coalesce(tgt.calibrated_performance_rating_id, 0) <>   coalesce(wrk.calibrated_performance_rating_id, 0)
          OR coalesce(tgt.overall_performance_rating_id, 0) <>      coalesce(wrk.overall_performance_rating_id, 0)
          OR coalesce(tgt.employee_promotability_interest_id, 0) <> coalesce(wrk.employee_promotability_interest_id, 0)
          OR coalesce(tgt.potential_performance_id, 0) <>           coalesce(wrk.potential_performance_id, 0)
          OR coalesce(tgt.future_role_1_leadership_level_id, 0) <>  coalesce(wrk.future_role_1_leadership_level_id, 0)
          OR coalesce(tgt.future_role_1_org_size_id, 0) <>          coalesce(wrk.future_role_1_org_size_id, 0)
          OR coalesce(tgt.future_role_1_timeframe_id, 0) <>         coalesce(wrk.future_role_1_timeframe_id, 0)
          OR coalesce(tgt.future_role_1_role_id, 0) <>              coalesce(wrk.future_role_1_role_id, 0)
          OR coalesce(tgt.future_role_2_leadership_level_id, 0) <>  coalesce(wrk.future_role_2_leadership_level_id, 0)
          OR coalesce(tgt.future_role_2_org_size_id, 0) <>          coalesce(wrk.future_role_2_org_size_id, 0)
          OR coalesce(tgt.future_role_2_timeframe_id, 0) <>         coalesce(wrk.future_role_2_timeframe_id, 0)
          OR coalesce(tgt.future_role_2_role_id, 0) <>              coalesce(wrk.future_role_2_role_id, 0)
          OR coalesce(tgt.flight_risk_probability_id, 0) <>         coalesce(wrk.flight_risk_probability_id, 0)
          OR coalesce(tgt.flight_risk_timeframe_id, 0) <>           coalesce(wrk.flight_risk_timeframe_id, 0)
          OR Upper(trim(coalesce(tgt.flight_risk_driver_text, ''))) <> Upper(trim(coalesce(wrk.flight_risk_driver_text, '')))
          OR Upper(trim(coalesce(tgt.flight_risk_secondary_driver_text, ''))) <> Upper(trim(coalesce(wrk.flight_risk_secondary_driver_text, '')))
          OR coalesce(safe_cast(tgt.calibration_session_created_date AS Date)) <> coalesce(safe_cast(wrk.calibration_session_created_date AS Date))
          OR Upper(trim(coalesce(tgt.calibration_session_name, ''))) <> Upper(trim(coalesce(wrk.calibration_session_name, '')))
          OR coalesce(safe_cast(tgt.calibration_session_last_mod_date_time AS Date)) <> coalesce(safe_cast(wrk.calibration_session_last_mod_date_time AS Date))
          OR coalesce(safe_cast(tgt.calibration_session_published_date AS DATE)) <> coalesce(safe_cast(wrk.calibration_session_published_date AS Date))
          OR Upper(trim(coalesce(tgt.calibration_session_created_by_name, ''))) <> Upper(trim(coalesce(wrk.calibration_session_created_by_name, '')))
          OR coalesce(tgt.calibration_box_id, 0) <> coalesce(wrk.calibration_box_id, 0)
          OR Upper(trim(coalesce(tgt.company_tenure_text, ''))) <> Upper(trim(coalesce(wrk.company_tenure_text, '')))
          OR Upper(trim(coalesce(tgt.position_tenure_text, ''))) <> Upper(trim(coalesce(wrk.position_tenure_text, '')))
          OR coalesce(tgt.lawson_company_num, 0) <> coalesce(wrk.lawson_company_num, 0))
          AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");


  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT employee_talent_profile_sid
      FROM {{ params.param_hr_core_dataset_name }}.employee_talent_profile
      GROUP BY employee_talent_profile_sid, valid_from_date
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.employee_talent_profile');
  ELSE
  COMMIT TRANSACTION;
  END IF;
/*(select * from  edwhr_Staging.Employee_Talent_Profile_Wrk
where Last_Pub_Calibration_Session = Calibration_Session_Name
qualify Row_Number () Over (Partition by Employee_Num Order by Calibration_Session_Last_Mod_Date_Time desc)  = 1) WRK */


BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_talent_profile (employee_talent_profile_sid, valid_from_date, employee_sid, employee_first_name, employee_middle_name, employee_last_name, employee_num, employee_3_4_login_code, job_family_text, job_code, job_code_sid, position_code_text, position_code, position_sid, position_title_text, org_hierarchy_text, manager_name, manager_employee_num, travel_willingness_id, travel_willingness_pct_range_text, travel_location_text, relocation_willingness_id, relocation_location_text, calibrated_performance_rating_id, overall_performance_rating_id, employee_promotability_interest_id, potential_performance_id, future_role_1_leadership_level_id, future_role_1_org_size_id, future_role_1_timeframe_id, future_role_1_role_id, future_role_2_leadership_level_id, future_role_2_org_size_id, future_role_2_timeframe_id, future_role_2_role_id, flight_risk_probability_id, flight_risk_timeframe_id, flight_risk_driver_text, flight_risk_secondary_driver_text, valid_to_date, calibration_session_created_date, calibration_session_name, calibration_session_last_mod_date_time, calibration_session_published_date, calibration_session_created_by_name, calibration_box_id, company_tenure_text, position_tenure_text, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        coalesce(wrk.employee_talent_profile_sid, 0),
        datetime_trunc(current_datetime('US/Central'), SECOND) AS valid_from_date,
        coalesce(wrk.employee_sid, 0),
        trim(coalesce(wrk.employee_first_name, '')),
        trim(coalesce(wrk.employee_middle_name, '')),
        trim(coalesce(wrk.employee_last_name, '')),
        coalesce(wrk.employee_num, 0),
        trim(coalesce(wrk.employee_3_4_login_code, '')),
        trim(coalesce(wrk.job_family_text, '')),
        trim(coalesce(wrk.job_code, '')),
        coalesce(wrk.job_code_sid, 0),
        trim(coalesce(wrk.position_code_text, '')),
        trim(coalesce(wrk.position_code, '')),
        coalesce(wrk.position_sid, 0),
        trim(coalesce(wrk.position_title_text, '')),
        trim(coalesce(wrk.org_hierarchy_text, '')),
        trim(coalesce(wrk.manager_name, '')),
        coalesce(wrk.manager_employee_num, 0),
        coalesce(wrk.travel_willingness_id, 0),
        trim(coalesce(wrk.travel_willingness_pct_range_text, '')),
        trim(coalesce(wrk.travel_location_text, '')),
        coalesce(wrk.relocation_willingness_id, 0),
        trim(coalesce(wrk.relocation_location_text, '')),
        coalesce(wrk.calibrated_performance_rating_id, 0),
        coalesce(wrk.overall_performance_rating_id, 0),
        coalesce(wrk.employee_promotability_interest_id, 0),
        coalesce(wrk.potential_performance_id, 0),
        coalesce(wrk.future_role_1_leadership_level_id, 0),
        coalesce(wrk.future_role_1_org_size_id, 0),
        coalesce(wrk.future_role_1_timeframe_id, 0),
        coalesce(wrk.future_role_1_role_id, 0),
        coalesce(wrk.future_role_2_leadership_level_id, 0),
        coalesce(wrk.future_role_2_org_size_id, 0),
        coalesce(wrk.future_role_2_timeframe_id, 0),
        coalesce(wrk.future_role_2_role_id, 0),
        coalesce(wrk.flight_risk_probability_id, 0),
        coalesce(wrk.flight_risk_timeframe_id, 0),
        trim(coalesce(wrk.flight_risk_driver_text, '')),
        trim(coalesce(wrk.flight_risk_secondary_driver_text, '')),
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        wrk.calibration_session_created_date,
        wrk.calibration_session_name,
        wrk.calibration_session_last_mod_date_time,
        wrk.calibration_session_published_date,
        wrk.calibration_session_created_by_name,
        coalesce(wrk.calibration_box_id, 0),
        wrk.company_tenure_text,
        wrk.position_tenure_text,
        coalesce(wrk.lawson_company_num, 0),
        wrk.process_level_code,
        wrk.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_talent_profile_wrk AS wrk
      WHERE ( wrk.employee_talent_profile_sid, 
              coalesce(wrk.employee_sid, 0), 
              Upper(trim(coalesce(wrk.employee_first_name, ''))), 
              Upper(trim(coalesce(wrk.employee_middle_name, ''))), 
              Upper(trim(coalesce(wrk.employee_last_name, ''))), 
              coalesce(wrk.employee_num, 0), 
              Upper(trim(coalesce(wrk.employee_3_4_login_code, ''))), 
              Upper(trim(coalesce(wrk.job_family_text, ''))), 
              Upper(trim(coalesce(wrk.job_code, ''))), 
              coalesce(wrk.job_code_sid, 0), 
              Upper(trim(coalesce(wrk.position_code_text, ''))), 
              Upper(trim(coalesce(wrk.position_code, ''))), 
              coalesce(wrk.position_sid, 0), 
              upper(trim(coalesce(wrk.position_title_text, ''))), 
              upper(trim(coalesce(wrk.org_hierarchy_text, ''))), 
              Upper(trim(coalesce(wrk.manager_name, ''))), 
              coalesce(wrk.manager_employee_num, 0), 
              coalesce(wrk.travel_willingness_id, 0), 
              upper(trim(coalesce(wrk.travel_willingness_pct_range_text, ''))), 
              upper(trim(coalesce(wrk.travel_location_text, ''))), 
              coalesce(wrk.relocation_willingness_id, 0), 
              upper(trim(coalesce(wrk.relocation_location_text, ''))), 
              coalesce(wrk.calibrated_performance_rating_id, 0), 
              coalesce(wrk.overall_performance_rating_id, 0), 
              coalesce(wrk.employee_promotability_interest_id, 0), 
              coalesce(wrk.potential_performance_id, 0), 
              coalesce(wrk.future_role_1_leadership_level_id, 0), 
              coalesce(wrk.future_role_1_org_size_id, 0), 
              coalesce(wrk.future_role_1_timeframe_id, 0), 
              coalesce(wrk.future_role_1_role_id, 0), 
              coalesce(wrk.future_role_2_leadership_level_id, 0), 
              coalesce(wrk.future_role_2_org_size_id, 0), 
              coalesce(wrk.future_role_2_timeframe_id, 0),
              coalesce(wrk.future_role_2_role_id, 0), 
              coalesce(wrk.flight_risk_probability_id, 0), 
              coalesce(wrk.flight_risk_timeframe_id, 0),
              Upper(trim(coalesce(wrk.flight_risk_driver_text, ''))), 
              Upper(trim(coalesce(wrk.flight_risk_secondary_driver_text, ''))), 
              coalesce(wrk.calibration_session_created_date, DATE '9999-01-01'), 
              upper(trim(coalesce(wrk.calibration_session_name, ''))), 
              coalesce(wrk.calibration_session_last_mod_date_time, DATE '9999-01-01'), 
              coalesce(wrk.calibration_session_published_date, DATE '9999-01-01'), 
              upper(trim(coalesce(wrk.calibration_session_created_by_name, ''))), 
              coalesce(wrk.calibration_box_id, 0), 
              upper(trim(coalesce(wrk.company_tenure_text, ''))), 
              upper(trim(coalesce(wrk.position_tenure_text, ''))), 
              coalesce(wrk.lawson_company_num, 0)) 
        NOT IN(
        SELECT AS STRUCT
            tgt.employee_talent_profile_sid,
            coalesce(tgt.employee_sid, 0),
            Upper(trim(coalesce(tgt.employee_first_name, ''))),
            Upper(trim(coalesce(tgt.employee_middle_name, ''))),
            Upper(trim(coalesce(tgt.employee_last_name, ''))),
            coalesce(tgt.employee_num, 0),
            Upper(trim(coalesce(tgt.employee_3_4_login_code, ''))),
            Upper(trim(coalesce(tgt.job_family_text, ''))),
            Upper(trim(coalesce(tgt.job_code, ''))),
            coalesce(tgt.job_code_sid, 0),
            Upper(trim(coalesce(tgt.position_code_text, ''))),
            Upper(trim(coalesce(tgt.position_code, ''))),
            coalesce(tgt.position_sid, 0),
            Upper(trim(coalesce(tgt.position_title_text, ''))),
            Upper(trim(coalesce(tgt.org_hierarchy_text, ''))),
            Upper(trim(coalesce(tgt.manager_name, ''))),
            coalesce(tgt.manager_employee_num, 0),
            coalesce(tgt.travel_willingness_id, 0),
            Upper(trim(coalesce(tgt.travel_willingness_pct_range_text, ''))),
            Upper(trim(coalesce(tgt.travel_location_text, ''))),
            coalesce(tgt.relocation_willingness_id, 0),
            Upper(trim(coalesce(tgt.relocation_location_text, ''))),
            coalesce(tgt.calibrated_performance_rating_id, 0),
            coalesce(tgt.overall_performance_rating_id, 0),
            coalesce(tgt.employee_promotability_interest_id, 0),
            coalesce(tgt.potential_performance_id, 0),
            coalesce(tgt.future_role_1_leadership_level_id, 0),
            coalesce(tgt.future_role_1_org_size_id, 0),
            coalesce(tgt.future_role_1_timeframe_id, 0),
            coalesce(tgt.future_role_1_role_id, 0),
            coalesce(tgt.future_role_2_leadership_level_id, 0),
            coalesce(tgt.future_role_2_org_size_id, 0),
            coalesce(tgt.future_role_2_timeframe_id, 0),
            coalesce(tgt.future_role_2_role_id, 0),
            coalesce(tgt.flight_risk_probability_id, 0),
            coalesce(tgt.flight_risk_timeframe_id, 0),
            Upper(trim(coalesce(tgt.flight_risk_driver_text, ''))),
            Upper(trim(coalesce(tgt.flight_risk_secondary_driver_text, ''))),
            coalesce(wrk.calibration_session_created_date, DATE '9999-01-01'),
            upper(trim(coalesce(wrk.calibration_session_name, ''))),
            coalesce(wrk.calibration_session_last_mod_date_time, DATE '9999-01-01'),
            coalesce(wrk.calibration_session_published_date, DATE '9999-01-01'),
            upper(trim(coalesce(wrk.calibration_session_created_by_name, ''))),
            coalesce(tgt.calibration_box_id, 0),
            upper(trim(coalesce(wrk.company_tenure_text, ''))),
            upper(trim(coalesce(wrk.position_tenure_text, ''))),
            coalesce(tgt.lawson_company_num, 0)
          FROM
            {{ params.param_hr_core_dataset_name }}.employee_talent_profile AS tgt
          WHERE tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
      )
  ;

  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT employee_talent_profile_sid
      FROM {{ params.param_hr_core_dataset_name }}.employee_talent_profile
      GROUP BY employee_talent_profile_sid, valid_from_date
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT('Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.employee_talent_profile');
  ELSE
  COMMIT TRANSACTION;
  END IF;
/*And WRK.Last_Pub_Calibration_Session = WRK.Calibration_Session_Name
qualify Row_Number () Over (Partition by Employee_Num Order by Calibration_Session_Last_Mod_Date_Time desc)  = 1 */
-- qualify count(*) over (partition by  Employee_Talent_Profile_SID order by Employee_Talent_Profile_SID) = 1

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_talent_profile AS tgt
   SET valid_to_date = (current_dt - interval 1 second),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) 
    WHERE tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
   AND tgt.employee_talent_profile_sid NOT IN(
    SELECT DISTINCT
        employee_talent_profile_wrk.employee_talent_profile_sid
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_talent_profile_wrk
  );

END;