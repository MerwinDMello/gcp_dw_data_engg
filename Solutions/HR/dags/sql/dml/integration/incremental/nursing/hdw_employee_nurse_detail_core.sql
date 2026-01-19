DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND) ;

TRUNCATE TABLE {{params.param_hr_stage_dataset_name}}.employee_nurse_detail_wrk;
/* Load Work Table with working Data */
INSERT INTO {{params.param_hr_stage_dataset_name}}.employee_nurse_detail_wrk (employee_sid, report_date, ncsbn_id, employee_34_login_code, employee_num, lawson_company_num, process_level_code, license_num_text, source_system_code, dw_last_update_date_time)
  SELECT
      cw_win_ss.employee_sid,
      cw_win_ss.report_date,
      cw_win_ss.ncsbn_id,
      cw_win_ss.employee_34_login_code,
      cw_win_ss.employee_num,
      cw_win_ss.lawson_company_num,
      cw_win_ss.process_level_code,
      cw_win_ss.license_num_text,
      cw_win_ss.source_system_code,
      current_ts
    FROM
      (
        SELECT
            b.employee_sid,
            parse_date('%m/%d/%Y', a.report_date) AS report_date,
            round(cast(a.ncsbn_num as numeric), 2, "ROUND_HALF_EVEN") AS ncsbn_id,
            b.employee_34_login_code,
            CASE
               trim(a.ee_num)
              WHEN '' THEN 0
              ELSE CAST(trim(a.ee_num) as INT64)
            END AS employee_num,
            CASE
               trim(a.hr_company_code)
              WHEN '' THEN 0
              ELSE CAST(trim(a.hr_company_code) as INT64)
            END AS lawson_company_num,
            '00000' AS process_level_code,
            max(a.license_num) AS license_num_text,
            'P' AS source_system_code,
            current_ts,
            upper(a.license_num) AS cw_win_part_0
          FROM
            {{params.param_hr_stage_dataset_name}}.employee_nurse_detail_stg AS a
            INNER JOIN {{params.param_hr_base_views_dataset_name}}.employee AS b ON CASE
               trim(a.ee_num)
              WHEN '' THEN 0
              ELSE CAST(trim(a.ee_num) as INT64)
            END = b.employee_num
             AND CASE
               trim(a.hr_company_code)
              WHEN '' THEN 0
              ELSE CAST(trim(a.hr_company_code) as INT64)
            END = b.lawson_company_num
          WHERE cast(b.valid_to_date as date) = '9999-12-31'
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 11, 9
      ) AS cw_win_ss
    QUALIFY row_number() OVER (PARTITION BY cw_win_ss.employee_sid, cw_win_ss.cw_win_part_0 ORDER BY cw_win_ss.employee_sid DESC) = 1
;

INSERT INTO {{params.param_hr_core_dataset_name}}.employee_nurse_detail (employee_sid, report_date, ncsbn_id, employee_34_login_code, employee_num, lawson_company_num, process_level_code, license_num_text, source_system_code, dw_last_update_date_time)
  SELECT
      coalesce(a.employee_sid),
      a.report_date,
      coalesce(a.ncsbn_id),
      coalesce(a.employee_34_login_code),
      coalesce(a.employee_num),
      coalesce(cast(a.lawson_company_num as int64)),
      coalesce(a.process_level_code),
      coalesce(trim(a.license_num_text)),
      coalesce(a.source_system_code),
      a.dw_last_update_date_time
    FROM
      {{params.param_hr_stage_dataset_name}}.employee_nurse_detail_wrk AS a
    WHERE (coalesce(a.employee_sid), coalesce(a.ncsbn_id), coalesce(trim(a.employee_34_login_code)), coalesce(a.employee_num), coalesce(cast(a.lawson_company_num as int64)), coalesce(trim(a.license_num_text))) NOT IN(
      SELECT AS STRUCT
          coalesce(c.employee_sid),
          coalesce(c.ncsbn_id),
          coalesce(trim(c.employee_34_login_code)),
          coalesce(c.employee_num),
          coalesce(cast(c.lawson_company_num as int64)), 
          coalesce(trim(c.license_num_text))
        FROM
          {{params.param_hr_base_views_dataset_name}}.employee_nurse_detail as c
    )
