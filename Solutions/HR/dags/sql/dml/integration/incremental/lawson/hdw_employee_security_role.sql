
BEGIN
DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
TRUNCATE TABLE
  {{ params.param_hr_core_dataset_name }}.employee_security_role;
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.employee_security_role (employee_34_login_code,
    system_code,
    security_role_code,
    access_role_code,
    span_code,
    create_date,
    creator_user_id_code,
    last_update_date,
    active_ind,
    source_system_code,
    dw_last_update_date_time)
SELECT
 trim(zsusrattbs.hca_uid_id) AS employee_34_login_code,
  trim(zsusrattbs.hca_syscode) AS system_code,
  trim(zsusrattbs.obj_type) AS security_role_code,
  trim(zsusrattbs.hca_obj_val1) AS access_role_code,
  trim(zsusrattbs.hca_obj_val2) AS span_code,
  zsusrattbs.create_date AS create_date,
  trim(zsusrattbs.create_user) AS creator_user_id_code,
  zsusrattbs.update_date AS last_update_date,
  trim(zsusrattbs.status_cd) AS active_ind,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.zsusrattbs
WHERE
  UPPER(zsusrattbs.obj_type) = 'APROL' QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_34_login_code, system_code, security_role_code, access_role_code, span_code ORDER BY employee_34_login_code, system_code, security_role_code, access_role_code, span_code DESC) = 1 ;
END
  ;