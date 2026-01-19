DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_user.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/**********************************************************************************************************************************
     Developer: Jason Chapman
          Date: 7/5/2015
          Name: Ref_CC_User.sql
		Mod1: Add purge statement at the end to clean-up deleted rows in source.  -  09/17/2015  jac
	Mod2:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
***********************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA283;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_user AS x USING
  (SELECT rccos.company_code,
          rccos.coid,
          usr.user_id,
          usr.customer_org_id,
          usr.first_name AS user_first_nm,
          usr.last_name AS user_last_nm,
          usr.usr_title AS user_title_nm,
          usr.email AS user_email_addr,
          CASE
              WHEN usr.expire_password_flag = 1 THEN 'Y'
              ELSE 'N'
          END AS user_expire_password_ind,
          usr.password_expire AS user_password_expire_dt,
          CASE
              WHEN usr.is_active = 1 THEN 'Y'
              ELSE 'N'
          END AS user_is_active_ind,
          usr.login_id AS user_login_id,
          usr.user_role AS user_role_nm,
          usr.default_summary_id AS user_default_summary_id,
          usr.date_user_created AS user_creation_dt,
          usr.created_by_user_id AS user_created_by_id,
          CASE
              WHEN usr.action_emails = 1 THEN 'Y'
              ELSE 'N'
          END AS user_action_emails_ind,
          usr.modified_by_user_id AS user_mod_by_user_id,
          usr.date_modified AS user_mod_date
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = usr.schema_id) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.user_id = z.user_id WHEN MATCHED THEN
UPDATE
SET customer_org_id = ROUND(z.customer_org_id, 0, 'ROUND_HALF_EVEN'),
    user_first_nm = z.user_first_nm,
    user_last_nm = z.user_last_nm,
    user_title_nm = z.user_title_nm,
    user_email_addr = z.user_email_addr,
    user_expire_password_ind = substr(z.user_expire_password_ind, 1, 1),
    user_password_expire_dt = z.user_password_expire_dt,
    user_is_active_ind = substr(z.user_is_active_ind, 1, 1),
    user_login_id = z.user_login_id,
    user_role_nm = z.user_role_nm,
    user_default_summary_id = ROUND(z.user_default_summary_id, 0, 'ROUND_HALF_EVEN'),
    user_creation_dt = z.user_creation_dt,
    user_created_by_id = ROUND(z.user_created_by_id, 0, 'ROUND_HALF_EVEN'),
    user_action_emails_ind = substr(z.user_action_emails_ind, 1, 1),
    user_mod_by_user_id = ROUND(z.user_mod_by_user_id, 0, 'ROUND_HALF_EVEN'),
    user_mod_date = z.user_mod_date,
    source_system_code = 'N',
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        user_id,
        customer_org_id,
        user_first_nm,
        user_last_nm,
        user_title_nm,
        user_email_addr,
        user_expire_password_ind,
        user_password_expire_dt,
        user_is_active_ind,
        user_login_id,
        user_role_nm,
        user_default_summary_id,
        user_creation_dt,
        user_created_by_id,
        user_action_emails_ind,
        user_mod_by_user_id,
        user_mod_date,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, z.coid, ROUND(z.user_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.customer_org_id, 0, 'ROUND_HALF_EVEN'), z.user_first_nm, z.user_last_nm, z.user_title_nm, z.user_email_addr, substr(z.user_expire_password_ind, 1, 1), z.user_password_expire_dt, substr(z.user_is_active_ind, 1, 1), z.user_login_id, z.user_role_nm, ROUND(z.user_default_summary_id, 0, 'ROUND_HALF_EVEN'), z.user_creation_dt, ROUND(z.user_created_by_id, 0, 'ROUND_HALF_EVEN'), substr(z.user_action_emails_ind, 1, 1), ROUND(z.user_mod_by_user_id, 0, 'ROUND_HALF_EVEN'), z.user_mod_date, 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             user_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_user
      GROUP BY company_code,
               coid,
               user_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_user');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_user
WHERE ref_cc_user.dw_last_update_date_time <>
    (SELECT max(ref_cc_user_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_user AS ref_cc_user_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','Ref_CC_User');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;