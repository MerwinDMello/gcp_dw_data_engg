DECLARE DUP_COUNT INT64;

DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Root_Cause_INSERT - BTEQ Script.
	  Purpose: Builds the root cause reference table used within the Business Objects AD-HOC Universe for reporting.
      Mod1: Creation of script on 7/26/2011. SW.
      Mod2: Added rename logic on 7/27/2011. SW.
      Mod3: Changed script for new DDL on 9/8/2011. SW.
      Mod4: Switched logic for Active_Ind due to defect on 10/4/2011. SW.
      Mod5: Removed joins to users and made it ready for CHP 04/09/2014 FY.
	  Mod6: Add Coid  02/19/2016  JC
	Mod7:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod8: Removed _MV tables along with Insert/Select. Replaced with Merge
****************************************************************************/ 
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_root_cause AS x USING
  (SELECT max(ros.company_code) AS company_code,
          max(ros.coid) AS coid,
          arc.id AS root_cause_id,
          max(arc.code) AS root_cause_code,
          trim(max(arc.description)) AS root_cause_desc,
          concat(max(arc.user_id_created_by),'.') AS create_login_userid,
          max(arc.date_created) AS create_date_time,
          concat(max(arc.user_id_modified_by),'.') AS update_login_userid,
          max(arc.date_modified) AS update_date_time,
          max(arc.effective_end_date) AS inactive_date,
          'Y' AS active_ind,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.apl_root_cause AS arc
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON arc.schema_id = ros.schema_id
   GROUP BY upper(ros.company_code),
            upper(ros.coid),
            3,
            upper(arc.code),
            upper(arc.description)) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.root_cause_id = z.root_cause_id WHEN MATCHED THEN
UPDATE
SET root_cause_code = z.root_cause_code,
    root_cause_desc = z.root_cause_desc,
    create_login_userid = substr(CAST(z.create_login_userid AS STRING), 1, 20),
    create_date_time = CAST(z.create_date_time AS DATETIME),
    update_login_userid = substr(CAST(z.update_login_userid AS STRING), 1, 20),
    update_date_time = CAST(z.update_date_time AS DATETIME),
    inactive_date = z.inactive_date,
    active_ind = substr(z.active_ind, 1, 1),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        root_cause_id,
        root_cause_code,
        root_cause_desc,
        create_login_userid,
        create_date_time,
        update_login_userid,
        update_date_time,
        inactive_date,
        active_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.root_cause_id, z.root_cause_code, z.root_cause_desc, substr(CAST(z.create_login_userid AS STRING), 1, 20), CAST(z.create_date_time AS DATETIME), substr(CAST(z.update_login_userid AS STRING), 1, 20), CAST(z.update_date_time AS DATETIME), z.inactive_date, substr(z.active_ind, 1, 1), datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             root_cause_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_root_cause
      GROUP BY company_code,
               coid,
               root_cause_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_root_cause');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;