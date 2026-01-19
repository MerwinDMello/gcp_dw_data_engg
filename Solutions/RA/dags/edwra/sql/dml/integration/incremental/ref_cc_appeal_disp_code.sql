DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_appeal_disp_code.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/*****************************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Appeal_Disp_Code_INSERT - BTEQ Script.
	  Purpose: Builds the appeal disp code reference table used within the Business Objects AD-HOC Universe
 for reporting.
      Mod1: Creation of script on 7/22/2011. SW.
      Mod2: Added rename logic on 7/27/2011. SW.
      Mod3: Changed script for new DDL on 9/8/2011. SW.
      Mod4: Changed active_ind logic per defect on 10/3/2011. SW.
	  Mod5: Add Coid  2/19/2016  JC
	  Mod6: Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	  Mod7: Removed _MV tables along with Insert/Select. Replaced with Merge. CB
*******************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA213;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_appeal_disp_code AS x USING
  (SELECT TRIM(max(ros.company_code)) AS company_code,
          TRIM(max(ros.coid)) AS coid,
          ad.id AS apl_disp_code_id,
          max(ad.code) AS apl_disp_code,
          TRIM(max(ad.description)) AS apl_disp_desc,
          ad.disposition_status_id AS apl_disp_status_id,
          max(usr.login_id) AS create_login_userid,
          CAST(ad.date_created AS DATETIME) AS create_date_time,
          max(usr2.login_id) AS update_login_userid,
          CAST(ad.date_modified AS DATETIME) AS update_date_time,
          ad.effective_end_date AS inactive_date,
          max(CASE
                  WHEN ad.is_deleted = 1 THEN 'N'
                  ELSE 'Y'
              END) AS active_ind,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.apl_disposition AS ad
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON ad.user_id_created_by = usr.user_id
   AND ad.schema_id = usr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON ad.schema_id = sm.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr2 ON ad.user_id_modified_by = usr2.user_id
   AND ad.schema_id = usr2.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON ad.schema_id = ros.schema_id
   GROUP BY upper(ros.company_code),
            upper(ros.coid),
            3,
            upper(ad.code),
            upper(ad.description),
            6,
            upper(usr.login_id),
            8,
            upper(usr2.login_id),
            10,
            11,
            upper(CASE
                      WHEN ad.is_deleted = 1 THEN 'N'
                      ELSE 'Y'
                  END)) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.apl_disp_code_id = z.apl_disp_code_id WHEN MATCHED THEN
UPDATE
SET apl_disp_code = z.apl_disp_code,
    apl_disp_desc = z.apl_disp_desc,
    apl_disp_status_id = z.apl_disp_status_id,
    create_login_userid = substr(z.create_login_userid, 1, 20),
    create_date_time = z.create_date_time,
    update_login_userid = substr(z.update_login_userid, 1, 20),
    update_date_time = z.update_date_time,
    inactive_date = z.inactive_date,
    active_ind = substr(z.active_ind, 1, 1),
    dw_last_update_date_time = z.dw_last_update_date_time,
    source_system_code = substr(z.source_system_code, 1, 1) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        apl_disp_code_id,
        apl_disp_code,
        apl_disp_desc,
        apl_disp_status_id,
        create_login_userid,
        create_date_time,
        update_login_userid,
        update_date_time,
        inactive_date,
        active_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.apl_disp_code_id, z.apl_disp_code, z.apl_disp_desc, z.apl_disp_status_id, substr(z.create_login_userid, 1, 20), z.create_date_time, substr(z.update_login_userid, 1, 20), z.update_date_time, z.inactive_date, substr(z.active_ind, 1, 1), z.dw_last_update_date_time, substr(z.source_system_code, 1, 1));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             apl_disp_code_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_appeal_disp_code
      GROUP BY company_code,
               coid,
               apl_disp_code_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_appeal_disp_code');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Appeal_Disp_Code');
 IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;