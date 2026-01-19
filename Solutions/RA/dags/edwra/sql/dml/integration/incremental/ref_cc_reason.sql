DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_reason.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Reason - BTEQ Script.
      Mod1: Creation of script on 7/26/2011. SW.
      Mod2: Added rename logic on 7/27/2011. SW.
      Mod3: Changed script for new DDL on 9/8/2011. SW.
	Mod4:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod5: Removed _MV tables along with Insert/Select. Replaced with Merge
***************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA153;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_reason AS x USING
  (SELECT max(ros.company_code) AS company_code,
          mr.id AS reason_id,
          max(mr.description) AS reason_desc,
          max(mr.name) AS reason_name,
          max(mr.external_reason_code) AS pa_reason_code,
          max(CASE
                  WHEN mr.is_denial = 1 THEN 'N'
                  ELSE 'Y'
              END) AS active_ind,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_reason AS mr
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON mr.schema_id = sm.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON sm.schema_id = ros.schema_id
   GROUP BY upper(ros.company_code),
            2,
            upper(mr.description),
            upper(mr.name),
            upper(mr.external_reason_code),
            upper(CASE
                      WHEN mr.is_denial = 1 THEN 'N'
                      ELSE 'Y'
                  END)) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND x.reason_id = z.reason_id WHEN MATCHED THEN
UPDATE
SET reason_desc = z.reason_desc,
    reason_name = z.reason_name,
    pa_reason_code = substr(z.pa_reason_code, 1, 3),
    active_ind = substr(z.active_ind, 1, 1),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        reason_id,
        reason_desc,
        reason_name,
        pa_reason_code,
        active_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.reason_id, z.reason_desc, z.reason_name, substr(z.pa_reason_code, 1, 3), substr(z.active_ind, 1, 1), datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             reason_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_reason
      GROUP BY company_code,
               reason_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_reason');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Reason');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;