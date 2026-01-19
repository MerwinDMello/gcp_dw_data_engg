DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_preset_value_group.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Preset_Value_Grp_INSERT - BTEQ Script.
      Purpose: Builds the preset value group reference table used within the Business Objects AD-HOC Universe
for reporting.
      Mod1: Creation of script on 9/8/2011. SW.
	  Mod2: Add Coid  02/19/2016  JC
	Mod3:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod4: Removed _MV tables along with Insert/Select. Replaced with Merge
****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA188;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_preset_value_group AS x USING
  (SELECT max(ros.company_code) AS company_code,
          max(ros.coid) AS coid,
          pv.id AS preset_value_id,
          max(pv.group_id) AS preset_value_group_type,
          max(pv.display_text) AS preset_value_display_text,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pv
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON pv.schema_id = sm.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON pv.schema_id = ros.schema_id
   GROUP BY upper(ros.company_code),
            upper(ros.coid),
            3,
            upper(pv.group_id),
            upper(pv.display_text)) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.preset_value_id = z.preset_value_id WHEN MATCHED THEN
UPDATE
SET preset_value_group_type = z.preset_value_group_type,
    preset_value_display_text = z.preset_value_display_text,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        preset_value_id,
        preset_value_group_type,
        preset_value_display_text,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.preset_value_id, z.preset_value_group_type, z.preset_value_display_text, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             preset_value_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_preset_value_group
      GROUP BY company_code,
               coid,
               preset_value_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_preset_value_group');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Preset_Value_Group');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;