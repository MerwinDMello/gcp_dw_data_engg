DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_activity_type.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/**********************************************************************
 Developer: Holly Ray
      Date: 6/21/2011
      Name: Ref_CC_Activity_Type_Build.sql
	  Purpose: Builds the activity type reference table used within the Business Objects AD-HOC Universe
for reporting.
      Mod1: Initial creation of BTEQ script on 6/21/2011.
      Mod2: Adjusted script for missing columns on 6/22/2011 - SW.
      Mod3: Updated to include Schema_Id and new load strategy - HR
      Mod4: Adapted new strucutre for INSERT clause on 8/8/2011 - SW.
      Mod5: Changed script for new DDL on 9/8/2011. SW.
	  Mod6: Add Coid  2/19/2016  JC
	  Mod7: Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	  Mod8: Removed _MV tables along with Insert/Select. Replaced with Merge. CB
***********************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA204;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_activity_type AS x USING
  (SELECT max(ros.company_code) AS company_code,
          max(ros.coid) AS coid,
          mat.id AS activity_type_id,
          max(mat.description) AS activity_type_desc,
          max(mat.name) AS activity_type_name,
          mat.default_followup_days AS default_days_num,
          mat.pay_days AS pay_days_num,
          max(CASE
                  WHEN mat.default_completion = 1 THEN 'Y'
                  ELSE 'N'
              END) AS auto_complete_ind,
          max(CASE
                  WHEN mat.incl_on_revenue_recovery_rprt = 1 THEN 'Y'
                  ELSE 'N'
              END) AS incl_rev_recov_rpt_ind,
          max(CASE
                  WHEN mat.include_in_notes = 1 THEN 'Y'
                  ELSE 'N'
              END) AS incl_notes_ind,
          max(mat.is_create_appeal_or_collection) AS create_appeal_collect_ind,
          mat.activity_type_id_followup AS follow_up_activity_type_id,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_activity_type AS mat
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON mat.schema_id = sm.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON mat.schema_id = ros.schema_id
   GROUP BY upper(ros.company_code),
            upper(ros.coid),
            3,
            upper(mat.description),
            upper(mat.name),
            6,
            7,
            upper(CASE
                      WHEN mat.default_completion = 1 THEN 'Y'
                      ELSE 'N'
                  END),
            upper(CASE
                      WHEN mat.incl_on_revenue_recovery_rprt = 1 THEN 'Y'
                      ELSE 'N'
                  END),
            upper(CASE
                      WHEN mat.include_in_notes = 1 THEN 'Y'
                      ELSE 'N'
                  END),
            upper(mat.is_create_appeal_or_collection),
            12) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.activity_type_id = z.activity_type_id WHEN MATCHED THEN
UPDATE
SET activity_type_desc = z.activity_type_desc,
    activity_type_name = z.activity_type_name,
    default_days_num = z.default_days_num,
    pay_days_num = CAST(z.pay_days_num AS INT64),
    auto_complete_ind = substr(z.auto_complete_ind, 1, 1),
    incl_rev_recov_rpt_ind = substr(z.incl_rev_recov_rpt_ind, 1, 1),
    incl_notes_ind = substr(z.incl_notes_ind, 1, 1),
    create_appeal_collect_ind = z.create_appeal_collect_ind,
    follow_up_activity_type_id = z.follow_up_activity_type_id,
    dw_last_update_date_time = z.dw_last_update_date_time,
    source_system_code = substr(z.source_system_code, 1, 1) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        activity_type_id,
        activity_type_desc,
        activity_type_name,
        default_days_num,
        pay_days_num,
        auto_complete_ind,
        incl_rev_recov_rpt_ind,
        incl_notes_ind,
        create_appeal_collect_ind,
        follow_up_activity_type_id,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.activity_type_id, z.activity_type_desc, z.activity_type_name, z.default_days_num, CAST(z.pay_days_num AS INT64), substr(z.auto_complete_ind, 1, 1), substr(z.incl_rev_recov_rpt_ind, 1, 1), substr(z.incl_notes_ind, 1, 1), z.create_appeal_collect_ind, z.follow_up_activity_type_id, z.dw_last_update_date_time, substr(z.source_system_code, 1, 1));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             activity_type_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_activity_type
      GROUP BY company_code,
               coid,
               activity_type_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_activity_type');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Activity_Type');
 IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;