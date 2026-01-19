DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_account_payer_stts.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/********************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Account_Payer_Stts - BTEQ Script.
      Mod1: Creation of script on 7/22/2011. SW.
      Mod2: Added rename logic on 7/27/2011. SW.
      Mod3: Changed script for new DDL on 9/7/2011. SW.
      Mod4: Updated Incl_New_Acct_Ind, Prvnt_Acct_Ovrd_Ind, Acct_Level_Assn_Ind on 4/7/2014 FY.
	  Mod5: Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	  Mod6: Removed _MV tables along with Insert/Select. Replaced with Merge. 4/28/2020 CB
********************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA152;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_account_payer_stts AS x USING
  (SELECT max(ros.company_code) AS company_code,
          ms.id AS status_id,
          ms.mon_stat_category_id AS status_category_id,
          max(ms.name) AS status_name,
          max(ms.description) AS status_desc,
          ms.status_phase AS status_phase_id,
          ms.coll_probability AS probability_pct,
          max(CASE
                  WHEN ms.is_priority_status = 1 THEN 'N'
                  ELSE 'N'
              END) AS incl_new_acct_ind,
          max(CASE
                  WHEN ms.is_available_to_assign = 1 THEN 'N'
                  ELSE 'N'
              END) AS pvnt_acct_ovrd_ind,
          max(CASE
                  WHEN ms.can_manually_assign_to_accts = 1 THEN 'N'
                  ELSE 'N'
              END) AS acct_level_assn_ind,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_status AS ms
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON ms.schema_id = sm.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON sm.schema_id = ros.schema_id
   GROUP BY upper(ros.company_code),
            2,
            3,
            upper(ms.name),
            upper(ms.description),
            6,
            7,
            upper(CASE
                      WHEN ms.is_priority_status = 1 THEN 'N'
                      ELSE 'N'
                  END),
            upper(CASE
                      WHEN ms.is_available_to_assign = 1 THEN 'N'
                      ELSE 'N'
                  END),
            upper(CASE
                      WHEN ms.can_manually_assign_to_accts = 1 THEN 'N'
                      ELSE 'N'
                  END)) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND x.status_id = z.status_id WHEN MATCHED THEN
UPDATE
SET status_category_id = z.status_category_id,
    status_name = z.status_name,
    status_desc = z.status_desc,
    status_phase_id = z.status_phase_id,
    probability_pct = ROUND(z.probability_pct, 4, 'ROUND_HALF_EVEN'),
    incl_new_acct_ind = substr(z.incl_new_acct_ind, 1, 1),
    pvnt_acct_ovrd_ind = substr(z.pvnt_acct_ovrd_ind, 1, 1),
    acct_level_assn_ind = substr(z.acct_level_assn_ind, 1, 1),
    dw_last_update_date_time = z.dw_last_update_date_time,
    source_system_code = substr(z.source_system_code, 1, 1) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        status_id,
        status_category_id,
        status_name,
        status_desc,
        status_phase_id,
        probability_pct,
        incl_new_acct_ind,
        pvnt_acct_ovrd_ind,
        acct_level_assn_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.status_id, z.status_category_id, z.status_name, z.status_desc, z.status_phase_id, ROUND(z.probability_pct, 4, 'ROUND_HALF_EVEN'), substr(z.incl_new_acct_ind, 1, 1), substr(z.pvnt_acct_ovrd_ind, 1, 1), substr(z.acct_level_assn_ind, 1, 1), z.dw_last_update_date_time, substr(z.source_system_code, 1, 1));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             status_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_account_payer_stts
      GROUP BY company_code,
               status_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_account_payer_stts');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Account_Payer_Stts');
 IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;