DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_fin_transaction.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/********************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Fin_Transaction - BTEQ Script.
      Mod1: Creation of script on 9/22/2011. SW.
      Mod2: Update to use MTM.Date_Created for new
            unique secondary index due to failures on
            1/30/2017 SW.
      Mod3: Changed LEFT JOIN to Apl_Transaction to
            INNER JOIN to avoid duplicate rows on 2/2/2017
            SW.
      Mod4: Changed code to only pick active transactions
            per Avinash on 2/7/2017 SW.
	  Mod5: Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	  Mod6: Removed _MV tables along with Insert/Select. Replaced with Merge. CB
*********************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA190;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_fin_transaction AS x USING
  (SELECT ros.company_code,
          CASE
              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
              ELSE og.client_id
          END AS coid,
          mtm.code AS transaction_code,
          apt.effective_begin_date AS eff_begin_date,
          og.short_name AS unit_num,
          mtm.trans_type AS transaction_type,
          mtm.description AS transaction_desc,
          apt.transaction_category_id AS transaction_category_id,
          mtm.id AS transaction_master_id,
          usr.login_id AS create_user_id,
          CAST(mtm.date_created AS DATETIME) AS create_date_time,
          usr2.login_id AS update_user_id,
          CAST(mtm.date_updated AS DATETIME) AS update_date_time,
          apt.effective_end_date AS inactive_date,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_transaction_master AS mtm
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON mtm.schema_id = og.schema_id
   AND mtm.org_id = og.org_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON mtm.schema_id = sm.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.apl_transaction AS apt ON mtm.schema_id = apt.schema_id
   AND mtm.id = apt.mon_transaction_master_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON mtm.user_id_created_by = usr.user_id
   AND mtm.schema_id = usr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr2 ON mtm.user_id_modified_by = usr2.user_id
   AND mtm.schema_id = usr2.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON mtm.schema_id = ros.schema_id
   AND mtm.org_id = ros.org_id
   WHERE apt.effective_end_date > current_date('US/Central') ) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND upper(rtrim(x.transaction_code)) = upper(rtrim(z.transaction_code))
AND x.eff_begin_date = z.eff_begin_date WHEN MATCHED THEN
UPDATE
SET unit_num = substr(z.unit_num, 1, 5),
    transaction_type = substr(z.transaction_type, 1, 1),
    transaction_desc = substr(z.transaction_desc, 1, 300),
    transaction_category_id = z.transaction_category_id,
    transaction_master_id = CAST(ROUND(z.transaction_master_id, 0, 'ROUND_HALF_EVEN') AS NUMERIC),
    inactive_date = z.inactive_date,
    create_user_id = substr(z.create_user_id, 1, 20),
    create_date_time = z.create_date_time,
    update_user_id = substr(z.update_user_id, 1, 20),
    update_date_time = z.update_date_time,
    dw_last_update_date_time = z.dw_last_update_date_time,
    source_system_code = substr(z.source_system_code, 1, 1) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        transaction_code,
        eff_begin_date,
        unit_num,
        transaction_type,
        transaction_desc,
        transaction_category_id,
        transaction_master_id,
        inactive_date,
        create_user_id,
        create_date_time,
        update_user_id,
        update_date_time,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, substr(z.coid, 1, 5), substr(z.transaction_code, 1, 8), z.eff_begin_date, substr(z.unit_num, 1, 5), substr(z.transaction_type, 1, 1), substr(z.transaction_desc, 1, 300), z.transaction_category_id, CAST(ROUND(z.transaction_master_id, 0, 'ROUND_HALF_EVEN') AS NUMERIC), z.inactive_date, substr(z.create_user_id, 1, 20), z.create_date_time, substr(z.update_user_id, 1, 20), z.update_date_time, z.dw_last_update_date_time, substr(z.source_system_code, 1, 1));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             transaction_code,
             eff_begin_date
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_fin_transaction
      GROUP BY company_code,
               coid,
               transaction_code,
               eff_begin_date
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_fin_transaction');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Fin_Transaction');
 IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;