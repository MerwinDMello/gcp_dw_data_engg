DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_fin_acct_period.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/********************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Fin_Acct_Period - BTEQ Script.
      Purpose: Builds the financial account period reference table used within the Business Objects AD-HOC Universe
 for reporting.
      Mod1: Creation of script on 9/8/2011. SW.
	  Mod2: Add Coid  02/19/2016  JC
	  Mod3: Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	  Mod4: Removed _MV tables along with Insert/Select. Replaced with Merge. CB
*********************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA214;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_fin_acct_period AS x USING
  (SELECT max(ros.company_code) AS company_code,
          max(ros.coid) AS coid,
          mac.id AS financial_period_id,
          max(mac.fiscal_year) AS fiscal_year,
          mac.accounting_period AS acctg_period_num,
          mac.start_date AS period_start_date,
          mac.end_date AS period_end_date,
          mac.close_date AS prelim_close_date,
          max(usr.login_id) AS update_login_userid,
          CAST(mac.modified_date AS DATETIME) AS update_date_time,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_period AS mac
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON mac.schema_id = sm.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON mac.modified_by = usr.user_id
   AND mac.schema_id = usr.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON mac.schema_id = ros.schema_id
   GROUP BY upper(ros.company_code),
            upper(ros.coid),
            3,
            upper(mac.fiscal_year),
            5,
            6,
            7,
            8,
            upper(usr.login_id),
            10) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.financial_period_id = z.financial_period_id WHEN MATCHED THEN
UPDATE
SET fiscal_year = substr(z.fiscal_year, 1, 4),
    acctg_period_num = CAST(z.acctg_period_num AS INT64),
    period_start_date = z.period_start_date,
    period_end_date = z.period_end_date,
    prelim_close_date = z.prelim_close_date,
    update_login_userid = substr(z.update_login_userid, 1, 20),
    update_date_time = z.update_date_time,
    dw_last_update_date_time = z.dw_last_update_date_time,
    source_system_code = substr(z.source_system_code, 1, 1) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        financial_period_id,
        fiscal_year,
        acctg_period_num,
        period_start_date,
        period_end_date,
        prelim_close_date,
        update_login_userid,
        update_date_time,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.financial_period_id, substr(z.fiscal_year, 1, 4), CAST(z.acctg_period_num AS INT64), z.period_start_date, z.period_end_date, z.prelim_close_date, substr(z.update_login_userid, 1, 20), z.update_date_time, z.dw_last_update_date_time, substr(z.source_system_code, 1, 1));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             financial_period_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_fin_acct_period
      GROUP BY company_code,
               coid,
               financial_period_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_fin_acct_period');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Fin_Acct_Period');
 IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;