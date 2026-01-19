DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_appeal_var_adj.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Appeal_Var_Adj - BTEQ Script.
      Mod1: Creation of script on 9/22/2011. SW.
	  Mod2: Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	  Mod3: Removed _MV tables along with Insert/Select. Replaced with Merge. CB
***************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA228;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_appeal_var_adj AS x USING
  (SELECT max(os.company_code) AS company_code,
          max(CASE
                  WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                  ELSE og.client_id
              END) AS coid,
          ava.id AS appeal_var_adj_code_id,
          max(ava.code) AS appeal_var_adj_code,
          max(ava.description) AS appeal_var_adj_code_desc,
          ava.effective_end_date AS appeal_var_adj_eff_end_date,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.apl_variance_adj AS ava
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ava.customer_id = og.customer_org_id
   AND ava.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON ava.schema_id = sm.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS os ON og.org_id = os.org_id
   GROUP BY upper(os.company_code),
            upper(CASE
                      WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                      ELSE og.client_id
                  END),
            3,
            upper(ava.code),
            upper(ava.description),
            6) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.appeal_var_adj_code_id = z.appeal_var_adj_code_id WHEN MATCHED THEN
UPDATE
SET appeal_var_adj_code = substr(z.appeal_var_adj_code, 1, 10),
    appeal_var_adj_code_desc = z.appeal_var_adj_code_desc,
    appeal_var_adj_eff_end_date = z.appeal_var_adj_eff_end_date,
    dw_last_update_date_time = z.dw_last_update_date_time,
    source_system_code = substr(z.source_system_code, 1, 1) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        appeal_var_adj_code_id,
        appeal_var_adj_code,
        appeal_var_adj_code_desc,
        appeal_var_adj_eff_end_date,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, substr(z.coid, 1, 5), z.appeal_var_adj_code_id, substr(z.appeal_var_adj_code, 1, 10), z.appeal_var_adj_code_desc, z.appeal_var_adj_eff_end_date, z.dw_last_update_date_time, substr(z.source_system_code, 1, 1));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             appeal_var_adj_code_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_appeal_var_adj
      GROUP BY company_code,
               coid,
               appeal_var_adj_code_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_appeal_var_adj');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Appeal_Var_Adj');
 IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;