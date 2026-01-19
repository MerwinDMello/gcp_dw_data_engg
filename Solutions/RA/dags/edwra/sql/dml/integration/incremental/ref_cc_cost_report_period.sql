DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_cost_report_period.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/********************************************************
 Developer: Sean Wilson
      Name: Ref_CC_Cost_Report_Period_INSERT - BTEQ Script.
      Mod1: Creation of script on 7/22/2011. SW.
	  Mod2: Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	  Mod3: Removed _MV tables along with Insert/Select. Replaced with Merge. CB
********************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA150;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cost_report_period AS x USING
  (SELECT ros.company_code,
          CASE
              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
              ELSE og.client_id
          END AS coid,
          macp.id AS cost_period_id,
          og.short_name AS unit_num,
          macp.mon_accounting_period_id AS financial_period_id,
          macp.cost_report_year AS cost_report_year,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_cost_period AS macp
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON macp.schema_id = og.schema_id
   AND macp.cost_report_org_id = og.org_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_schema_master AS sm ON macp.schema_id = sm.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON macp.schema_id = ros.schema_id
   AND macp.cost_report_org_id = ros.org_id) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.cost_period_id = z.cost_period_id WHEN MATCHED THEN
UPDATE
SET unit_num = substr(z.unit_num, 1, 5),
    financial_period_id = z.financial_period_id,
    cost_report_year = substr(z.cost_report_year, 1, 4),
    dw_last_update_date_time = z.dw_last_update_date_time,
    source_system_code = substr(z.source_system_code, 1, 1) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        cost_period_id,
        unit_num,
        financial_period_id,
        cost_report_year,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, substr(z.coid, 1, 5), z.cost_period_id, substr(z.unit_num, 1, 5), z.financial_period_id, substr(z.cost_report_year, 1, 4), z.dw_last_update_date_time, substr(z.source_system_code, 1, 1));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cost_period_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cost_report_period
      GROUP BY company_code,
               coid,
               cost_period_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cost_report_period');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_Cost_Report_Period');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;