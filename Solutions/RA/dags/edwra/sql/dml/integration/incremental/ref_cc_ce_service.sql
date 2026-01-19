DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_ce_service.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************************************
     Developer: Sean Wilson
          Date: 10/14/2014
          Name: Ref_CC_CE_Service.sql
          Mod1: Corrected column names on 11/11/2014 SW.
          Mod2: Removed 2 table logic on 11/13/2014 SW.
		  Mod3: Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
		  Mod4: Removed _MV tables along with Insert/Select. Replaced with Merge. CB
*************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA256;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_ce_service AS x USING
  (SELECT rccos.company_code,
          rccos.coid,
          ces.id AS ce_service_id,
          TRIM(ces.name) AS ce_service_name,
          CASE
              WHEN ces.is_doc_service = 1 THEN 'Y'
              ELSE 'N'
          END AS doc_service_ind,
          CASE
              WHEN ces.is_pass_through = 1 THEN 'Y'
              ELSE 'N'
          END AS pass_through_ind,
          CASE
              WHEN ces.is_pass_through_active = 1 THEN 'Y'
              ELSE 'N'
          END AS pass_through_active_ind,
          ces.date_created AS ce_service_create_date_time,
          ces.date_updated AS ce_service_update_date_time,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.ce_service AS ces
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = ces.schema_id) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.ce_service_id = z.ce_service_id WHEN MATCHED THEN
UPDATE
SET ce_service_name = z.ce_service_name,
    doc_service_ind = substr(z.doc_service_ind, 1, 1),
    pass_through_ind = substr(z.pass_through_ind, 1, 1),
    pass_through_active_ind = substr(z.pass_through_active_ind, 1, 1),
    ce_service_create_date_time = CAST(z.ce_service_create_date_time AS DATETIME),
    ce_service_update_date_time = CAST(z.ce_service_update_date_time AS DATETIME),
    source_system_code = substr(z.source_system_code, 1, 1),
    dw_last_update_date_time = z.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        ce_service_id,
        ce_service_name,
        doc_service_ind,
        pass_through_ind,
        pass_through_active_ind,
        ce_service_create_date_time,
        ce_service_update_date_time,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, z.coid, z.ce_service_id, z.ce_service_name, substr(z.doc_service_ind, 1, 1), substr(z.pass_through_ind, 1, 1), substr(z.pass_through_active_ind, 1, 1), CAST(z.ce_service_create_date_time AS DATETIME), CAST(z.ce_service_update_date_time AS DATETIME), substr(z.source_system_code, 1, 1), z.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             ce_service_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_ce_service
      GROUP BY company_code,
               coid,
               ce_service_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_ce_service');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_CE_Service');
 IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;