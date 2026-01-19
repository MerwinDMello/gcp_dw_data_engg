DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_cers_category.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/**********************************************************************************************************************************
     Developer: Jason Chapman
          Date: 7/5/2015
          Name: Ref_CC_CERS_Category.sql
      Mod1: Add purge statement at the end to clean-up deleted rows in source.  -  09/17/2015  jac
	  Mod2: Updated merge logic for consistency with other tables. CB
***********************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA280;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_category AS x USING
  (SELECT rccos.company_code,
          rccos.coid,
          crsc.id AS cers_category_id,
          crsc.name AS cers_category_name,
          CASE
              WHEN crsc.is_deleted = 1 THEN 'Y'
              ELSE 'N'
          END AS cers_category_deleted_ind,
          crsc.user_id_created_by AS cers_category_create_user_id,
          crsc.date_created AS cers_category_create_date,
          crsc.user_id_modified_by AS cers_category_update_user_id,
          crsc.date_modified AS cers_category_update_date,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.ce_rs_category AS crsc
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = crsc.schema_id) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.cers_category_id = z.cers_category_id WHEN MATCHED THEN
UPDATE
SET cers_category_name = z.cers_category_name,
    cers_category_deleted_ind = substr(z.cers_category_deleted_ind, 1, 1),
    cers_category_create_user_id = ROUND(z.cers_category_create_user_id, 0, 'ROUND_HALF_EVEN'),
    cers_category_create_date = DATE(z.cers_category_create_date),
    cers_category_update_user_id = ROUND(z.cers_category_update_user_id, 0, 'ROUND_HALF_EVEN'),
    cers_category_update_date = DATE(z.cers_category_update_date),
    source_system_code = substr(z.source_system_code, 1, 1),
    dw_last_update_date_time = z.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        cers_category_id,
        cers_category_name,
        cers_category_deleted_ind,
        cers_category_create_user_id,
        cers_category_create_date,
        cers_category_update_user_id,
        cers_category_update_date,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, z.coid, ROUND(z.cers_category_id, 0, 'ROUND_HALF_EVEN'), z.cers_category_name, substr(z.cers_category_deleted_ind, 1, 1), ROUND(z.cers_category_create_user_id, 0, 'ROUND_HALF_EVEN'), DATE(z.cers_category_create_date), ROUND(z.cers_category_update_user_id, 0, 'ROUND_HALF_EVEN'), DATE(z.cers_category_update_date), substr(z.source_system_code, 1, 1), z.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cers_category_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_category
      GROUP BY company_code,
               coid,
               cers_category_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_category');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_category
WHERE ref_cc_cers_category.dw_last_update_date_time <>
    (SELECT max(ref_cc_cers_category_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_category AS ref_cc_cers_category_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','Ref_CC_CERS_Category');
 IF FALSE THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;