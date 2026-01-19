DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/mt_ref_ajcc_stage.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.Ref_AJCC_Stage	          	   		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.Ref_Lookup_Code_Stg			#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                          		#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_MT_REF_AJCC_STAGE;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Ref_Lookup_Code_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(ref_lookup_code_stg.lookup_code)) AS ajcc_stage_id,
                                     ref_lookup_code_stg.lookup_code AS ajcc_stage_code,
                                     ref_lookup_code_stg.lookup_sub_code AS ajcc_stage_sub_code,
                                     substr(ref_lookup_code_stg.lookup_desc, 1, 300) AS ajcc_stage_desc,
                                     ref_lookup_code_stg.group_id,
                                     ref_lookup_code_stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.ref_lookup_code_stg
   WHERE ref_lookup_code_stg.lookup_id = 4040
     AND ref_lookup_code_stg.lookup_desc IS NOT NULL
     AND upper(trim(ref_lookup_code_stg.lookup_desc)) <> '' ) AS ms ON mt.ajcc_stage_id = ms.ajcc_stage_id
AND (upper(coalesce(mt.ajcc_stage_code, '0')) = upper(coalesce(ms.ajcc_stage_code, '0'))
     AND upper(coalesce(mt.ajcc_stage_code, '1')) = upper(coalesce(ms.ajcc_stage_code, '1')))
AND (upper(coalesce(mt.ajcc_stage_sub_code, '0')) = upper(coalesce(ms.ajcc_stage_sub_code, '0'))
     AND upper(coalesce(mt.ajcc_stage_sub_code, '1')) = upper(coalesce(ms.ajcc_stage_sub_code, '1')))
AND (upper(coalesce(mt.ajcc_stage_desc, '0')) = upper(coalesce(ms.ajcc_stage_desc, '0'))
     AND upper(coalesce(mt.ajcc_stage_desc, '1')) = upper(coalesce(ms.ajcc_stage_desc, '1')))
AND (coalesce(mt.ajcc_stage_group_id, 0) = coalesce(ms.group_id, 0)
     AND coalesce(mt.ajcc_stage_group_id, 1) = coalesce(ms.group_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (ajcc_stage_id,
        ajcc_stage_code,
        ajcc_stage_sub_code,
        ajcc_stage_desc,
        ajcc_stage_group_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.ajcc_stage_id, ms.ajcc_stage_code, ms.ajcc_stage_sub_code, ms.ajcc_stage_desc, ms.group_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT ajcc_stage_id
      FROM {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage
      GROUP BY ajcc_stage_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','Ref_AJCC_Stage');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF