DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_ref_tumor_type.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: REF_TUMOR_TYPE          				#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: 				#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	:
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_REF_TUMOR_TYPE;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Ref_Tumor_Type_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which delete logic to purge data only for Source_System_Cd='N' for Ref_Tumor_Type */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_cr_core_dataset_name }}.ref_tumor_type
WHERE upper(rtrim(ref_tumor_type.source_system_code)) = 'N';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.ref_tumor_type AS mt USING
  (SELECT DISTINCT ref_tumor_type_stg.tumor_type_id,
                   ref_tumor_type_stg.tumor_type_desc,
                   ref_tumor_type_stg.navigation_tumor_code_id,
                   ref_tumor_type_stg.tumor_type_group_name,
                   ref_tumor_type_stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.ref_tumor_type_stg) AS ms ON mt.tumor_type_id = ms.tumor_type_id
AND (upper(coalesce(mt.tumor_type_desc, '0')) = upper(coalesce(ms.tumor_type_desc, '0'))
     AND upper(coalesce(mt.tumor_type_desc, '1')) = upper(coalesce(ms.tumor_type_desc, '1')))
AND (coalesce(mt.navigation_tumor_code_id, 0) = coalesce(ms.navigation_tumor_code_id, 0)
     AND coalesce(mt.navigation_tumor_code_id, 1) = coalesce(ms.navigation_tumor_code_id, 1))
AND (upper(coalesce(mt.tumor_type_group_name, '0')) = upper(coalesce(ms.tumor_type_group_name, '0'))
     AND upper(coalesce(mt.tumor_type_group_name, '1')) = upper(coalesce(ms.tumor_type_group_name, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (tumor_type_id,
        tumor_type_desc,
        navigation_tumor_code_id,
        tumor_type_group_name,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.tumor_type_id, ms.tumor_type_desc, ms.navigation_tumor_code_id, ms.tumor_type_group_name, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT tumor_type_id
      FROM {{ params.param_cr_core_dataset_name }}.ref_tumor_type
      GROUP BY tumor_type_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_tumor_type');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','Ref_Tumor_Type');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF