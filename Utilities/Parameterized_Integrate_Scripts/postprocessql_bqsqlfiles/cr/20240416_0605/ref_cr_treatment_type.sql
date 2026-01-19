DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_cr_treatment_type.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.Ref_CR_Treatment_Type	             		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.Ref_CR_Treatment_Type_Stg		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                          		#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_REF_TREATMENT_TYPE_GRP;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Ref_CR_Treatment_Type_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_cr_treatment_type;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_cr_treatment_type AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(ref_cr_treatment_type_stg.treatment_type_code)) AS treatment_type_group_id,
                                     ref_cr_treatment_type_stg.treatment_type_code AS treatment_type_code,
                                     ref_cr_treatment_type_stg.treatment_type_desc,
                                     ref_cr_treatment_type_stg.treatment_group_id,
                                     ref_cr_treatment_type_stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_cr_treatment_type_stg) AS ms ON mt.treatment_type_id = ms.treatment_type_group_id
AND (upper(coalesce(mt.treatment_type_code, '0')) = upper(coalesce(ms.treatment_type_code, '0'))
     AND upper(coalesce(mt.treatment_type_code, '1')) = upper(coalesce(ms.treatment_type_code, '1')))
AND (upper(coalesce(mt.treatment_type_desc, '0')) = upper(coalesce(ms.treatment_type_desc, '0'))
     AND upper(coalesce(mt.treatment_type_desc, '1')) = upper(coalesce(ms.treatment_type_desc, '1')))
AND (coalesce(mt.treatment_group_id, 0) = coalesce(ms.treatment_group_id, 0)
     AND coalesce(mt.treatment_group_id, 1) = coalesce(ms.treatment_group_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (treatment_type_id,
        treatment_type_code,
        treatment_type_desc,
        treatment_group_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.treatment_type_group_id, ms.treatment_type_code, ms.treatment_type_desc, ms.treatment_group_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT treatment_type_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_cr_treatment_type
      GROUP BY treatment_type_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_cr_treatment_type');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','Ref_CR_Treatment_Type');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF