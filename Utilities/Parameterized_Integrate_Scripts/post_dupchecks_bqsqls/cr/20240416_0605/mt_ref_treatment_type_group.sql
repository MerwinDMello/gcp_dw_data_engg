DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/mt_ref_treatment_type_group.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.Ref_Treatment_Type_Group	             		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.Ref_Treatment_Type_Group_Stg		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                          		#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_MT_REF_TREATMENT_TYPE_GRP;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Ref_Treatment_Type_Group_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_treatment_type_group;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_treatment_type_group AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(ref_treatment_type_group_stg.treatment_type_group_code)) AS treatment_type_group_id,
                                     ref_treatment_type_group_stg.treatment_type_group_code AS treatment_type_group_code,
                                     substr(CASE upper(rtrim(ref_treatment_type_group_stg.treatment_type_group_code))
                                                WHEN 'D' THEN 'Surgical Diagnostic/Staging Procedure (Biopsy)'
                                                WHEN 'S' THEN 'Surgery'
                                                WHEN 'R' THEN 'Radiation'
                                                WHEN 'C' THEN 'Chemotherapy'
                                                WHEN 'H' THEN 'Hormone Therapy'
                                                WHEN 'I' THEN 'Immunotherapy'
                                                WHEN 'T' THEN 'Hematologic Transplant and Endocrine Procedures'
                                                WHEN 'O' THEN 'Other Treatment'
                                                WHEN 'P' THEN 'Palliative Care'
                                                ELSE 'NULL'
                                            END, 1, 50) AS treatment_type_group_desc,
                                     substr(CASE upper(rtrim(ref_treatment_type_group_stg.treatment_type_group_code))
                                                WHEN 'D' THEN 'BIOPSY'
                                                WHEN 'S' THEN 'SURGERY'
                                                WHEN 'R' THEN 'RAD ONC'
                                                WHEN 'C' THEN 'MED ONC'
                                                WHEN 'H' THEN 'MED ONC'
                                                WHEN 'I' THEN 'MED ONC'
                                                WHEN 'T' THEN 'MED ONC'
                                                WHEN 'O' THEN 'OTHER'
                                                WHEN 'P' THEN 'OTHER'
                                                ELSE 'NULL'
                                            END, 1, 255) AS nav_treatment_type_group_desc,
                                     ref_treatment_type_group_stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_treatment_type_group_stg) AS ms ON mt.treatment_type_group_id = ms.treatment_type_group_id
AND (upper(coalesce(mt.treatment_type_group_code, '0')) = upper(coalesce(ms.treatment_type_group_code, '0'))
     AND upper(coalesce(mt.treatment_type_group_code, '1')) = upper(coalesce(ms.treatment_type_group_code, '1')))
AND (upper(coalesce(mt.treatment_type_group_desc, '0')) = upper(coalesce(ms.treatment_type_group_desc, '0'))
     AND upper(coalesce(mt.treatment_type_group_desc, '1')) = upper(coalesce(ms.treatment_type_group_desc, '1')))
AND (upper(coalesce(mt.nav_treatment_type_group_desc, '0')) = upper(coalesce(ms.nav_treatment_type_group_desc, '0'))
     AND upper(coalesce(mt.nav_treatment_type_group_desc, '1')) = upper(coalesce(ms.nav_treatment_type_group_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (treatment_type_group_id,
        treatment_type_group_code,
        treatment_type_group_desc,
        nav_treatment_type_group_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.treatment_type_group_id, ms.treatment_type_group_code, ms.treatment_type_group_desc, ms.nav_treatment_type_group_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT treatment_type_group_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_treatment_type_group
      GROUP BY treatment_type_group_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_treatment_type_group');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','Ref_Treatment_Type_Group');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;