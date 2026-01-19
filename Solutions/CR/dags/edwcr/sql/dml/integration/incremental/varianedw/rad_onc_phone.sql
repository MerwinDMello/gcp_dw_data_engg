DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/rad_onc_phone.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Rad_Onc_Phone                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		  		   : EDWCR_STAGING.Stg_DimHospitalDepartment,EDWCR_Staging.stg_DIMPATIENT ##
-- ##							,EDWCR_STAGING.STG_DIMDOCTOR  		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_REF_RAD_ONC_PHONE;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Rad_Onc_Phone_wrk');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_stage_dataset_name }}.rad_onc_phone_wrk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO {{ params.param_cr_stage_dataset_name }}.rad_onc_phone_wrk (phone_num_text)
SELECT DISTINCT substr(trim(src.phone_num_text), 1, 200)
FROM
  (SELECT trim(stg_dimpatient.patienthomephone) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
   UNION ALL SELECT trim(stg_dimpatient.patientworkphone) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
   UNION ALL SELECT trim(stg_dimpatient.patientmobilephone) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
   UNION ALL SELECT trim(stg_dimpatient.patientpagernumber) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
   UNION ALL SELECT trim(stg_dimpatient.patemergencycontacthomephone) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
   UNION ALL SELECT trim(stg_dimpatient.patemergencycontactmobilephone) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
   UNION ALL SELECT trim(stg_dimpatient.patemergencycontactworkphone) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
   UNION ALL SELECT trim(stg_dimpatient.transportationphone) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
   UNION ALL SELECT trim(stg_dimdoctor.doctorprimaryphonenumber) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimdoctor
   UNION ALL SELECT trim(stg_dimdoctor.doctorsecondaryphonenumber) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimdoctor
   UNION ALL SELECT trim(stg_dimdoctor.doctorpagernumber) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimdoctor
   UNION ALL SELECT trim(stg_dimdoctor.doctorfaxnumber) AS phone_num_text
   FROM {{ params.param_cr_stage_dataset_name }}.stg_dimdoctor) AS src
WHERE src.phone_num_text IS NOT NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- Deleting Data
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_phone;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.rad_onc_phone AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(trim(ds.phone_num_text))) AS phone_num_sk,
                                     substr(trim(substr(ds.phone_num_text, 1, 100)), 1, 100) AS phone_num_text,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.rad_onc_phone_wrk AS ds) AS ms ON mt.phone_num_sk = ms.phone_num_sk
AND (upper(coalesce(mt.phone_num_text, '0')) = upper(coalesce(ms.phone_num_text, '0'))
     AND upper(coalesce(mt.phone_num_text, '1')) = upper(coalesce(ms.phone_num_text, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (phone_num_sk,
        phone_num_text,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.phone_num_sk, ms.phone_num_text, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT phone_num_sk
      FROM {{ params.param_cr_core_dataset_name }}.rad_onc_phone
      GROUP BY phone_num_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.rad_onc_phone');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Rad_Onc_Phone');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF