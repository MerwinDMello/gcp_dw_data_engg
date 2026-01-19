DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_phone_num.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CN_PATIENT_PHONE_NUM		                ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   : EDWCR_staging.CN_PATIENT_PHONE_NUM_Stg		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_PHONE_NUM;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_PATIENT_PHONE_NUM_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_phone_num
WHERE cn_patient_phone_num.nav_patient_id NOT IN
    (SELECT cn_patient_phone_num_stg.nav_patient_id
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_phone_num_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_phone_num AS mt USING
  (SELECT DISTINCT cn_patient_phone_num_stg.nav_patient_id,
                   cn_patient_phone_num_stg.phone_num_type_code AS phone_num_type_code,
                   cn_patient_phone_num_stg.phone_num,
                   cn_patient_phone_num_stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_phone_num_stg
   WHERE cn_patient_phone_num_stg.nav_patient_id NOT IN
       (SELECT cn_patient_phone_num.nav_patient_id
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_phone_num) ) AS ms ON mt.nav_patient_id = ms.nav_patient_id
AND mt.phone_num_type_code = ms.phone_num_type_code
AND (upper(coalesce(mt.phone_num, '0')) = upper(coalesce(ms.phone_num, '0'))
     AND upper(coalesce(mt.phone_num, '1')) = upper(coalesce(ms.phone_num, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (nav_patient_id,
        phone_num_type_code,
        phone_num,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.nav_patient_id, ms.phone_num_type_code, ms.phone_num, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT nav_patient_id,
             phone_num_type_code
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_phone_num
      GROUP BY nav_patient_id,
               phone_num_type_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_phone_num');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_Patient_Phone_Num');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;