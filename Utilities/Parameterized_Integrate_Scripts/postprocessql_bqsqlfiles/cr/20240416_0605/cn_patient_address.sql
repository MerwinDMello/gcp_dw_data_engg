DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_address.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CN_Patient_Address	                        ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   : EDWCR_STAGING.CN_PATIENT_ADDRESS_Stg		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_Patient_Address;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_PATIENT_ADDRESS_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_address
WHERE cn_patient_address.nav_patient_id NOT IN
    (SELECT cn_patient_address_stg.nav_patient_id
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_address_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_address AS mt USING
  (SELECT DISTINCT stg.nav_patient_id,
                   ht.housing_type_id,
                   stg.address_line_1_text,
                   stg.address_line_2_text,
                   stg.city_name,
                   stg.state_code AS state_code,
                   stg.zip_code AS zip_code,
                   substr(coalesce(concat(trim(stg.localhousingaddress), '-', trim(stg.otherlocalhousingaddress)), trim(stg.localhousingaddress), trim(stg.otherlocalhousingaddress)), 1, 200) AS local_housing_address_text,
                   stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_address_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_housing_type AS ht ON upper(rtrim(ht.housing_type_name)) = upper(rtrim(stg.housing))
   WHERE stg.nav_patient_id NOT IN
       (SELECT cn_patient_address.nav_patient_id
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_address) ) AS ms ON mt.nav_patient_id = ms.nav_patient_id
AND (coalesce(mt.housing_type_id, 0) = coalesce(ms.housing_type_id, 0)
     AND coalesce(mt.housing_type_id, 1) = coalesce(ms.housing_type_id, 1))
AND (upper(coalesce(mt.address_line_1_text, '0')) = upper(coalesce(ms.address_line_1_text, '0'))
     AND upper(coalesce(mt.address_line_1_text, '1')) = upper(coalesce(ms.address_line_1_text, '1')))
AND (upper(coalesce(mt.address_line_2_text, '0')) = upper(coalesce(ms.address_line_2_text, '0'))
     AND upper(coalesce(mt.address_line_2_text, '1')) = upper(coalesce(ms.address_line_2_text, '1')))
AND (upper(coalesce(mt.city_name, '0')) = upper(coalesce(ms.city_name, '0'))
     AND upper(coalesce(mt.city_name, '1')) = upper(coalesce(ms.city_name, '1')))
AND (upper(coalesce(mt.state_code, '0')) = upper(coalesce(ms.state_code, '0'))
     AND upper(coalesce(mt.state_code, '1')) = upper(coalesce(ms.state_code, '1')))
AND (upper(coalesce(mt.zip_code, '0')) = upper(coalesce(ms.zip_code, '0'))
     AND upper(coalesce(mt.zip_code, '1')) = upper(coalesce(ms.zip_code, '1')))
AND (upper(coalesce(mt.local_housing_address_text, '0')) = upper(coalesce(ms.local_housing_address_text, '0'))
     AND upper(coalesce(mt.local_housing_address_text, '1')) = upper(coalesce(ms.local_housing_address_text, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (nav_patient_id,
        housing_type_id,
        address_line_1_text,
        address_line_2_text,
        city_name,
        state_code,
        zip_code,
        local_housing_address_text,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.nav_patient_id, ms.housing_type_id, ms.address_line_1_text, ms.address_line_2_text, ms.city_name, ms.state_code, ms.zip_code, ms.local_housing_address_text, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT nav_patient_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_address
      GROUP BY nav_patient_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_address');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_Patient_Address');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF