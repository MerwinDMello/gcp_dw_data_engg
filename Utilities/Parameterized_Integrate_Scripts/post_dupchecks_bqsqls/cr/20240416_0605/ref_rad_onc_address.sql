DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_address.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.RAD_ONC_ADDRESS                          ##
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
 --Job=J_CR_RO_REF_RAD_ONC_ADDRESS;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CR_RO_DIMSITE_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.rad_onc_address_wrk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO `hca-hin-dev-cur-ops`.edwcr_staging.rad_onc_address_wrk (address_sk, address_line_1_text, address_line_2_text, full_address_text, address_comment_text)
SELECT NULL AS address_sk,
       CAST(NULL AS STRING) AS address_line_1_text,
       CAST(NULL AS STRING) AS address_line_2_text,
       substr(trim(stg_dimhospitaldepartment.hospitalcompleteaddress), 1, 255) AS full_address_text,
       CAST(NULL AS STRING) AS address_comment_text
FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimhospitaldepartment
WHERE stg_dimhospitaldepartment.hospitalcompleteaddress IS NOT NULL
UNION DISTINCT
SELECT NULL AS address_sk,
       substr(trim(stg_dimpatient.patientaddressline1), 1, 255) AS address_line_1_text,
       substr(trim(stg_dimpatient.patientaddressline2), 1, 255) AS address_line_2_text,
       substr(trim(stg_dimpatient.patientfulladdress), 1, 255) AS full_address_text,
       substr(trim(stg_dimpatient.patientaddresscomment), 1, 255) AS address_comment_text
FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
WHERE stg_dimpatient.patientaddressline1 IS NOT NULL
  OR stg_dimpatient.patientaddressline2 IS NOT NULL
  OR stg_dimpatient.patientfulladdress IS NOT NULL
  OR stg_dimpatient.patientaddresscomment IS NOT NULL
UNION DISTINCT
SELECT NULL AS address_sk,
       substr(NULL, 1, 255) AS address_line_1_text,
       substr(NULL, 1, 255) AS address_line_2_text,
       substr(trim(stg_dimpatient.patemergencycontactaddress), 1, 255) AS full_address_text,
       CAST(NULL AS STRING) AS address_comment_text
FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
WHERE stg_dimpatient.patemergencycontactaddress IS NOT NULL
UNION DISTINCT
SELECT NULL AS address_sk,
       substr(NULL, 1, 255) AS address_line_1_text,
       substr(NULL, 1, 255) AS address_line_2_text,
       substr(trim(stg_dimdoctor.doctorcompleteaddress), 1, 255) AS full_address_text,
       substr(stg_dimdoctor.doctoraddresscomment, 1, 255) AS address_comment_text
FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimdoctor
WHERE stg_dimdoctor.doctorcompleteaddress IS NOT NULL
  OR stg_dimdoctor.doctoraddresscomment IS NOT NULL;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- Deleting Data
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.rad_onc_address;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.rad_onc_address AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(CASE
                                                         WHEN upper(trim(ds.address_line_1_text)) = '' THEN CAST(NULL AS STRING)
                                                         ELSE trim(ds.address_line_1_text)
                                                     END),
                                               upper(CASE
                                                         WHEN upper(trim(ds.address_line_2_text)) = '' THEN CAST(NULL AS STRING)
                                                         ELSE trim(ds.address_line_2_text)
                                                     END),
                                               upper(CASE
                                                         WHEN upper(trim(ds.full_address_text)) = '' THEN CAST(NULL AS STRING)
                                                         ELSE trim(ds.full_address_text)
                                                     END),
                                               upper(CASE
                                                         WHEN upper(trim(ds.address_comment_text)) = '' THEN CAST(NULL AS STRING)
                                                         ELSE trim(ds.address_comment_text)
                                                     END)) AS address_sk,
                                     substr(CASE
                                                WHEN upper(trim(ds.address_line_1_text)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.address_line_1_text)
                                            END, 1, 255) AS address_line_1_text,
                                     substr(CASE
                                                WHEN upper(trim(ds.address_line_2_text)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.address_line_2_text)
                                            END, 1, 255) AS address_line_2_text,
                                     substr(CASE
                                                WHEN upper(trim(ds.full_address_text)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.full_address_text)
                                            END, 1, 255) AS full_address_text,
                                     substr(CASE
                                                WHEN upper(trim(ds.address_comment_text)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(ds.address_comment_text)
                                            END, 1, 255) AS address_comment_text,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.rad_onc_address_wrk AS ds) AS ms ON mt.address_sk = ms.address_sk
AND (upper(coalesce(mt.address_line_1_text, '0')) = upper(coalesce(ms.address_line_1_text, '0'))
     AND upper(coalesce(mt.address_line_1_text, '1')) = upper(coalesce(ms.address_line_1_text, '1')))
AND (upper(coalesce(mt.address_line_2_text, '0')) = upper(coalesce(ms.address_line_2_text, '0'))
     AND upper(coalesce(mt.address_line_2_text, '1')) = upper(coalesce(ms.address_line_2_text, '1')))
AND (upper(coalesce(mt.full_address_text, '0')) = upper(coalesce(ms.full_address_text, '0'))
     AND upper(coalesce(mt.full_address_text, '1')) = upper(coalesce(ms.full_address_text, '1')))
AND (upper(coalesce(mt.address_comment_text, '0')) = upper(coalesce(ms.address_comment_text, '0'))
     AND upper(coalesce(mt.address_comment_text, '1')) = upper(coalesce(ms.address_comment_text, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (address_sk,
        address_line_1_text,
        address_line_2_text,
        full_address_text,
        address_comment_text,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.address_sk, ms.address_line_1_text, ms.address_line_2_text, ms.full_address_text, ms.address_comment_text, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT address_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.rad_onc_address
      GROUP BY address_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.rad_onc_address');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Rad_Onc_Address');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;