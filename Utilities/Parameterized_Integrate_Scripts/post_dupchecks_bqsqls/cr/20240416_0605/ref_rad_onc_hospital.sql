DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_rad_onc_hospital.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_RAD_ONC_HOSPITAL                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		  		   : EDWCR_STAGING.stg_DimHospitalDepartment     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_REF_RAD_ONC_HOSPITAL;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','STG_DIMHOSPITALDEPARTMENT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_hospital;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_hospital AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimsiteid) AS INT64),
                                               CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimhospitaldepartmentid) AS INT64)) AS hospital_sk,
                                     ra.address_sk AS hospital_address_sk,
                                     rr.site_sk AS site_sk,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimhospitaldepartmentid) AS INT64) AS source_hospital_id,
                                     coalesce(ff.coid, '-1') AS coid,
                                     'H' AS company_code,
                                     substr(CASE
                                                WHEN upper(trim(dhd.hospitalname)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.hospitalname)
                                            END, 1, 65) AS hospital_name,
                                     substr(CASE
                                                WHEN upper(trim(dhd.hospitalwebaddress)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.hospitalwebaddress)
                                            END, 1, 65) AS hospital_email_address_text,
                                     substr(CASE
                                                WHEN upper(trim(dhd.hospitalhstryusername)) = '' THEN CAST(NULL AS STRING)
                                                ELSE trim(dhd.hospitalhstryusername)
                                            END, 1, 30) AS hospital_history_user_name,
                                     CAST(trim(substr(trim(dhd.hospitalhstrydatetime), 1, 19)) AS DATETIME) AS hospital_history_date_time,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.logid) AS INT64) AS log_id,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.runid) AS INT64) AS run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimhospitaldepartment AS dhd
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_address AS ra ON upper(trim(dhd.hospitalcompleteaddress)) = upper(trim(ra.full_address_text))
   AND ra.address_line_1_text IS NULL
   AND ra.address_line_2_text IS NULL
   AND ra.address_comment_text IS NULL
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON rr.source_site_id = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dhd.dimsiteid) AS INT64)
   LEFT OUTER JOIN
     (SELECT DISTINCT stg_sc_factfacility.hospitalname,
                      stg_sc_factfacility.coid
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_sc_factfacility
      WHERE upper(trim(stg_sc_factfacility.hospitalname)) <> '' ) AS ff ON upper(trim(dhd.hospitalname)) = upper(trim(ff.hospitalname))) AS ms ON mt.hospital_sk = ms.hospital_sk
AND (coalesce(mt.hospital_address_sk, 0) = coalesce(ms.hospital_address_sk, 0)
     AND coalesce(mt.hospital_address_sk, 1) = coalesce(ms.hospital_address_sk, 1))
AND mt.site_sk = ms.site_sk
AND mt.source_hospital_id = ms.source_hospital_id
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (upper(coalesce(mt.hospital_name, '0')) = upper(coalesce(ms.hospital_name, '0'))
     AND upper(coalesce(mt.hospital_name, '1')) = upper(coalesce(ms.hospital_name, '1')))
AND (upper(coalesce(mt.hospital_email_address_text, '0')) = upper(coalesce(ms.hospital_email_address_text, '0'))
     AND upper(coalesce(mt.hospital_email_address_text, '1')) = upper(coalesce(ms.hospital_email_address_text, '1')))
AND (upper(coalesce(mt.hospital_history_user_name, '0')) = upper(coalesce(ms.hospital_history_user_name, '0'))
     AND upper(coalesce(mt.hospital_history_user_name, '1')) = upper(coalesce(ms.hospital_history_user_name, '1')))
AND (coalesce(mt.hospital_history_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.hospital_history_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.hospital_history_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.hospital_history_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (hospital_sk,
        hospital_address_sk,
        site_sk,
        source_hospital_id,
        coid,
        company_code,
        hospital_name,
        hospital_email_address_text,
        hospital_history_user_name,
        hospital_history_date_time,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.hospital_sk, ms.hospital_address_sk, ms.site_sk, ms.source_hospital_id, ms.coid, ms.company_code, ms.hospital_name, ms.hospital_email_address_text, ms.hospital_history_user_name, ms.hospital_history_date_time, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT hospital_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_hospital
      GROUP BY hospital_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_hospital');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_RAD_ONC_HOSPITAL');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;