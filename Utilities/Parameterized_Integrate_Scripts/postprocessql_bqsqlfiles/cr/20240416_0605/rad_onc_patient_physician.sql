DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/rad_onc_patient_physician.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Rad_Onc_Patient_Physician                    ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_STAGING.stg_DIMPATIENTDOCTOR					##
-- ##							  													##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_Rad_Onc_Patient_Physician;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_DIMPATIENTDOCTOR');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_physician;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_physician AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY dhd.dimsiteid,
                                               dhd.dimpatientdoctorid) AS patient_physician_sk,
                                     rs.site_sk AS site_sk,
                                     dhd.dimpatientdoctorid AS source_patient_physician_id,
                                     ra.patient_sk AS patient_sk,
                                     CASE dhd.primaryflag
                                         WHEN 1 THEN 'Y'
                                         WHEN 0 THEN 'N'
                                         ELSE CAST(NULL AS STRING)
                                     END AS primary_physician_ind,
                                     CASE dhd.oncologistflag
                                         WHEN 1 THEN 'Y'
                                         WHEN 0 THEN 'N'
                                         ELSE CAST(NULL AS STRING)
                                     END AS oncologist_ind,
                                     dhd.ctrresourceser AS resource_id,
                                     dhd.logid AS log_id,
                                     dhd.runid AS run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatientdoctor AS dhd
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rs ON rs.source_site_id = dhd.dimsiteid
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient AS ra ON dhd.dimpatientid = ra.source_patient_id
   AND rs.site_sk = ra.site_sk) AS ms ON mt.patient_physician_sk = ms.patient_physician_sk
AND mt.site_sk = ms.site_sk
AND mt.source_patient_physician_id = ms.source_patient_physician_id
AND (coalesce(mt.patient_sk, 0) = coalesce(ms.patient_sk, 0)
     AND coalesce(mt.patient_sk, 1) = coalesce(ms.patient_sk, 1))
AND (upper(coalesce(mt.primary_physician_ind, '0')) = upper(coalesce(ms.primary_physician_ind, '0'))
     AND upper(coalesce(mt.primary_physician_ind, '1')) = upper(coalesce(ms.primary_physician_ind, '1')))
AND (upper(coalesce(mt.oncologist_ind, '0')) = upper(coalesce(ms.oncologist_ind, '0'))
     AND upper(coalesce(mt.oncologist_ind, '1')) = upper(coalesce(ms.oncologist_ind, '1')))
AND (coalesce(mt.resource_id, 0) = coalesce(ms.resource_id, 0)
     AND coalesce(mt.resource_id, 1) = coalesce(ms.resource_id, 1))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_physician_sk,
        site_sk,
        source_patient_physician_id,
        patient_sk,
        primary_physician_ind,
        oncologist_ind,
        resource_id,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.patient_physician_sk, ms.site_sk, ms.source_patient_physician_id, ms.patient_sk, ms.primary_physician_ind, ms.oncologist_ind, ms.resource_id, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_physician_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_physician
      GROUP BY patient_physician_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_physician');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Rad_Onc_Patient_Physician');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF