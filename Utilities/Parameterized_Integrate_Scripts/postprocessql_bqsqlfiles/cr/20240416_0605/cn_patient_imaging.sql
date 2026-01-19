DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_imaging.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_Patient_Imaging	              		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CN_Patient_Imaging_STG			#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_IMAGING;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Imaging_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_imaging
WHERE upper(cn_patient_imaging.hashbite_ssk) NOT IN
    (SELECT upper(cn_patient_imaging_stg.hashbite_ssk) AS hashbite_ssk
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_imaging_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_imaging AS mt USING
  (SELECT DISTINCT stg.cn_patient_imaging_sid,
                   stg.core_record_type_id,
                   stg.nav_patient_id,
                   stg.med_spcl_physician_id,
                   stg.tumor_type_id,
                   stg.diagnosis_result_id,
                   stg.nav_diagnosis_id,
                   stg.navigator_id,
                   stg.coid AS coid,
                   'H' AS company_code,
                   stg.imaging_type_id,
                   stg.imaging_date,
                   rim.imaging_mode_id AS imaging_mode_id,
                   rs.side_id AS imaging_area_side_id,
                   stg.imaging_location_text,
                   rf.facility_id AS imaging_facility_id,
                   CASE
                       WHEN upper(rtrim(stg.birad_scale_code)) = 'RESULTS NOT AVAILABLE' THEN CAST(NULL AS STRING)
                       ELSE stg.birad_scale_code
                   END AS birad_scale_code, -- STG.Birad_Scale_Code,
 stg.comment_text,
 ds.status_id AS disease_status_id,
 ts.status_id AS treatment_status_id,
 stg.other_image_type_text,
 CASE upper(rtrim(stg.initial_diagnosis_ind))
     WHEN 'YES' THEN 'Y'
     WHEN 'NO' THEN 'N'
     ELSE 'U'
 END AS initial_diagnosis_ind,
 CASE upper(rtrim(stg.disease_monitoring_ind))
     WHEN 'YES' THEN 'Y'
     WHEN 'NO' THEN 'N'
     ELSE 'U'
 END AS disease_monitoring_ind,
 stg.radiology_result_text,
 stg.hashbite_ssk,
 'N' AS source_system_code,
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_imaging_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_imaging_mode AS rim ON upper(rtrim(coalesce(trim(stg.imagemode), 'X'))) = upper(rtrim(coalesce(trim(rim.imaging_mode_desc), 'X')))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_side AS rs ON upper(rtrim(coalesce(trim(stg.imagearea), 'XX'))) = upper(rtrim(coalesce(trim(rs.side_desc), 'XX')))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_facility AS rf ON upper(rtrim(coalesce(trim(stg.imagecenter), 'XXX'))) = upper(rtrim(coalesce(trim(rf.facility_name), 'XXX')))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status AS ds ON upper(rtrim(coalesce(trim(stg.disease_status), 'XXX'))) = upper(rtrim(coalesce(trim(ds.status_desc), 'XXX')))
   AND upper(rtrim(ds.status_type_desc)) = 'DISEASE'
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status AS ts ON upper(rtrim(coalesce(trim(stg.treatment_status), 'XXX'))) = upper(rtrim(coalesce(trim(ts.status_desc), 'XXX')))
   AND upper(rtrim(ts.status_type_desc)) = 'TREATMENT'
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_imaging.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_imaging) ) AS ms ON mt.cn_patient_imaging_sid = ms.cn_patient_imaging_sid
AND mt.core_record_type_id = ms.core_record_type_id
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.med_spcl_physician_id, 0) = coalesce(ms.med_spcl_physician_id, 0)
     AND coalesce(mt.med_spcl_physician_id, 1) = coalesce(ms.med_spcl_physician_id, 1))
AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
     AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
AND (coalesce(mt.diagnosis_result_id, 0) = coalesce(ms.diagnosis_result_id, 0)
     AND coalesce(mt.diagnosis_result_id, 1) = coalesce(ms.diagnosis_result_id, 1))
AND (coalesce(mt.nav_diagnosis_id, 0) = coalesce(ms.nav_diagnosis_id, 0)
     AND coalesce(mt.nav_diagnosis_id, 1) = coalesce(ms.nav_diagnosis_id, 1))
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND mt.coid = ms.coid
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (coalesce(mt.imaging_type_id, 0) = coalesce(ms.imaging_type_id, 0)
     AND coalesce(mt.imaging_type_id, 1) = coalesce(ms.imaging_type_id, 1))
AND (coalesce(mt.imaging_date, DATE '1970-01-01') = coalesce(ms.imaging_date, DATE '1970-01-01')
     AND coalesce(mt.imaging_date, DATE '1970-01-02') = coalesce(ms.imaging_date, DATE '1970-01-02'))
AND (coalesce(mt.imaging_mode_id, 0) = coalesce(ms.imaging_mode_id, 0)
     AND coalesce(mt.imaging_mode_id, 1) = coalesce(ms.imaging_mode_id, 1))
AND (coalesce(mt.imaging_area_side_id, 0) = coalesce(ms.imaging_area_side_id, 0)
     AND coalesce(mt.imaging_area_side_id, 1) = coalesce(ms.imaging_area_side_id, 1))
AND (upper(coalesce(mt.imaging_location_text, '0')) = upper(coalesce(ms.imaging_location_text, '0'))
     AND upper(coalesce(mt.imaging_location_text, '1')) = upper(coalesce(ms.imaging_location_text, '1')))
AND (coalesce(mt.imaging_facility_id, 0) = coalesce(ms.imaging_facility_id, 0)
     AND coalesce(mt.imaging_facility_id, 1) = coalesce(ms.imaging_facility_id, 1))
AND (upper(coalesce(mt.birad_scale_code, '0')) = upper(coalesce(ms.birad_scale_code, '0'))
     AND upper(coalesce(mt.birad_scale_code, '1')) = upper(coalesce(ms.birad_scale_code, '1')))
AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
     AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
AND (coalesce(mt.disease_status_id, 0) = coalesce(ms.disease_status_id, 0)
     AND coalesce(mt.disease_status_id, 1) = coalesce(ms.disease_status_id, 1))
AND (coalesce(mt.treatment_status_id, 0) = coalesce(ms.treatment_status_id, 0)
     AND coalesce(mt.treatment_status_id, 1) = coalesce(ms.treatment_status_id, 1))
AND (upper(coalesce(mt.other_image_type_text, '0')) = upper(coalesce(ms.other_image_type_text, '0'))
     AND upper(coalesce(mt.other_image_type_text, '1')) = upper(coalesce(ms.other_image_type_text, '1')))
AND (upper(coalesce(mt.initial_diagnosis_ind, '0')) = upper(coalesce(ms.initial_diagnosis_ind, '0'))
     AND upper(coalesce(mt.initial_diagnosis_ind, '1')) = upper(coalesce(ms.initial_diagnosis_ind, '1')))
AND (upper(coalesce(mt.disease_monitoring_ind, '0')) = upper(coalesce(ms.disease_monitoring_ind, '0'))
     AND upper(coalesce(mt.disease_monitoring_ind, '1')) = upper(coalesce(ms.disease_monitoring_ind, '1')))
AND (upper(coalesce(mt.radiology_result_text, '0')) = upper(coalesce(ms.radiology_result_text, '0'))
     AND upper(coalesce(mt.radiology_result_text, '1')) = upper(coalesce(ms.radiology_result_text, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_imaging_sid,
        core_record_type_id,
        nav_patient_id,
        med_spcl_physician_id,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        navigator_id,
        coid,
        company_code,
        imaging_type_id,
        imaging_date,
        imaging_mode_id,
        imaging_area_side_id,
        imaging_location_text,
        imaging_facility_id,
        birad_scale_code,
        comment_text,
        disease_status_id,
        treatment_status_id,
        other_image_type_text,
        initial_diagnosis_ind,
        disease_monitoring_ind,
        radiology_result_text,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_imaging_sid, ms.core_record_type_id, ms.nav_patient_id, ms.med_spcl_physician_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.imaging_type_id, ms.imaging_date, ms.imaging_mode_id, ms.imaging_area_side_id, ms.imaging_location_text, ms.imaging_facility_id, ms.birad_scale_code, ms.comment_text, ms.disease_status_id, ms.treatment_status_id, ms.other_image_type_text, ms.initial_diagnosis_ind, ms.disease_monitoring_ind, ms.radiology_result_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_imaging_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_imaging
      GROUP BY cn_patient_imaging_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_imaging');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_Patient_Imaging');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF