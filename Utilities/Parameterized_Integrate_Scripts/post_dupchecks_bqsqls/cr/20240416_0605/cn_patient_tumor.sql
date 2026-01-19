DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_tumor.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_Patient_Tumor			            #
-- #  SOURCE		: EDWCR_STAGING.CN_Patient_Tumor_STG			    #
-- #	                                                                            #
-- #  INITIAL RELEASE	   	: 2017-10-10					    #
-- #  PROJECT             	: CANCER RESEARCH	    				    #
-- #  ------------------------------------------------------------------------	    #
-- #                                                                              	    #
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_TUMOR;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Tumor_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_tumor
WHERE upper(cn_patient_tumor.hashbite_ssk) NOT IN
    (SELECT upper(cn_patient_tumor_stg.hashbite_ssk) AS hashbite_ssk
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_tumor_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_tumor AS mt USING
  (SELECT DISTINCT stg.cn_patient_tumor_sid,
                   stg.nav_patient_id,
                   stg.tumor_type_id,
                   stg.navigator_id,
                   stg.coid AS coid,
                   'H' AS company_code,
                   stg.electronic_folder_id_text,
                   rf1.facility_id AS referral_source_facility_id,
                   rs.status_id AS nav_status_id,
                   stg.identification_period_text,
                   stg.referral_date,
                   stg.referring_physician_id,
                   stg.nav_end_reason_text,
                   stg.treatment_end_reason_text,
                   pd.physician_id AS treatment_end_physician_id,
                   rf2.facility_id AS treatment_end_facility_id,
                   stg.hashbite_ssk,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_tumor_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_facility AS rf1 ON upper(rtrim(stg.tumorreferralsource)) = upper(rtrim(rf1.facility_name))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_status AS rs ON upper(rtrim(stg.navigationstatus)) = upper(rtrim(rs.status_desc))
   AND upper(rtrim(rs.status_type_desc)) = 'NAVIGATION'
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_physician_detail AS pd ON upper(rtrim(stg.endtreatmentphysician)) = upper(rtrim(pd.physician_name))
   AND pd.physician_phone_num IS NULL
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_facility AS rf2 ON upper(rtrim(stg.endtreatmentlocation)) = upper(rtrim(rf2.facility_name))
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_tumor.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_tumor) QUALIFY row_number() OVER (PARTITION BY stg.cn_patient_tumor_sid
                                                                                      ORDER BY treatment_end_physician_id DESC) = 1 ) AS ms ON mt.cn_patient_tumor_sid = ms.cn_patient_tumor_sid
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
     AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (upper(coalesce(mt.electronic_folder_id_text, '0')) = upper(coalesce(ms.electronic_folder_id_text, '0'))
     AND upper(coalesce(mt.electronic_folder_id_text, '1')) = upper(coalesce(ms.electronic_folder_id_text, '1')))
AND (coalesce(mt.referral_source_facility_id, 0) = coalesce(ms.referral_source_facility_id, 0)
     AND coalesce(mt.referral_source_facility_id, 1) = coalesce(ms.referral_source_facility_id, 1))
AND (coalesce(mt.nav_status_id, 0) = coalesce(ms.nav_status_id, 0)
     AND coalesce(mt.nav_status_id, 1) = coalesce(ms.nav_status_id, 1))
AND (upper(coalesce(mt.identification_period_text, '0')) = upper(coalesce(ms.identification_period_text, '0'))
     AND upper(coalesce(mt.identification_period_text, '1')) = upper(coalesce(ms.identification_period_text, '1')))
AND (coalesce(mt.referral_date, DATE '1970-01-01') = coalesce(ms.referral_date, DATE '1970-01-01')
     AND coalesce(mt.referral_date, DATE '1970-01-02') = coalesce(ms.referral_date, DATE '1970-01-02'))
AND (coalesce(mt.referring_physician_id, 0) = coalesce(ms.referring_physician_id, 0)
     AND coalesce(mt.referring_physician_id, 1) = coalesce(ms.referring_physician_id, 1))
AND (upper(coalesce(mt.nav_end_reason_text, '0')) = upper(coalesce(ms.nav_end_reason_text, '0'))
     AND upper(coalesce(mt.nav_end_reason_text, '1')) = upper(coalesce(ms.nav_end_reason_text, '1')))
AND (upper(coalesce(mt.treatment_end_reason_text, '0')) = upper(coalesce(ms.treatment_end_reason_text, '0'))
     AND upper(coalesce(mt.treatment_end_reason_text, '1')) = upper(coalesce(ms.treatment_end_reason_text, '1')))
AND (coalesce(mt.treatment_end_physician_id, 0) = coalesce(ms.treatment_end_physician_id, 0)
     AND coalesce(mt.treatment_end_physician_id, 1) = coalesce(ms.treatment_end_physician_id, 1))
AND (coalesce(mt.treatment_end_facility_id, 0) = coalesce(ms.treatment_end_facility_id, 0)
     AND coalesce(mt.treatment_end_facility_id, 1) = coalesce(ms.treatment_end_facility_id, 1))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_tumor_sid,
        nav_patient_id,
        tumor_type_id,
        navigator_id,
        coid,
        company_code,
        electronic_folder_id_text,
        referral_source_facility_id,
        nav_status_id,
        identification_period_text,
        referral_date,
        referring_physician_id,
        nav_end_reason_text,
        treatment_end_reason_text,
        treatment_end_physician_id,
        treatment_end_facility_id,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_tumor_sid, ms.nav_patient_id, ms.tumor_type_id, ms.navigator_id, ms.coid, ms.company_code, ms.electronic_folder_id_text, ms.referral_source_facility_id, ms.nav_status_id, ms.identification_period_text, ms.referral_date, ms.referring_physician_id, ms.nav_end_reason_text, ms.treatment_end_reason_text, ms.treatment_end_physician_id, ms.treatment_end_facility_id, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_tumor_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_tumor
      GROUP BY cn_patient_tumor_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_tumor');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_Patient_Tumor');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;