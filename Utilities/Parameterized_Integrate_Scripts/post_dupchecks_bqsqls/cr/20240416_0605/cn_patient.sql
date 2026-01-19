DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_Patient	           				#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_staging.CN_Patient_STG				#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient
WHERE cn_patient.nav_patient_id NOT IN
    (SELECT cn_patient_stg.nav_patient_id
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient AS mt USING
  (SELECT DISTINCT stg.nav_patient_id,
                   stg.navigator_id,
                   CASE
                       WHEN trim(stg.coid) IS NULL THEN '-1'
                       ELSE stg.coid
                   END AS coid,
                   'H' AS company_code,
                   CASE
                       WHEN upper(trim(stg.patient_market_urn)) = '' THEN CAST(NULL AS STRING)
                       ELSE stg.patient_market_urn
                   END AS patient_market_urn,
                   CASE
                       WHEN upper(trim(stg.medical_record_num)) = '' THEN CAST(NULL AS STRING)
                       ELSE stg.medical_record_num
                   END AS medical_record_num,
                   stg.empi_text,
                   pd1.physician_id AS gynecologist_physician_id,
                   pd2.physician_id AS primary_care_physician_id,
                   cf.facility_mnemonic_cs AS facility_mnemonic_cs,
                   cf.network_mnemonic_cs AS network_mnemonic_cs,
                   stg.nav_create_date,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_physician_detail AS pd1 ON upper(rtrim(coalesce(trim(stg.gynecologist), 'X'))) = upper(rtrim(coalesce(trim(pd1.physician_name), 'X')))
   AND upper(rtrim(coalesce(trim(stg.gynecologistphone), 'X'))) = upper(rtrim(coalesce(trim(pd1.physician_phone_num), 'X')))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_physician_detail AS pd2 ON upper(rtrim(coalesce(trim(stg.primarycarephysician), 'XX'))) = upper(rtrim(coalesce(trim(pd2.physician_name), 'XX')))
   AND upper(rtrim(coalesce(trim(stg.pcpphone), 'XX'))) = upper(rtrim(coalesce(trim(pd2.physician_phone_num), 'XX')))
   LEFT OUTER JOIN
     (SELECT clinical_facility.facility_mnemonic_cs,
             clinical_facility.network_mnemonic_cs,
             clinical_facility.coid,
             clinical_facility.facility_active_ind
      FROM `hca-hin-dev-cur-ops`.edw_pub_views.clinical_facility QUALIFY row_number() OVER (PARTITION BY upper(clinical_facility.coid)
                                                                                            ORDER BY upper(clinical_facility.facility_active_ind) DESC) = 1) AS cf ON upper(rtrim(stg.coid)) = upper(rtrim(cf.coid))
   WHERE stg.nav_patient_id NOT IN
       (SELECT -- where STG.COID is NOT NULL
 cn_patient.nav_patient_id
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient) QUALIFY row_number() OVER (PARTITION BY stg.nav_patient_id
                                                                                ORDER BY primary_care_physician_id DESC) = 1 ) AS ms ON mt.nav_patient_id = ms.nav_patient_id
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (upper(coalesce(mt.patient_market_urn, '0')) = upper(coalesce(ms.patient_market_urn, '0'))
     AND upper(coalesce(mt.patient_market_urn, '1')) = upper(coalesce(ms.patient_market_urn, '1')))
AND (upper(coalesce(mt.medical_record_num, '0')) = upper(coalesce(ms.medical_record_num, '0'))
     AND upper(coalesce(mt.medical_record_num, '1')) = upper(coalesce(ms.medical_record_num, '1')))
AND (upper(coalesce(mt.empi_text, '0')) = upper(coalesce(ms.empi_text, '0'))
     AND upper(coalesce(mt.empi_text, '1')) = upper(coalesce(ms.empi_text, '1')))
AND (coalesce(mt.gynecologist_physician_id, 0) = coalesce(ms.gynecologist_physician_id, 0)
     AND coalesce(mt.gynecologist_physician_id, 1) = coalesce(ms.gynecologist_physician_id, 1))
AND (coalesce(mt.primary_care_physician_id, 0) = coalesce(ms.primary_care_physician_id, 0)
     AND coalesce(mt.primary_care_physician_id, 1) = coalesce(ms.primary_care_physician_id, 1))
AND (coalesce(mt.facility_mnemonic_cs, '0') = coalesce(ms.facility_mnemonic_cs, '0')
     AND coalesce(mt.facility_mnemonic_cs, '1') = coalesce(ms.facility_mnemonic_cs, '1'))
AND (coalesce(mt.network_mnemonic_cs, '0') = coalesce(ms.network_mnemonic_cs, '0')
     AND coalesce(mt.network_mnemonic_cs, '1') = coalesce(ms.network_mnemonic_cs, '1'))
AND (coalesce(mt.nav_create_date, DATE '1970-01-01') = coalesce(ms.nav_create_date, DATE '1970-01-01')
     AND coalesce(mt.nav_create_date, DATE '1970-01-02') = coalesce(ms.nav_create_date, DATE '1970-01-02'))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (nav_patient_id,
        navigator_id,
        coid,
        company_code,
        patient_market_urn,
        medical_record_num,
        empi_text,
        gynecologist_physician_id,
        primary_care_physician_id,
        facility_mnemonic_cs,
        network_mnemonic_cs,
        nav_create_date,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.nav_patient_id, ms.navigator_id, ms.coid, ms.company_code, ms.patient_market_urn, ms.medical_record_num, ms.empi_text, ms.gynecologist_physician_id, ms.primary_care_physician_id, ms.facility_mnemonic_cs, ms.network_mnemonic_cs, ms.nav_create_date, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT nav_patient_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient
      GROUP BY nav_patient_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_PATIENT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;