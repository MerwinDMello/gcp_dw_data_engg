DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/rad_onc_patient_history.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Rad_Onc_Patient_History                     	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.stg_DIMPATIENT 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_Rad_Onc_Patient_History;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_DIMPATIENT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_history;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_history AS mt USING
  (SELECT DISTINCT pt.patient_sk AS patient_sk,
                   dhd.history_query_id AS history_query_id,
                   substr(dhd.history_value_text, 1, 255) AS history_value_text,
                   dhd.source_system_code AS source_system_code,
                   dhd.dw_last_update_date_time
   FROM
     (SELECT CAST(NULL AS INT64) AS patient_sk,
             stg_dimpatient.dimsiteid AS dimsiteid,
             stg_dimpatient.dimpatientid AS dimpatientid,
             1 AS history_query_id,
             trim(substr(CAST(stg_dimpatient.noalcoholuseperweek AS STRING), 1, 255)) AS history_value_text,
             'R' AS source_system_code,
             datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            2 AS history_query_id,
                            trim(substr(ltrim(regexp_replace(format('%#4.1f', stg_dimpatient.nopacksperday), r'^( *?)(-)?0(\..*)', r'\2\3')), 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            3 AS history_query_id,
                            trim(substr(CAST(stg_dimpatient.noyearsquitalcohol AS STRING), 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            4 AS history_query_id,
                            trim(substr(CAST(stg_dimpatient.noyearsquitsmoking AS STRING), 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            5 AS history_query_id,
                            trim(substr(CAST(stg_dimpatient.noyearsactivesmoker AS STRING), 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            6 AS history_query_id,
                            trim(substr(stg_dimpatient.hazardmaterialcontactindicator, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            7 AS history_query_id,
                            trim(substr(stg_dimpatient.alcoholusestatus, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            8 AS history_query_id,
                            trim(substr(stg_dimpatient.smokingusestatus, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            9 AS history_query_id,
                            trim(substr(stg_dimpatient.patientmaritalstatus, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            10 AS history_query_id,
                            trim(substr(stg_dimpatient.patientoccupation, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            11 AS history_query_id,
                            trim(substr(stg_dimpatient.patientpresentemployername, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            12 AS history_query_id,
                            trim(substr(stg_dimpatient.mothername, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            13 AS history_query_id,
                            trim(substr(stg_dimpatient.fathername, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            14 AS history_query_id,
                            trim(substr(stg_dimpatient.ethnicity, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            15 AS history_query_id,
                            trim(substr(stg_dimpatient.religion, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            16 AS history_query_id,
                            trim(substr(stg_dimpatient.mothermaidenname, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            17 AS history_query_id,
                            trim(substr(stg_dimpatient.healthcaredpoa, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            18 AS history_query_id,
                            trim(substr(stg_dimpatient.donotresuscitate, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            19 AS history_query_id,
                            trim(substr(stg_dimpatient.donothospitalize, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            20 AS history_query_id,
                            trim(substr(stg_dimpatient.haslivingwill, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            21 AS history_query_id,
                            trim(substr(stg_dimpatient.isautopsyrequested, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            22 AS history_query_id,
                            trim(substr(stg_dimpatient.feedingrestrictions, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            23 AS history_query_id,
                            trim(substr(stg_dimpatient.isorgandonor, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            24 AS history_query_id,
                            trim(substr(stg_dimpatient.birthplace, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            25 AS history_query_id,
                            trim(substr(stg_dimpatient.medicationrestrictions, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            26 AS history_query_id,
                            trim(substr(stg_dimpatient.treatmentrestrictions, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            27 AS history_query_id,
                            trim(substr(stg_dimpatient.ismotestpatient, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            28 AS history_query_id,
                            trim(substr(stg_dimpatient.smokingsincedate, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            29 AS history_query_id,
                            trim(substr(stg_dimpatient.smokingquitdate, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            30 AS history_query_id,
                            trim(substr(stg_dimpatient.alcoholquitdate, 1, 255)) AS history_value_text,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimpatient) AS dhd
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site AS rr ON rr.source_site_id = dhd.dimsiteid
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient AS pt ON rr.site_sk = pt.site_sk
   AND dhd.dimpatientid = pt.source_patient_id
   WHERE dhd.history_value_text IS NOT NULL ) AS ms ON mt.patient_sk = ms.patient_sk
AND mt.history_query_id = ms.history_query_id
AND (upper(coalesce(mt.history_value_text, '0')) = upper(coalesce(ms.history_value_text, '0'))
     AND upper(coalesce(mt.history_value_text, '1')) = upper(coalesce(ms.history_value_text, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_sk,
        history_query_id,
        history_value_text,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.patient_sk, ms.history_query_id, ms.history_value_text, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_sk,
             history_query_id
      FROM `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_history
      GROUP BY patient_sk,
               history_query_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_history');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Rad_Onc_Patient_History');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF