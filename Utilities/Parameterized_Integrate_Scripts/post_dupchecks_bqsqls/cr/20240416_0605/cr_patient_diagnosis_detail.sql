DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_diagnosis_detail.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CR_PATIENT_DIAGNOSIS_DETAIL             		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_DIAGNOSIS_DETAIL_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_DIAGNOSIS_DETAIL;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CR_PATIENT_DIAGNOSIS_DETAIL_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_patient_diagnosis_detail;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient_diagnosis_detail AS mt USING
  (SELECT DISTINCT stg.tumor_id,
                   stg.cr_patient_id,
                   coalesce(lkp.master_lookup_sid,
                              (SELECT rlc.master_lookup_sid
                               FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
                               INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
                               WHERE upper(rtrim(rln.lookup_name)) = 'LATERALITY'
                                 AND upper(rtrim(rlc.lookup_code)) = '-99' )) AS tumor_site_id,
                   coalesce(lkp1.master_lookup_sid,
                              (SELECT rlc1.master_lookup_sid
                               FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln1
                               INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc1 ON rlc1.lookup_id = rln1.lookup_sid
                               WHERE upper(rtrim(rln1.lookup_name)) = 'HISTOLOGY'
                                 AND upper(rtrim(rlc1.lookup_code)) = '-99' )) AS diagnosis_name_id,
                   stg.diagnosis_date,
                   stg.diagnose_age_num AS diagnosis_age_num,
                   stg.first_diagnose_year_num,
                   'M' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_diagnosis_detail_stg AS stg
   LEFT OUTER JOIN
     (SELECT rln.lookup_sid,
             rlc.lookup_code,
             rlc.lookup_sub_code,
             rlc.master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
      WHERE upper(rtrim(rln.lookup_name)) = 'LATERALITY' ) AS lkp ON upper(rtrim(stg.laterality)) = upper(rtrim(lkp.lookup_code))
   LEFT OUTER JOIN -- AND STG.histosubcode = LKP.Lookup_Sub_Code

     (SELECT rln.lookup_sid,
             rlc.lookup_code,
             rlc.lookup_sub_code,
             rlc.master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
      WHERE upper(rtrim(rln.lookup_name)) = 'HISTOLOGY' ) AS lkp1 ON upper(rtrim(stg.histology)) = upper(rtrim(lkp1.lookup_code))
   AND upper(rtrim(stg.histosubcode)) = upper(rtrim(lkp1.lookup_sub_code)) QUALIFY row_number() OVER (PARTITION BY stg.tumor_id,
                                                                                                                   stg.cr_patient_id
                                                                                                      ORDER BY stg.tumor_id,
                                                                                                               stg.cr_patient_id DESC) = 1) AS ms ON mt.tumor_id = ms.tumor_id
AND mt.cr_patient_id = ms.cr_patient_id
AND (coalesce(mt.tumor_site_id, 0) = coalesce(ms.tumor_site_id, 0)
     AND coalesce(mt.tumor_site_id, 1) = coalesce(ms.tumor_site_id, 1))
AND (coalesce(mt.diagnosis_name_id, 0) = coalesce(ms.diagnosis_name_id, 0)
     AND coalesce(mt.diagnosis_name_id, 1) = coalesce(ms.diagnosis_name_id, 1))
AND (coalesce(mt.diagnosis_date, DATE '1970-01-01') = coalesce(ms.diagnosis_date, DATE '1970-01-01')
     AND coalesce(mt.diagnosis_date, DATE '1970-01-02') = coalesce(ms.diagnosis_date, DATE '1970-01-02'))
AND (coalesce(mt.diagnose_age_num, 0) = coalesce(ms.diagnosis_age_num, 0)
     AND coalesce(mt.diagnose_age_num, 1) = coalesce(ms.diagnosis_age_num, 1))
AND (coalesce(mt.first_diagnose_year_num, 0) = coalesce(ms.first_diagnose_year_num, 0)
     AND coalesce(mt.first_diagnose_year_num, 1) = coalesce(ms.first_diagnose_year_num, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (tumor_id,
        cr_patient_id,
        tumor_site_id,
        diagnosis_name_id,
        diagnosis_date,
        diagnose_age_num,
        first_diagnose_year_num,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.tumor_id, ms.cr_patient_id, ms.tumor_site_id, ms.diagnosis_name_id, ms.diagnosis_date, ms.diagnosis_age_num, ms.first_diagnose_year_num, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT tumor_id,
             cr_patient_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cr_patient_diagnosis_detail
      GROUP BY tumor_id,
               cr_patient_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cr_patient_diagnosis_detail');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CR_PATIENT_DIAGNOSIS_DETAIL');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;