DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_insurance.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CR_PATIENT_INSURANCE	             		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_INSURANCE_STG			#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_INSURANCE;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CR_PATIENT_INSURANCE_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate Core Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_patient_insurance;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient_insurance AS mt USING
  (SELECT DISTINCT stg.tumor_id AS tumor_id,
                   stg.patient_id AS cr_patient_id,
                   coalesce(lkp.master_lookup_sid,
                              (SELECT rlc.master_lookup_sid
                               FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
                               INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
                               WHERE upper(rtrim(rln.lookup_name)) = 'INSURANCE TYPE'
                                 AND upper(rtrim(rlc.lookup_code)) = '-99' )) AS insurance_type_id,
                   lkp1.master_lookup_sid AS insurance_company_id,
                   'M' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_insurance_stg AS stg
   LEFT OUTER JOIN
     (SELECT rln.lookup_sid,
             rlc.lookup_code,
             rlc.master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
      WHERE upper(rtrim(rln.lookup_name)) = 'INSURANCE TYPE' ) AS lkp ON upper(rtrim(stg.primpayerdx)) = upper(rtrim(lkp.lookup_code))
   LEFT OUTER JOIN
     (SELECT rln.lookup_sid,
             rlc.lookup_desc,
             rlc.master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
      WHERE upper(rtrim(rln.lookup_name)) = 'INSURANCE COMPANY' ) AS lkp1 ON upper(rtrim(CASE
                                                                                             WHEN stg.txtpaysource IS NULL
                                                                                                  OR upper(rtrim(stg.txtpaysource)) = '' THEN 'Unknown Description'
                                                                                             ELSE stg.txtpaysource
                                                                                         END)) = upper(rtrim(lkp1.lookup_desc))) AS ms ON mt.tumor_id = ms.tumor_id
AND mt.cr_patient_id = ms.cr_patient_id
AND (coalesce(mt.insurance_type_id, 0) = coalesce(ms.insurance_type_id, 0)
     AND coalesce(mt.insurance_type_id, 1) = coalesce(ms.insurance_type_id, 1))
AND (coalesce(mt.insurance_company_id, 0) = coalesce(ms.insurance_company_id, 0)
     AND coalesce(mt.insurance_company_id, 1) = coalesce(ms.insurance_company_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (tumor_id,
        cr_patient_id,
        insurance_type_id,
        insurance_company_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.tumor_id, ms.cr_patient_id, ms.insurance_type_id, ms.insurance_company_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cr_patient_id,
             tumor_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cr_patient_insurance
      GROUP BY cr_patient_id,
               tumor_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cr_patient_insurance');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CR_PATIENT_INSURANCE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;