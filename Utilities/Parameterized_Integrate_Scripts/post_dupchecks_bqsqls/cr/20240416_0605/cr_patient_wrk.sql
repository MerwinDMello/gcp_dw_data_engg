DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_wrk.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR_STAGING.CR_PATIENT_WRK             		#
-- #  TARGET  DATABASE	   	: EDWCR_STAGING	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CR_PATIENT_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_PATIENT_STG;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate WRK Table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_wrk;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate WRK Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_wrk AS mt USING
  (SELECT DISTINCT CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(x.patientid) AS INT64) AS cr_patient_id,
                   coalesce(rlc.master_lookup_sid,
                              (SELECT rcl.master_lookup_sid
                               FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rcl
                               LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS nm ON rcl.lookup_id = nm.lookup_sid
                               WHERE upper(rtrim(nm.lookup_name)) = 'GENDER'
                                 AND upper(rtrim(rcl.lookup_code)) = '-99' )) AS patient_gender_id,
                   coalesce(rlc1.master_lookup_sid,
                              (SELECT rcl1.master_lookup_sid
                               FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rcl1
                               LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS nm1 ON rcl1.lookup_id = nm1.lookup_sid
                               WHERE upper(rtrim(nm1.lookup_name)) = 'RACE'
                                 AND upper(rtrim(rcl1.lookup_code)) = '-99' )) AS patient_race_id,
                   coalesce(rlc2.master_lookup_sid,
                              (SELECT rcl2.master_lookup_sid
                               FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rcl2
                               LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS nm2 ON rcl2.lookup_id = nm2.lookup_sid
                               WHERE upper(rtrim(nm2.lookup_name)) = 'VITAL STATUS'
                                 AND upper(rtrim(rcl2.lookup_code)) = '-99' )) AS vital_status_id, -- Coalesce(RLC.Master_Lookup_Sid,(Select Master_Lookup_Sid from EDWCR.Ref_Lookup_Code where Lookup_Code=-99)) as Patient_Gender_Id,
 -- Coalesce(RLC1.Master_Lookup_Sid,(Select Master_Lookup_Sid from EDWCR.Ref_Lookup_Code where Lookup_Code=-99)) as Patient_Race_Id,
 -- Coalesce(RLC2.Master_Lookup_Sid,(Select Master_Lookup_Sid from EDWCR.Ref_Lookup_Code where Lookup_Code=-99)) as Vital_Status_Id,
 CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(x.patientsystemid) AS INT64) AS patientsystemid,
 x.coid AS coid,
 'H' AS companycode,
 x.birthdate,
 x.datelastcontact,
 substr(x.firstname, 1, 50) AS patient_first_name,
 substr(x.middlename, 1, 50) AS patient_middle_name,
 substr(x.lastname, 1, 50) AS patient_last_name,
 substr(x.patemail, 1, 50) AS patient_email_address_text,
 x.accessionno AS accession_num_code,
 substr(x.ptudf90, 1, 20) AS patient_market_urn_text,
 x.medrecno AS medical_record_num,
 x.source_system_code AS source_system_code,
 x.dw_last_update_date_time AS dw_last_update_date_time
   FROM
     (SELECT trim(format('%11d', a.patientid)) AS patientid,
             trim(a.sex) AS sex,
             trim(a.race1) AS race1,
             trim(a.vitalstatus) AS vitalstatus,
             trim(format('%11d', a.patientsystemid)) AS patientsystemid,
             trim(format('%11d', a.facilityid)) AS facilityid,
             trim(a.coid) AS coid,
             a.birthdate, /*CASE
              			WHEN SUBSTR(TRIM(BirthDate),5,4) IN ('9999' , '0000') THEN SUBSTR(TRIM(BirthDate),1,4)||'01'||'01'
              			WHEN  SUBSTR(TRIM(BirthDate),7,2) IN ('99' , '00') THEN SUBSTR(TRIM(BirthDate),1,6)||'01'
              			WHEN SUBSTR(TRIM(BirthDate),5,2) IN ('99' , '00') THEN SUBSTR(TRIM(BirthDate),1,4)||'01'||SUBSTR(TRIM(BirthDate),7,2)
              			ELSE TRIM(A.BirthDate)
              		END AS NEW_BIRTHDATE,*/ a.datelastcontact, /*CASE
              			WHEN SUBSTR(TRIM(DATELASTCONTACT),5,4) IN ('9999' , '0000') THEN SUBSTR(TRIM(DATELASTCONTACT),1,4)||'01'||'01'
              			WHEN  SUBSTR(TRIM(DATELASTCONTACT),7,2) IN ('99' , '00') THEN SUBSTR(TRIM(DATELASTCONTACT),1,6)||'01'
              			WHEN SUBSTR(TRIM(DATELASTCONTACT),5,2) IN ('99' , '00') THEN SUBSTR(TRIM(DATELASTCONTACT),1,4)||'01'||SUBSTR(TRIM(DATELASTCONTACT),7,2)
              			ELSE TRIM(A.DATELASTCONTACT)
              		END AS NEW_DATELASTCONTACT,*/ trim(a.firstname) AS firstname,
                                              trim(a.middlename) AS middlename,
                                              trim(a.lastname) AS lastname,
                                              trim(a.patemail) AS patemail,
                                              trim(a.accessionno) AS accessionno,
                                              trim(a.ptudf90) AS ptudf90,
                                              trim(a.medrecno) AS medrecno,
                                              a.source_system_code AS source_system_code,
                                              a.dw_last_update_date_time AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_stg AS a) AS x
   LEFT OUTER JOIN /*LEFT JOIN
        SYS_CALENDAR.CALENDAR B
        ON
        ((SUBSTR(X.NEW_BIRTHDATE,1,4 )-1900) *10000 +
        SUBSTR(X.NEW_BIRTHDATE,5,2 ) *100 +
        SUBSTR(X.NEW_BIRTHDATE,7,2 )) =CAST(B.CALENDAR_DATE AS INTEGER)
        LEFT JOIN
        SYS_CALENDAR.CALENDAR B1
        ON
        ( (SUBSTR(X.NEW_DATELASTCONTACT,1,4 )-1900) *10000 +
        SUBSTR(X.NEW_DATELASTCONTACT,5,2 ) *100 +
        SUBSTR(X.NEW_DATELASTCONTACT,7,2 )) =CAST(B1.CALENDAR_DATE AS INTEGER)*/ /*LEFT JOIN
        EDWCR.REF_LOOKUP_CODE RLC
        ON
        X.Sex=RLC.Lookup_Code
        LEFT JOIN
        EDWCR.REF_LOOKUP_NAME RLN
        ON
        RLC.Lookup_Id=RLN.Lookup_SID
        AND RLN.Lookup_Name='Gender'
        LEFT JOIN
        EDWCR.REF_LOOKUP_CODE RLC1
        ON
        X.Race1=RLC1.Lookup_Code
        LEFT JOIN
        EDWCR.REF_LOOKUP_NAME RLN1
        ON
        RLC1.Lookup_Id=RLN1.Lookup_SID
        AND RLN1.Lookup_Name='Race'
        LEFT JOIN
        EDWCR.REF_LOOKUP_CODE RLC2
        ON
        X.VitalStatus=RLC2.Lookup_Code
        LEFT JOIN
        EDWCR.REF_LOOKUP_NAME RLN2
        ON
        RLC2.Lookup_Id=RLN2.Lookup_SID
        AND RLN2.Lookup_Name='Vital Status'*/
     (SELECT rln.lookup_sid,
             rlc_0.lookup_code,
             rlc_0.lookup_sub_code,
             rlc_0.master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc_0 ON rlc_0.lookup_id = rln.lookup_sid
      WHERE upper(rtrim(rln.lookup_name)) = 'GENDER' ) AS rlc ON upper(rtrim(x.sex)) = upper(rtrim(rlc.lookup_code))
   LEFT OUTER JOIN
     (SELECT rln.lookup_sid,
             rlc_0.lookup_code,
             rlc_0.lookup_sub_code,
             rlc_0.master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc_0 ON rlc_0.lookup_id = rln.lookup_sid
      AND upper(rtrim(rln.lookup_name)) = 'RACE') AS rlc1 ON upper(rtrim(x.race1)) = upper(rtrim(rlc1.lookup_code))
   LEFT OUTER JOIN
     (SELECT rln.lookup_sid,
             rlc_0.lookup_code,
             rlc_0.lookup_sub_code,
             rlc_0.master_lookup_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
      INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc_0 ON rlc_0.lookup_id = rln.lookup_sid
      WHERE upper(rtrim(rln.lookup_name)) = 'VITAL STATUS' ) AS rlc2 ON upper(rtrim(x.vitalstatus)) = upper(rtrim(rlc2.lookup_code))) AS ms ON mt.cr_patient_id = ms.cr_patient_id
AND (coalesce(mt.patient_gender_id, 0) = coalesce(ms.patient_gender_id, 0)
     AND coalesce(mt.patient_gender_id, 1) = coalesce(ms.patient_gender_id, 1))
AND (coalesce(mt.patient_race_id, 0) = coalesce(ms.patient_race_id, 0)
     AND coalesce(mt.patient_race_id, 1) = coalesce(ms.patient_race_id, 1))
AND (coalesce(mt.vital_status_id, 0) = coalesce(ms.vital_status_id, 0)
     AND coalesce(mt.vital_status_id, 1) = coalesce(ms.vital_status_id, 1))
AND (coalesce(mt.patient_system_id, 0) = coalesce(ms.patientsystemid, 0)
     AND coalesce(mt.patient_system_id, 1) = coalesce(ms.patientsystemid, 1))
AND mt.coid = ms.coid
AND mt.company_code = ms.companycode
AND (coalesce(mt.patient_birth_date, DATE '1970-01-01') = coalesce(ms.birthdate, DATE '1970-01-01')
     AND coalesce(mt.patient_birth_date, DATE '1970-01-02') = coalesce(ms.birthdate, DATE '1970-01-02'))
AND (coalesce(mt.last_contact_date, DATE '1970-01-01') = coalesce(ms.datelastcontact, DATE '1970-01-01')
     AND coalesce(mt.last_contact_date, DATE '1970-01-02') = coalesce(ms.datelastcontact, DATE '1970-01-02'))
AND (upper(coalesce(mt.patient_first_name, '0')) = upper(coalesce(ms.patient_first_name, '0'))
     AND upper(coalesce(mt.patient_first_name, '1')) = upper(coalesce(ms.patient_first_name, '1')))
AND (upper(coalesce(mt.patient_middle_name, '0')) = upper(coalesce(ms.patient_middle_name, '0'))
     AND upper(coalesce(mt.patient_middle_name, '1')) = upper(coalesce(ms.patient_middle_name, '1')))
AND (upper(coalesce(mt.patient_last_name, '0')) = upper(coalesce(ms.patient_last_name, '0'))
     AND upper(coalesce(mt.patient_last_name, '1')) = upper(coalesce(ms.patient_last_name, '1')))
AND (upper(coalesce(mt.patient_email_address_text, '0')) = upper(coalesce(ms.patient_email_address_text, '0'))
     AND upper(coalesce(mt.patient_email_address_text, '1')) = upper(coalesce(ms.patient_email_address_text, '1')))
AND (upper(coalesce(mt.accession_num_code, '0')) = upper(coalesce(ms.accession_num_code, '0'))
     AND upper(coalesce(mt.accession_num_code, '1')) = upper(coalesce(ms.accession_num_code, '1')))
AND (upper(coalesce(mt.patient_market_urn_text, '0')) = upper(coalesce(ms.patient_market_urn_text, '0'))
     AND upper(coalesce(mt.patient_market_urn_text, '1')) = upper(coalesce(ms.patient_market_urn_text, '1')))
AND (upper(coalesce(mt.medical_record_num, '0')) = upper(coalesce(ms.medical_record_num, '0'))
     AND upper(coalesce(mt.medical_record_num, '1')) = upper(coalesce(ms.medical_record_num, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cr_patient_id,
        patient_gender_id,
        patient_race_id,
        vital_status_id,
        patient_system_id,
        coid,
        company_code,
        patient_birth_date,
        last_contact_date,
        patient_first_name,
        patient_middle_name,
        patient_last_name,
        patient_email_address_text,
        accession_num_code,
        patient_market_urn_text,
        medical_record_num,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cr_patient_id, ms.patient_gender_id, ms.patient_race_id, ms.vital_status_id, ms.patientsystemid, ms.coid, ms.companycode, ms.birthdate, ms.datelastcontact, ms.patient_first_name, ms.patient_middle_name, ms.patient_last_name, ms.patient_email_address_text, ms.accession_num_code, ms.patient_market_urn_text, ms.medical_record_num, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cr_patient_id
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_wrk
      GROUP BY cr_patient_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr_staging.cr_patient_wrk');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','CR_PATIENT_WRK');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;