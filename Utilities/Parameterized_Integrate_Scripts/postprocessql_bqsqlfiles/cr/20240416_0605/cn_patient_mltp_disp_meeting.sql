DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_mltp_disp_meeting.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_Patient_Mltp_Disciplinary_Meeting	              #
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_staging.CN_Patient_Mltp_Disciplinary_Meeting_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_Patient_Mltp_Disciplinary_Meeting;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Mltp_Disciplinary_Meeting_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_mltp_disciplinary_meeting
WHERE upper(cn_patient_mltp_disciplinary_meeting.hashbite_ssk) NOT IN
    (SELECT upper(cn_patient_mltp_disciplinary_meeting_stg.hashbite_ssk) AS hashbite_ssk
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_mltp_disciplinary_meeting_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_mltp_disciplinary_meeting AS mt USING
  (SELECT DISTINCT stg.cn_patient_mltp_disc_meet_sid,
                   stg.nav_patient_id,
                   stg.tumor_type_id,
                   stg.navigator_id,
                   stg.coid AS coid,
                   stg.company_code AS company_code,
                   stg.meeting_date,
                   stg.patient_discussed_ind AS patient_discussed_ind,
                   stg.treatment_change_ind AS treatment_change_ind,
                   stg.meeting_notes_text,
                   stg.hashbite_ssk,
                   stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_mltp_disciplinary_meeting_stg AS stg
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_mltp_disciplinary_meeting.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_mltp_disciplinary_meeting) ) AS ms ON mt.cn_patient_mltp_disciplinary_meet_sid = ms.cn_patient_mltp_disc_meet_sid
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
     AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (coalesce(mt.meeting_date, DATE '1970-01-01') = coalesce(ms.meeting_date, DATE '1970-01-01')
     AND coalesce(mt.meeting_date, DATE '1970-01-02') = coalesce(ms.meeting_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.patient_discussed_ind, '0')) = upper(coalesce(ms.patient_discussed_ind, '0'))
     AND upper(coalesce(mt.patient_discussed_ind, '1')) = upper(coalesce(ms.patient_discussed_ind, '1')))
AND (upper(coalesce(mt.treatment_change_ind, '0')) = upper(coalesce(ms.treatment_change_ind, '0'))
     AND upper(coalesce(mt.treatment_change_ind, '1')) = upper(coalesce(ms.treatment_change_ind, '1')))
AND (upper(coalesce(mt.meeting_notes_text, '0')) = upper(coalesce(ms.meeting_notes_text, '0'))
     AND upper(coalesce(mt.meeting_notes_text, '1')) = upper(coalesce(ms.meeting_notes_text, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_mltp_disciplinary_meet_sid,
        nav_patient_id,
        tumor_type_id,
        navigator_id,
        coid,
        company_code,
        meeting_date,
        patient_discussed_ind,
        treatment_change_ind,
        meeting_notes_text,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_mltp_disc_meet_sid, ms.nav_patient_id, ms.tumor_type_id, ms.navigator_id, ms.coid, ms.company_code, ms.meeting_date, ms.patient_discussed_ind, ms.treatment_change_ind, ms.meeting_notes_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_mltp_disciplinary_meet_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_mltp_disciplinary_meeting
      GROUP BY cn_patient_mltp_disciplinary_meet_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_mltp_disciplinary_meeting');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_Patient_Mltp_Disciplinary_Meeting');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF