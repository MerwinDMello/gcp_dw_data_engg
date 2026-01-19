DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_cr_ref_treatment_phase.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE			: 	edwcr.Ref_Treatment_Phase             		#
-- #  TARGET  DATABASE	   	: 	EDWCR		 				#
-- #  SOURCE		   	: 	edwcr_staging.Patient_Heme_Treatment_Regimen_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 							#
-- #  PROJECT             		:
-- #  Created by			:       Syntel   					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              		#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_REF_TREATMENT_PHASE;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Patient_Heme_Treatment_Regimen_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert data into Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_treatment_phase AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(ref_treatment_phase)) + coalesce(
                                                                                        (SELECT max(coalesce(a.treatment_phase_id, 0)) AS max_key
                                                                                         FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_treatment_phase AS a), 0) AS treatment_phase_id,
                                     ref_treatment_phase AS ref_treatment_phase,
                                     'N' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT trim(st.treatmentphase) AS treatment_phase_desc
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_treatment_regimen_stg AS st
      WHERE st.treatmentphase IS NOT NULL ) AS dis
   CROSS JOIN UNNEST(ARRAY[ substr(trim(dis.treatment_phase_desc), 1, 50) ]) AS ref_treatment_phase
   WHERE NOT EXISTS
       (SELECT 1
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_treatment_phase AS rcdc
        WHERE upper(trim(rcdc.treatment_phase_desc)) = upper(trim(dis.treatment_phase_desc)) ) ) AS ms ON mt.treatment_phase_id = ms.treatment_phase_id
AND (upper(coalesce(mt.treatment_phase_desc, '0')) = upper(coalesce(ms.ref_treatment_phase, '0'))
     AND upper(coalesce(mt.treatment_phase_desc, '1')) = upper(coalesce(ms.ref_treatment_phase, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (treatment_phase_id,
        treatment_phase_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.treatment_phase_id, ms.ref_treatment_phase, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT treatment_phase_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_treatment_phase
      GROUP BY treatment_phase_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_treatment_phase');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Collect Stats on Core Table */ -- CALL dbadmin_procs.collect_stats_table ('edwcr','Ref_Treatment_Phase');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

RETURN;

---- EOF;;