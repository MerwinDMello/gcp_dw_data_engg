-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_cr_ref_regimen.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE			: 	EDWCR.Ref_Regimen             		#
-- #  TARGET  DATABASE	   	: 	EDWCR		 				#
-- #  SOURCE		   	: 	EDWCR_STAGING.Patient_Heme_Treatment_Regimen_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 							#
-- #  PROJECT             		:
-- #  Created by			:       Syntel   					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              		#
-- #####################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CR_REF_REGIMEN;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Patient_Heme_Treatment_Regimen_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Insert data into Core Table */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_regimen AS mt USING (
    SELECT DISTINCT
        row_number() OVER (ORDER BY upper(dis.regimen_name)) + coalesce((
          SELECT
              max(coalesce(a.regimen_id, 0)) AS max_key
            FROM
              `hca-hin-dev-cur-ops`.edwcr_base_views.ref_regimen AS a
        ), 0) AS regimen_id,
        substr(trim(dis.regimen_name), 1, 50) AS regimen_name,
        'N' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              trim(st.regimen) AS regimen_name
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_treatment_regimen_stg AS st
            WHERE st.regimen IS NOT NULL
        ) AS dis
      WHERE NOT EXISTS (
        SELECT
            1
          FROM
            `hca-hin-dev-cur-ops`.edwcr_base_views.ref_regimen AS rcdc
          WHERE upper(trim(rcdc.regimen_name)) = upper(trim(dis.regimen_name))
      )
  ) AS ms
  ON mt.regimen_id = ms.regimen_id
   AND (upper(coalesce(mt.regimen_name, '0')) = upper(coalesce(ms.regimen_name, '0'))
   AND upper(coalesce(mt.regimen_name, '1')) = upper(coalesce(ms.regimen_name, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (regimen_id, regimen_name, source_system_code, dw_last_update_date_time)
      VALUES (ms.regimen_id, ms.regimen_name, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Collect Stats on Core Table */ -- CALL dbadmin_procs.collect_stats_table ('edwcr','Ref_Regimen');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;
-- EOF
