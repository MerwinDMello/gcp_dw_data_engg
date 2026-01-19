DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/rad_onc_junc_patient_phone.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Rad_Onc_Junc_Patient_Phone                   ##
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
 --Job=J_CR_RO_Rad_Onc_Junc_Pat_Phone;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_DIMPATIENT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.rad_onc_junc_patient_phone;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.rad_onc_junc_patient_phone AS mt USING
  (SELECT DISTINCT pt.patient_sk AS patient_sk,
                   dim_sub.phone_num_type_code AS phone_num_type_code,
                   rp.phone_num_sk,
                   dim_sub.source_system_code AS source_system_code,
                   dim_sub.dw_last_update_date_time
   FROM
     (SELECT CAST(NULL AS INT64) AS patient_sk,
             stg_dimpatient.dimsiteid AS dimsiteid,
             stg_dimpatient.dimpatientid AS dimpatientid,
             'H' AS phone_num_type_code,
             stg_dimpatient.patienthomephone AS phone,
             'R' AS source_system_code,
             datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            'W' AS phone_num_type_code,
                            stg_dimpatient.patientworkphone AS phone,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            'M' AS phone_num_type_code,
                            stg_dimpatient.patientmobilephone AS phone,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            'P' AS phone_num_type_code,
                            stg_dimpatient.patientpagernumber AS phone,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_sk,
                            stg_dimpatient.dimsiteid AS dimsiteid,
                            stg_dimpatient.dimpatientid AS dimpatientid,
                            'T' AS phone_num_type_code,
                            stg_dimpatient.transportationphone AS phone,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_cr_stage_dataset_name }}.stg_dimpatient) AS dim_sub
   INNER JOIN {{ params.param_cr_base_views_dataset_name }}.rad_onc_phone AS rp ON upper(trim(dim_sub.phone)) = upper(trim(rp.phone_num_text))
   INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_site AS rr ON rr.source_site_id = dim_sub.dimsiteid
   INNER JOIN {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient AS pt ON rr.site_sk = pt.site_sk
   AND dim_sub.dimpatientid = pt.source_patient_id) AS ms ON mt.patient_sk = ms.patient_sk
AND mt.phone_num_type_code = ms.phone_num_type_code
AND (coalesce(mt.phone_num_sk, 0) = coalesce(ms.phone_num_sk, 0)
     AND coalesce(mt.phone_num_sk, 1) = coalesce(ms.phone_num_sk, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_sk,
        phone_num_type_code,
        phone_num_sk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.patient_sk, ms.phone_num_type_code, ms.phone_num_sk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_sk,
             phone_num_type_code
      FROM {{ params.param_cr_core_dataset_name }}.rad_onc_junc_patient_phone
      GROUP BY patient_sk,
               phone_num_type_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.rad_onc_junc_patient_phone');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Rad_Onc_Junc_Patient_Phone');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF