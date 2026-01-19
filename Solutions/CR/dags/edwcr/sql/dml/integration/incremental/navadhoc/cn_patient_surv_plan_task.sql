DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_surv_plan_task.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_PATIENT_SURV_PLAN_TASK	              #
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_staging.CN_Patient_Survivorship_Plan_Task_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_SURV_PLAN_TASK;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Survivorship_Plan_Task_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_cr_core_dataset_name }}.cn_patient_survivorship_plan_task
WHERE upper(cn_patient_survivorship_plan_task.hashbite_ssk) NOT IN
    (SELECT upper(cn_patient_survivorship_plan_task_stg.hashbite_ssk) AS hashbite_ssk
     FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_survivorship_plan_task_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cn_patient_survivorship_plan_task AS mt USING
  (SELECT DISTINCT stg.nav_survivorship_plan_task_sid,
                   ref2.status_id,
                   ref1.contact_method_id,
                   stg.nav_patient_id,
                   stg.navigator_id,
                   stg.coid AS coid,
                   stg.company_code AS company_code,
                   stg.task_desc_text,
                   stg.task_resolution_date,
                   stg.task_closed_date,
                   stg.contact_result_text,
                   stg.contact_date,
                   stg.comment_text,
                   stg.hashbite_ssk,
                   stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_survivorship_plan_task_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_contact_method AS ref1 ON upper(rtrim(stg.taskmeasnofcontact)) = upper(rtrim(ref1.contact_method_desc))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_status AS ref2 ON upper(rtrim(stg.taskstate)) = upper(rtrim(ref2.status_desc))
   AND upper(rtrim(ref2.status_type_desc)) = 'TASK'
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_survivorship_plan_task.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_survivorship_plan_task) ) AS ms ON mt.nav_survivorship_plan_task_sid = ms.nav_survivorship_plan_task_sid
AND (coalesce(mt.task_status_id, 0) = coalesce(ms.status_id, 0)
     AND coalesce(mt.task_status_id, 1) = coalesce(ms.status_id, 1))
AND (coalesce(mt.contact_method_id, 0) = coalesce(ms.contact_method_id, 0)
     AND coalesce(mt.contact_method_id, 1) = coalesce(ms.contact_method_id, 1))
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (upper(coalesce(mt.task_desc_text, '0')) = upper(coalesce(ms.task_desc_text, '0'))
     AND upper(coalesce(mt.task_desc_text, '1')) = upper(coalesce(ms.task_desc_text, '1')))
AND (coalesce(mt.task_resolution_date, DATE '1970-01-01') = coalesce(ms.task_resolution_date, DATE '1970-01-01')
     AND coalesce(mt.task_resolution_date, DATE '1970-01-02') = coalesce(ms.task_resolution_date, DATE '1970-01-02'))
AND (coalesce(mt.task_closed_date, DATE '1970-01-01') = coalesce(ms.task_closed_date, DATE '1970-01-01')
     AND coalesce(mt.task_closed_date, DATE '1970-01-02') = coalesce(ms.task_closed_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.contact_result_text, '0')) = upper(coalesce(ms.contact_result_text, '0'))
     AND upper(coalesce(mt.contact_result_text, '1')) = upper(coalesce(ms.contact_result_text, '1')))
AND (coalesce(mt.contact_date, DATE '1970-01-01') = coalesce(ms.contact_date, DATE '1970-01-01')
     AND coalesce(mt.contact_date, DATE '1970-01-02') = coalesce(ms.contact_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
     AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (nav_survivorship_plan_task_sid,
        task_status_id,
        contact_method_id,
        nav_patient_id,
        navigator_id,
        coid,
        company_code,
        task_desc_text,
        task_resolution_date,
        task_closed_date,
        contact_result_text,
        contact_date,
        comment_text,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.nav_survivorship_plan_task_sid, ms.status_id, ms.contact_method_id, ms.nav_patient_id, ms.navigator_id, ms.coid, ms.company_code, ms.task_desc_text, ms.task_resolution_date, ms.task_closed_date, ms.contact_result_text, ms.contact_date, ms.comment_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT nav_survivorship_plan_task_sid
      FROM {{ params.param_cr_core_dataset_name }}.cn_patient_survivorship_plan_task
      GROUP BY nav_survivorship_plan_task_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cn_patient_survivorship_plan_task');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_Patient_Survivorship_Plan_Task');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF