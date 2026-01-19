DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/ref_procedure_type.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.REF_PROCEDURE_TYPE                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :"EDWCR_staging.Contact_Purpose_stg     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_REF_PROCEDURE_TYPE;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Contact_Purpose_stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.ref_procedure_type AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(trim(type_stg.procedure_type_desc))) +
     (SELECT coalesce(max(ref_procedure_type.procedure_type_id), 0) AS id1
      FROM {{ params.param_cr_base_views_dataset_name }}.ref_procedure_type) AS procedure_type_id,
                                     substr(trim(type_stg.procedure_type_desc), 1, 50) AS procedure_type_desc,
                                     substr(trim(type_stg.procedure_sub_type_desc), 1, 100) AS procedure_sub_type_desc,
                                     type_stg.source_system_code AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT procedure_type_stg.procedure_type_desc,
             procedure_type_stg.procedure_sub_type_desc,
             procedure_type_stg.source_system_code
      FROM {{ params.param_cr_stage_dataset_name }}.procedure_type_stg
      WHERE (upper(trim(procedure_type_stg.procedure_type_desc)),
             upper(trim(coalesce(procedure_type_stg.procedure_sub_type_desc, '')))) NOT IN
          (SELECT AS STRUCT upper(trim(ref_procedure_type.procedure_type_desc)),
                            upper(trim(coalesce(ref_procedure_type.procedure_sub_type_desc, '')))
           FROM {{ params.param_cr_base_views_dataset_name }}.ref_procedure_type
           WHERE ref_procedure_type.procedure_type_desc IS NOT NULL ) ) AS type_stg
   WHERE type_stg.procedure_type_desc IS NOT NULL
     AND upper(trim(type_stg.procedure_type_desc)) <> '' ) AS ms ON mt.procedure_type_id = ms.procedure_type_id
AND (upper(coalesce(mt.procedure_type_desc, '0')) = upper(coalesce(ms.procedure_type_desc, '0'))
     AND upper(coalesce(mt.procedure_type_desc, '1')) = upper(coalesce(ms.procedure_type_desc, '1')))
AND (upper(coalesce(mt.procedure_sub_type_desc, '0')) = upper(coalesce(ms.procedure_sub_type_desc, '0'))
     AND upper(coalesce(mt.procedure_sub_type_desc, '1')) = upper(coalesce(ms.procedure_sub_type_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (procedure_type_id,
        procedure_type_desc,
        procedure_sub_type_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.procedure_type_id, ms.procedure_type_desc, ms.procedure_sub_type_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT procedure_type_id
      FROM {{ params.param_cr_core_dataset_name }}.ref_procedure_type
      GROUP BY procedure_type_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_procedure_type');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','REF_PROCEDURE_TYPE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF