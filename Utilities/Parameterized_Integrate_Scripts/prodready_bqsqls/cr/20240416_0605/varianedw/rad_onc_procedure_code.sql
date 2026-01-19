DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/rad_onc_procedure_code.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Ref_Rad_Onc_Procedure_Code                   ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.stg_DimProcedureCode 				##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_Rad_Onc_Procedure_code;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_Staging','stg_DimProcedureCode');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_procedure_code;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.ref_rad_onc_procedure_code AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY dp.dimsiteid,
                                               dp.dimprocedurecodeid) AS procedure_code_sk,
                                     rtt.treatment_type_sk,
                                     rr.site_sk,
                                     dp.dimprocedurecodeid,
                                     dp.procedurecode AS procedure_code,
                                     substr(dp.description, 1, 65) AS procedure_short_desc,
                                     substr(dp.procedurecodedescription, 1, 255) AS procedure_long_desc,
                                     dp.activeind AS active_ind,
                                     dp.log_id,
                                     dp.run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(stg_dimprocedurecode.dimsiteid) AS INT64) AS dimsiteid,
             CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(stg_dimprocedurecode.dimprocedurecodeid) AS INT64) AS dimprocedurecodeid,
             stg_dimprocedurecode.procedurecode,
             stg_dimprocedurecode.description,
             stg_dimprocedurecode.procedurecodedescription,
             stg_dimprocedurecode.activeind,
             CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(stg_dimprocedurecode.logid) AS INT64) AS log_id,
             CAST({{ params.param_cr_bqutil_fns_dataset_name }}.cw_td_normalize_number(stg_dimprocedurecode.runid) AS INT64) AS run_id
      FROM {{ params.param_cr_stage_dataset_name }}.stg_dimprocedurecode) AS dp
   LEFT OUTER JOIN
     (SELECT DISTINCT stg_sc_modalities.treatment_type,
                      stg_sc_modalities.treatment_category,
                      stg_sc_modalities.procedure_code
      FROM {{ params.param_cr_stage_dataset_name }}.stg_sc_modalities) AS sm ON upper(rtrim(sm.procedure_code)) = upper(rtrim(dp.procedurecode))
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_treatment_type AS rtt ON upper(rtrim(sm.treatment_category)) = upper(rtrim(rtt.treatment_category_desc))
   AND upper(rtrim(sm.treatment_type)) = upper(rtrim(rtt.treatment_type_desc))
   INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_site AS rr ON dp.dimsiteid = rr.source_site_id) AS ms ON mt.procedure_code_sk = ms.procedure_code_sk
AND (coalesce(mt.treatment_type_sk, 0) = coalesce(ms.treatment_type_sk, 0)
     AND coalesce(mt.treatment_type_sk, 1) = coalesce(ms.treatment_type_sk, 1))
AND mt.site_sk = ms.site_sk
AND mt.source_procedure_code_id = ms.dimprocedurecodeid
AND (upper(coalesce(mt.procedure_code, '0')) = upper(coalesce(ms.procedure_code, '0'))
     AND upper(coalesce(mt.procedure_code, '1')) = upper(coalesce(ms.procedure_code, '1')))
AND (upper(coalesce(mt.procedure_short_desc, '0')) = upper(coalesce(ms.procedure_short_desc, '0'))
     AND upper(coalesce(mt.procedure_short_desc, '1')) = upper(coalesce(ms.procedure_short_desc, '1')))
AND (upper(coalesce(mt.procedure_long_desc, '0')) = upper(coalesce(ms.procedure_long_desc, '0'))
     AND upper(coalesce(mt.procedure_long_desc, '1')) = upper(coalesce(ms.procedure_long_desc, '1')))
AND (upper(coalesce(mt.active_ind, '0')) = upper(coalesce(ms.active_ind, '0'))
     AND upper(coalesce(mt.active_ind, '1')) = upper(coalesce(ms.active_ind, '1')))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (procedure_code_sk,
        treatment_type_sk,
        site_sk,
        source_procedure_code_id,
        procedure_code,
        procedure_short_desc,
        procedure_long_desc,
        active_ind,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.procedure_code_sk, ms.treatment_type_sk, ms.site_sk, ms.dimprocedurecodeid, ms.procedure_code, ms.procedure_short_desc, ms.procedure_long_desc, ms.active_ind, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT procedure_code_sk
      FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_procedure_code
      GROUP BY procedure_code_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.ref_rad_onc_procedure_code');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Ref_Rad_Onc_Procedure_Code');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF