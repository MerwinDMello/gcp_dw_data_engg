DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_cr_ref_pathway_var_reason.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE			: 	EDWCR.Ref_Pathway_Var_Reason             		#
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
 --Job=J_CR_REF_PATHWAY_VAR_REASON;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Patient_Heme_Treatment_Regimen_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert data into Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.ref_pathway_var_reason AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(dis.pathway_var_reason_type_desc),
                                               upper(dis.pathway_var_reason_sub_type_desc)) + coalesce(
                                                                                                         (SELECT max(coalesce(a.pathway_var_reason_id, 0)) AS max_key
                                                                                                          FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_pathway_var_reason AS a), 0) AS pathway_var_reason_id,
                                     substr(trim(dis.pathway_var_reason_type_desc), 1, 30) AS pathway_var_reason_type_desc,
                                     substr(trim(dis.pathway_var_reason_sub_type_desc), 1, 50) AS pathway_var_reason_sub_type_desc,
                                     'N' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT DISTINCT trim(st.pathwayvariancereason) AS pathway_var_reason_type_desc,
                      trim(st.otherpathwayreason) AS pathway_var_reason_sub_type_desc
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_treatment_regimen_stg AS st
      WHERE st.pathwayvariancereason IS NOT NULL
        OR st.otherpathwayreason IS NOT NULL ) AS dis
   WHERE
       (SELECT logical_and(NOT ((upper(trim(rcdc.pathway_var_reason_type_desc)) = upper(trim(dis.pathway_var_reason_type_desc))
                                 OR upper(trim(coalesce(rcdc.pathway_var_reason_sub_type_desc, '##'))) = upper(trim(coalesce(dis.pathway_var_reason_sub_type_desc, '##'))))))
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_pathway_var_reason AS rcdc) ) AS ms ON mt.pathway_var_reason_id = ms.pathway_var_reason_id
AND (upper(coalesce(mt.pathway_var_reason_type_desc, '0')) = upper(coalesce(ms.pathway_var_reason_type_desc, '0'))
     AND upper(coalesce(mt.pathway_var_reason_type_desc, '1')) = upper(coalesce(ms.pathway_var_reason_type_desc, '1')))
AND (upper(coalesce(mt.pathway_var_reason_sub_type_desc, '0')) = upper(coalesce(ms.pathway_var_reason_sub_type_desc, '0'))
     AND upper(coalesce(mt.pathway_var_reason_sub_type_desc, '1')) = upper(coalesce(ms.pathway_var_reason_sub_type_desc, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (pathway_var_reason_id,
        pathway_var_reason_type_desc,
        pathway_var_reason_sub_type_desc,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.pathway_var_reason_id, ms.pathway_var_reason_type_desc, ms.pathway_var_reason_sub_type_desc, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT pathway_var_reason_id
      FROM `hca-hin-dev-cur-ops`.edwcr.ref_pathway_var_reason
      GROUP BY pathway_var_reason_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.ref_pathway_var_reason');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Collect Stats on Core Table */ -- CALL dbadmin_procs.collect_stats_table ('edwcr','Ref_Pathway_Var_Reason');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

RETURN;

---- EOF;;