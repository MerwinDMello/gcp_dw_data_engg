-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_patient_tumor_pathology_result.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CR_PATIENT_TUMOR_PATHOLOGY_RESULT       		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CR_PAT_TUMOR_PATHOLOGY_RESULT_STG	#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CR_PATIENT_TUMOR_PATHOLOGY_RESULT;' FOR SESSION;
/* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CR_PAT_TUMOR_PATHOLOGY_RESULT_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Truncate Core Table */
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor_pathology_result;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Populate Core Table */
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cr_patient_tumor_pathology_result AS mt USING (
    SELECT DISTINCT
        stg.patientid AS cr_patient_id,
        stg.tumorid AS tumor_id,
        coalesce(lkp.master_lookup_sid, (
          SELECT
              rlc.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
              INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
            WHERE upper(rtrim(rln.lookup_name)) = 'NODES EXAMINED'
             AND upper(rtrim(rlc.lookup_code)) = '-99'
        )) AS nodes_examined_id,
        coalesce(lkp1.master_lookup_sid, (
          SELECT
              rlc1.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln1
              INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc1 ON rlc1.lookup_id = rln1.lookup_sid
            WHERE upper(rtrim(rln1.lookup_name)) = 'NODES POSITIVE'
             AND upper(rtrim(rlc1.lookup_code)) = '-99'
        )) AS positive_node_id,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.cr_pat_tumor_pathology_result_stg AS stg
        LEFT OUTER JOIN (
          SELECT
              rlc.lookup_code,
              rlc.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
              INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
            WHERE upper(rtrim(rln.lookup_name)) = 'NODES EXAMINED'
        ) AS lkp ON upper(rtrim(stg.nodesexamined)) = upper(rtrim(lkp.lookup_code))
        LEFT OUTER JOIN (
          SELECT
              rlc.lookup_code,
              rlc.master_lookup_sid
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_lookup_name AS rln
              INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_lookup_code AS rlc ON rlc.lookup_id = rln.lookup_sid
            WHERE upper(rtrim(rln.lookup_name)) = 'NODES POSITIVE'
        ) AS lkp1 ON upper(rtrim(stg.nodespositive)) = upper(rtrim(lkp1.lookup_code))
  ) AS ms
  ON mt.cr_patient_id = ms.cr_patient_id
   AND mt.tumor_id = ms.tumor_id
   AND (coalesce(mt.nodes_examined_id, 0) = coalesce(ms.nodes_examined_id, 0)
   AND coalesce(mt.nodes_examined_id, 1) = coalesce(ms.nodes_examined_id, 1))
   AND (coalesce(mt.positive_node_id, 0) = coalesce(ms.positive_node_id, 0)
   AND coalesce(mt.positive_node_id, 1) = coalesce(ms.positive_node_id, 1))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cr_patient_id, tumor_id, nodes_examined_id, positive_node_id, source_system_code, dw_last_update_date_time)
      VALUES (ms.cr_patient_id, ms.tumor_id, ms.nodes_examined_id, ms.positive_node_id, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CR_PATIENT_TUMOR_PATHOLOGY_RESULT');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
