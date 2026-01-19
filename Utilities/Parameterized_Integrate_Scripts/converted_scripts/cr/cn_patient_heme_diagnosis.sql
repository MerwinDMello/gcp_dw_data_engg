-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_heme_diagnosis.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
-- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CN_PATIENT_HEME_DIAGNOSIS                          ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   :       EDWCR_staging.stg_PatientHemeDiagnosis     		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;
-- SET QUERY_BAND = 'App=EDWCR_ETL; Job=J_CN_PATIENT_HEME_DIAGNOSIS;' FOR SESSION;
-- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_PatientHemeDiagnosis');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_diagnosis AS mt USING (
    SELECT DISTINCT
        iq.cn_patient_heme_diagnosis_sid AS cn_patient_heme_diagnosis_sid,
        CAST(iq.nav_patient_id as NUMERIC) AS nav_patient_id,
        iq.disease_status_id,
        iq.tumor_type_id AS tumor_type_id,
        iq.diagnosis_result_id AS diagnosis_result_id,
        iq.nav_diagnosis_id AS nav_diagnosis_id,
        iq.navigator_id AS navigator_id,
        iq.coid AS coid,
        iq.company_code AS company_code,
        iq.speciman_date,
        substr(iq.disease_diagnosis_text, 1, 100) AS disease_diagnosis_text,
        iq.therapy_related_ind AS therapy_related_ind,
        iq.transformed_from_mds_ind AS transformed_from_mds_ind,
        substr(iq.mipi_text, 1, 20) AS mipi_text,
        substr(iq.ipi_text, 1, 20) AS ipi_text,
        substr(iq.flipi_text, 1, 20) AS flipi_text,
        iq.aids_related_ind AS aids_related_ind,
        substr(iq.comment_text, 1, 500) AS comment_text,
        substr(iq.classification_text, 1, 100) AS classification_text,
        substr(iq.sub_classification_text, 1, 100) AS sub_classification_text,
        substr(iq.nhl_type_text, 1, 50) AS nhl_type_text,
        substr(iq.other_nhl_type_text, 1, 50) AS other_nhl_type_text,
        substr(iq.transformed_disease_text, 1, 100) AS transformed_disease_text,
        substr(iq.non_malignant_type_text, 1, 100) AS non_malignant_type_text,
        substr(iq.feature_text, 1, 50) AS feature_text,
        substr(iq.risk_category_text, 1, 20) AS risk_category_text,
        substr(iq.mds_disease_risk_text, 1, 20) AS mds_disease_risk_text,
        substr(iq.staging_field_1_text, 1, 10) AS staging_field_1_text,
        substr(iq.staging_field_2_text, 1, 10) AS staging_field_2_text,
        substr(iq.hashbite_ssk, 1, 60) AS hashbite_ssk,
        iq.source_system_code AS source_system_code,
        iq.dw_last_update_date_time
      FROM
        (
          SELECT
              phd.patienthemediagnosisfactid AS cn_patient_heme_diagnosis_sid,
              phd.patientdimid AS nav_patient_id,
              rs.status_id AS disease_status_id,
              phd.tumortypedimid AS tumor_type_id,
              phd.diagnosisresultid AS diagnosis_result_id,
              phd.diagnosisdimid AS nav_diagnosis_id,
              phd.navigatordimid AS navigator_id,
              phd.coid AS coid,
              'H' AS company_code,
              parse_date('%Y-%m-%d', trim(phd.specimandate)) AS speciman_date,
              trim(phd.diseasediagnosis) AS disease_diagnosis_text,
              CASE
                 upper(trim(phd.therapyrelated))
                WHEN 'YES' THEN 'Y'
                WHEN 'NO' THEN 'N'
                WHEN 'UNKNOWN' THEN 'U'
                ELSE trim(phd.therapyrelated)
              END AS therapy_related_ind,
              CASE
                WHEN upper(trim(phd.transformedfrommds)) = 'TRANSFORMED FROM MDS' THEN 'Y'
                ELSE 'N'
              END AS transformed_from_mds_ind,
              trim(phd.mipi) AS mipi_text,
              trim(phd.ipi) AS ipi_text,
              trim(phd.flipi) AS flipi_text,
              CASE
                WHEN upper(trim(phd.aidsrelated)) = 'AIDS RELATED' THEN 'Y'
                ELSE 'N'
              END AS aids_related_ind,
              trim(phd.comments) AS comment_text,
              trim(phd.classification) AS classification_text,
              trim(phd.subclassification) AS sub_classification_text,
              trim(phd.nhltype) AS nhl_type_text,
              trim(phd.othernhltype) AS other_nhl_type_text,
              trim(phd.transformeddisease) AS transformed_disease_text,
              trim(phd.nonmalignanttype) AS non_malignant_type_text,
              trim(phd.features) AS feature_text,
              trim(phd.risk) AS risk_category_text,
              trim(phd.riskripss) AS mds_disease_risk_text,
              trim(phd.stagingfield1) AS staging_field_1_text,
              trim(phd.stagingfield2) AS staging_field_2_text,
              trim(phd.hbsource) AS hashbite_ssk,
              'N' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_staging.stg_patienthemediagnosis AS phd
              LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status AS rs ON upper(trim(phd.diseasestatus)) = upper(trim(rs.status_desc))
               AND upper(trim(rs.status_type_desc)) = 'DISEASE'
        ) AS iq
        LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_heme_diagnosis AS cphd ON upper(trim(iq.hashbite_ssk)) = upper(trim(cphd.hashbite_ssk))
      WHERE trim(cphd.hashbite_ssk) IS NULL
  ) AS ms
  ON mt.cn_patient_heme_diagnosis_sid = ms.cn_patient_heme_diagnosis_sid
   AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
   AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
   AND (coalesce(mt.disease_status_id, 0) = coalesce(ms.disease_status_id, 0)
   AND coalesce(mt.disease_status_id, 1) = coalesce(ms.disease_status_id, 1))
   AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
   AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
   AND (coalesce(mt.diagnosis_result_id, 0) = coalesce(ms.diagnosis_result_id, 0)
   AND coalesce(mt.diagnosis_result_id, 1) = coalesce(ms.diagnosis_result_id, 1))
   AND (coalesce(mt.nav_diagnosis_id, 0) = coalesce(ms.nav_diagnosis_id, 0)
   AND coalesce(mt.nav_diagnosis_id, 1) = coalesce(ms.nav_diagnosis_id, 1))
   AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
   AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
   AND mt.coid = ms.coid
   AND mt.company_code = ms.company_code
   AND (coalesce(mt.speciman_date, DATE '1970-01-01') = coalesce(ms.speciman_date, DATE '1970-01-01')
   AND coalesce(mt.speciman_date, DATE '1970-01-02') = coalesce(ms.speciman_date, DATE '1970-01-02'))
   AND (upper(coalesce(mt.disease_diagnosis_text, '0')) = upper(coalesce(ms.disease_diagnosis_text, '0'))
   AND upper(coalesce(mt.disease_diagnosis_text, '1')) = upper(coalesce(ms.disease_diagnosis_text, '1')))
   AND (upper(coalesce(mt.therapy_related_ind, '0')) = upper(coalesce(ms.therapy_related_ind, '0'))
   AND upper(coalesce(mt.therapy_related_ind, '1')) = upper(coalesce(ms.therapy_related_ind, '1')))
   AND (upper(coalesce(mt.transformed_from_mds_ind, '0')) = upper(coalesce(ms.transformed_from_mds_ind, '0'))
   AND upper(coalesce(mt.transformed_from_mds_ind, '1')) = upper(coalesce(ms.transformed_from_mds_ind, '1')))
   AND (upper(coalesce(mt.mipi_text, '0')) = upper(coalesce(ms.mipi_text, '0'))
   AND upper(coalesce(mt.mipi_text, '1')) = upper(coalesce(ms.mipi_text, '1')))
   AND (upper(coalesce(mt.ipi_text, '0')) = upper(coalesce(ms.ipi_text, '0'))
   AND upper(coalesce(mt.ipi_text, '1')) = upper(coalesce(ms.ipi_text, '1')))
   AND (upper(coalesce(mt.flipi_text, '0')) = upper(coalesce(ms.flipi_text, '0'))
   AND upper(coalesce(mt.flipi_text, '1')) = upper(coalesce(ms.flipi_text, '1')))
   AND (upper(coalesce(mt.aids_related_ind, '0')) = upper(coalesce(ms.aids_related_ind, '0'))
   AND upper(coalesce(mt.aids_related_ind, '1')) = upper(coalesce(ms.aids_related_ind, '1')))
   AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
   AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
   AND (upper(coalesce(mt.classification_text, '0')) = upper(coalesce(ms.classification_text, '0'))
   AND upper(coalesce(mt.classification_text, '1')) = upper(coalesce(ms.classification_text, '1')))
   AND (upper(coalesce(mt.sub_classification_text, '0')) = upper(coalesce(ms.sub_classification_text, '0'))
   AND upper(coalesce(mt.sub_classification_text, '1')) = upper(coalesce(ms.sub_classification_text, '1')))
   AND (upper(coalesce(mt.nhl_type_text, '0')) = upper(coalesce(ms.nhl_type_text, '0'))
   AND upper(coalesce(mt.nhl_type_text, '1')) = upper(coalesce(ms.nhl_type_text, '1')))
   AND (upper(coalesce(mt.other_nhl_type_text, '0')) = upper(coalesce(ms.other_nhl_type_text, '0'))
   AND upper(coalesce(mt.other_nhl_type_text, '1')) = upper(coalesce(ms.other_nhl_type_text, '1')))
   AND (upper(coalesce(mt.transformed_disease_text, '0')) = upper(coalesce(ms.transformed_disease_text, '0'))
   AND upper(coalesce(mt.transformed_disease_text, '1')) = upper(coalesce(ms.transformed_disease_text, '1')))
   AND (upper(coalesce(mt.non_malignant_type_text, '0')) = upper(coalesce(ms.non_malignant_type_text, '0'))
   AND upper(coalesce(mt.non_malignant_type_text, '1')) = upper(coalesce(ms.non_malignant_type_text, '1')))
   AND (upper(coalesce(mt.feature_text, '0')) = upper(coalesce(ms.feature_text, '0'))
   AND upper(coalesce(mt.feature_text, '1')) = upper(coalesce(ms.feature_text, '1')))
   AND (upper(coalesce(mt.risk_category_text, '0')) = upper(coalesce(ms.risk_category_text, '0'))
   AND upper(coalesce(mt.risk_category_text, '1')) = upper(coalesce(ms.risk_category_text, '1')))
   AND (upper(coalesce(mt.mds_disease_risk_text, '0')) = upper(coalesce(ms.mds_disease_risk_text, '0'))
   AND upper(coalesce(mt.mds_disease_risk_text, '1')) = upper(coalesce(ms.mds_disease_risk_text, '1')))
   AND (upper(coalesce(mt.staging_field_1_text, '0')) = upper(coalesce(ms.staging_field_1_text, '0'))
   AND upper(coalesce(mt.staging_field_1_text, '1')) = upper(coalesce(ms.staging_field_1_text, '1')))
   AND (upper(coalesce(mt.staging_field_2_text, '0')) = upper(coalesce(ms.staging_field_2_text, '0'))
   AND upper(coalesce(mt.staging_field_2_text, '1')) = upper(coalesce(ms.staging_field_2_text, '1')))
   AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
   AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
   AND mt.source_system_code = ms.source_system_code
   AND mt.dw_last_update_date_time = ms.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN
      INSERT (cn_patient_heme_diagnosis_sid, nav_patient_id, disease_status_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id, navigator_id, coid, company_code, speciman_date, disease_diagnosis_text, therapy_related_ind, transformed_from_mds_ind, mipi_text, ipi_text, flipi_text, aids_related_ind, comment_text, classification_text, sub_classification_text, nhl_type_text, other_nhl_type_text, transformed_disease_text, non_malignant_type_text, feature_text, risk_category_text, mds_disease_risk_text, staging_field_1_text, staging_field_2_text, hashbite_ssk, source_system_code, dw_last_update_date_time)
      VALUES (ms.cn_patient_heme_diagnosis_sid, ms.nav_patient_id, ms.disease_status_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.speciman_date, ms.disease_diagnosis_text, ms.therapy_related_ind, ms.transformed_from_mds_ind, ms.mipi_text, ms.ipi_text, ms.flipi_text, ms.aids_related_ind, ms.comment_text, ms.classification_text, ms.sub_classification_text, ms.nhl_type_text, ms.other_nhl_type_text, ms.transformed_disease_text, ms.non_malignant_type_text, ms.feature_text, ms.risk_category_text, ms.mds_disease_risk_text, ms.staging_field_1_text, ms.staging_field_2_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time)
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- Deleting Records From Core table if matching Hashbite_SSK not coming from source
BEGIN
  SET _ERROR_CODE = 0;
  DELETE FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_diagnosis AS cphd WHERE NOT EXISTS (
    SELECT
        'X'
      FROM
        `hca-hin-dev-cur-ops`.edwcr_staging.stg_patienthemediagnosis AS sphd
      WHERE upper(trim(sphd.hbsource)) = upper(trim(cphd.hashbite_ssk))
  );
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_Patient_Heme_Diagnosis');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- EOF
