DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/fact_rad_onc_patient_toxicity.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Fact_Rad_Onc_Patient_Toxicity                        	##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCR_Staging.stg_FACTPATIENTTOXICITY 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_Fact_Rad_Onc_Patient_Toxicity;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','stg_FACTPATIENTTOXICITY');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_patient_toxicity;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_patient_toxicity AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY dp.dimsiteid,
                                               dp.factpatienttoxicityid DESC) AS fact_patient_toxicity_sk,
                                     tc.toxicity_component_sk AS toxicity_component_sk,
                                     ta.toxicity_assessment_type_sk AS toxicity_assessment_type_sk,
                                     rat.activity_transaction_sk AS activity_transaction_sk,
                                     rp.patient_sk AS patient_sk,
                                     dp.dimlookupid_scheme AS scheme_id,
                                     dp.dimlookupid_toxctycsecrtntytyp AS toxicity_cause_certainty_type_id,
                                     dp.dimlookupid_toxicitycausetype AS toxicity_cause_type_id,
                                     dp.dimdiagnosiscodeid AS diagnosis_code_sk,
                                     rr.site_sk AS site_sk,
                                     dp.factpatienttoxicityid AS source_fact_patient_toxicity_id,
                                     CAST(trim(substr(dp.assessmentdatetime, 1, 19)) AS DATETIME) AS assessment_date_time,
                                     CAST(trim(substr(dp.toxicityeffectivedate, 1, 10)) AS DATE) AS toxicity_effective_date,
                                     dp.toxicitygrade AS toxicity_grade_num,
                                     dp.validentryindicator AS valid_entry_ind,
                                     CAST(trim(substr(dp.toxicityapproveddatetime, 1, 19)) AS DATETIME) AS toxicity_approved_date_time,
                                     CAST(trim(substr(dp.assessmentperformeddatetime, 1, 19)) AS DATETIME) AS assessment_performed_date_time,
                                     substr(dp.toxicityreason, 1, 1000) AS toxicity_reason_text,
                                     dp.toxicityapprovedindicator AS toxicity_approved_ind,
                                     dp.toxctyheadervalidetryindicator AS toxicity_header_valid_entry_ind,
                                     dp.revisionnumber AS revision_num,
                                     dp.logid AS log_id,
                                     dp.runid AS run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT stg_factpatienttoxicity.toxicitycomponentname,
             stg_factpatienttoxicity.toxicityassessmenttype,
             stg_factpatienttoxicity.dimactivitytransactionid,
             stg_factpatienttoxicity.dimpatientid,
             stg_factpatienttoxicity.dimlookupid_scheme,
             stg_factpatienttoxicity.dimlookupid_toxctycsecrtntytyp,
             stg_factpatienttoxicity.dimlookupid_toxicitycausetype,
             stg_factpatienttoxicity.dimdiagnosiscodeid,
             stg_factpatienttoxicity.dimsiteid,
             stg_factpatienttoxicity.factpatienttoxicityid,
             stg_factpatienttoxicity.assessmentdatetime,
             stg_factpatienttoxicity.toxicityeffectivedate,
             stg_factpatienttoxicity.toxicitygrade,
             stg_factpatienttoxicity.validentryindicator,
             stg_factpatienttoxicity.toxicityapproveddatetime,
             stg_factpatienttoxicity.assessmentperformeddatetime,
             trim(stg_factpatienttoxicity.toxicityreason) AS toxicityreason,
             stg_factpatienttoxicity.toxicityapprovedindicator,
             stg_factpatienttoxicity.toxctyheadervalidetryindicator,
             stg_factpatienttoxicity.revisionnumber,
             stg_factpatienttoxicity.logid,
             stg_factpatienttoxicity.runid
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_factpatienttoxicity) AS dp
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_site) AS rr ON dp.dimsiteid = rr.source_site_id
   LEFT OUTER JOIN
     (SELECT ref_rad_onc_toxicity_component.toxicity_component_name,
             ref_rad_onc_toxicity_component.toxicity_component_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_toxicity_component) AS tc ON upper(rtrim(dp.toxicitycomponentname)) = upper(rtrim(tc.toxicity_component_name))
   LEFT OUTER JOIN
     (SELECT ref_rad_onc_toxicity_assessment_type.toxicity_assessment_type_desc,
             ref_rad_onc_toxicity_assessment_type.toxicity_assessment_type_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_rad_onc_toxicity_assessment_type) AS ta ON upper(rtrim(dp.toxicityassessmenttype)) = upper(rtrim(ta.toxicity_assessment_type_desc))
   LEFT OUTER JOIN
     (SELECT rad_onc_activity_transaction.source_activity_transaction_id,
             rad_onc_activity_transaction.activity_transaction_sk,
             rad_onc_activity_transaction.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_activity_transaction) AS rat ON dp.dimactivitytransactionid = rat.source_activity_transaction_id
   AND rr.site_sk = rat.site_sk
   LEFT OUTER JOIN
     (SELECT rad_onc_patient.source_patient_id,
             rad_onc_patient.patient_sk,
             rad_onc_patient.site_sk
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.rad_onc_patient) AS rp ON dp.dimpatientid = rp.source_patient_id
   AND rr.site_sk = rp.site_sk) AS ms ON mt.fact_patient_toxicity_sk = ms.fact_patient_toxicity_sk
AND mt.toxicity_component_sk = ms.toxicity_component_sk
AND mt.toxicity_assessment_type_sk = ms.toxicity_assessment_type_sk
AND (coalesce(mt.activity_transaction_sk, 0) = coalesce(ms.activity_transaction_sk, 0)
     AND coalesce(mt.activity_transaction_sk, 1) = coalesce(ms.activity_transaction_sk, 1))
AND (coalesce(mt.patient_sk, 0) = coalesce(ms.patient_sk, 0)
     AND coalesce(mt.patient_sk, 1) = coalesce(ms.patient_sk, 1))
AND (coalesce(mt.scheme_id, 0) = coalesce(ms.scheme_id, 0)
     AND coalesce(mt.scheme_id, 1) = coalesce(ms.scheme_id, 1))
AND (coalesce(mt.toxicity_cause_certainty_type_id, 0) = coalesce(ms.toxicity_cause_certainty_type_id, 0)
     AND coalesce(mt.toxicity_cause_certainty_type_id, 1) = coalesce(ms.toxicity_cause_certainty_type_id, 1))
AND (coalesce(mt.toxicity_cause_type_id, 0) = coalesce(ms.toxicity_cause_type_id, 0)
     AND coalesce(mt.toxicity_cause_type_id, 1) = coalesce(ms.toxicity_cause_type_id, 1))
AND mt.diagnosis_code_sk = ms.diagnosis_code_sk
AND mt.site_sk = ms.site_sk
AND mt.source_fact_patient_toxicity_id = ms.source_fact_patient_toxicity_id
AND (coalesce(mt.assessment_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.assessment_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.assessment_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.assessment_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.toxicity_effective_date, DATE '1970-01-01') = coalesce(ms.toxicity_effective_date, DATE '1970-01-01')
     AND coalesce(mt.toxicity_effective_date, DATE '1970-01-02') = coalesce(ms.toxicity_effective_date, DATE '1970-01-02'))
AND (coalesce(mt.toxicity_grade_num, 0) = coalesce(ms.toxicity_grade_num, 0)
     AND coalesce(mt.toxicity_grade_num, 1) = coalesce(ms.toxicity_grade_num, 1))
AND (upper(coalesce(mt.valid_entry_ind, '0')) = upper(coalesce(ms.valid_entry_ind, '0'))
     AND upper(coalesce(mt.valid_entry_ind, '1')) = upper(coalesce(ms.valid_entry_ind, '1')))
AND (coalesce(mt.toxicity_approved_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.toxicity_approved_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.toxicity_approved_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.toxicity_approved_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.assessment_performed_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.assessment_performed_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.assessment_performed_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.assessment_performed_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.toxicity_reason_text, '0')) = upper(coalesce(ms.toxicity_reason_text, '0'))
     AND upper(coalesce(mt.toxicity_reason_text, '1')) = upper(coalesce(ms.toxicity_reason_text, '1')))
AND (upper(coalesce(mt.toxicity_approved_ind, '0')) = upper(coalesce(ms.toxicity_approved_ind, '0'))
     AND upper(coalesce(mt.toxicity_approved_ind, '1')) = upper(coalesce(ms.toxicity_approved_ind, '1')))
AND (upper(coalesce(mt.toxicity_header_valid_entry_ind, '0')) = upper(coalesce(ms.toxicity_header_valid_entry_ind, '0'))
     AND upper(coalesce(mt.toxicity_header_valid_entry_ind, '1')) = upper(coalesce(ms.toxicity_header_valid_entry_ind, '1')))
AND (coalesce(mt.revision_num, 0) = coalesce(ms.revision_num, 0)
     AND coalesce(mt.revision_num, 1) = coalesce(ms.revision_num, 1))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (fact_patient_toxicity_sk,
        toxicity_component_sk,
        toxicity_assessment_type_sk,
        activity_transaction_sk,
        patient_sk,
        scheme_id,
        toxicity_cause_certainty_type_id,
        toxicity_cause_type_id,
        diagnosis_code_sk,
        site_sk,
        source_fact_patient_toxicity_id,
        assessment_date_time,
        toxicity_effective_date,
        toxicity_grade_num,
        valid_entry_ind,
        toxicity_approved_date_time,
        assessment_performed_date_time,
        toxicity_reason_text,
        toxicity_approved_ind,
        toxicity_header_valid_entry_ind,
        revision_num,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.fact_patient_toxicity_sk, ms.toxicity_component_sk, ms.toxicity_assessment_type_sk, ms.activity_transaction_sk, ms.patient_sk, ms.scheme_id, ms.toxicity_cause_certainty_type_id, ms.toxicity_cause_type_id, ms.diagnosis_code_sk, ms.site_sk, ms.source_fact_patient_toxicity_id, ms.assessment_date_time, ms.toxicity_effective_date, ms.toxicity_grade_num, ms.valid_entry_ind, ms.toxicity_approved_date_time, ms.assessment_performed_date_time, ms.toxicity_reason_text, ms.toxicity_approved_ind, ms.toxicity_header_valid_entry_ind, ms.revision_num, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT fact_patient_toxicity_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_patient_toxicity
      GROUP BY fact_patient_toxicity_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.fact_rad_onc_patient_toxicity');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Fact_Rad_Onc_Patient_Toxicity');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF