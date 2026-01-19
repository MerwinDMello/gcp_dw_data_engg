DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/fact_cancer_patient_financial.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Fact_Cancer_Patient_Financial                   ##
-- ##  TARGET  DATABASE	   : EDWCR	 											##
-- ##  SOURCE		  		   : EDWCP.Consolidated_Patient_Encounter 						##
-- ##														  						##
-- ##	                                                                        	##
-- ##  INITIAL RELEASE	   : 														##
-- ##  PROJECT            	   : 	 		    									##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CR_RO_Fact_Cancer_Patient_Financial;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('EDWCP','Consolidated_Patient_Encounter');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Data
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.fact_cancer_patient_financial;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.fact_cancer_patient_financial AS mt USING
  (SELECT DISTINCT cpe.encounter_id AS encounter_id,
                   trim(cpe.encounter_source_system_code) AS encounter_source_system_code,
                   substr(trim(cpe.empi_text), 1, 80) AS empi_text,
                   substr(trim(cpe.pat_acct_num), 1, 20) AS pat_acct_num,
                   substr(trim(cpe.medical_record_num), 1, 18) AS medical_record_num,
                   substr(trim(cpe.patient_market_urn), 1, 30) AS patient_market_urn,
                   cpe.encounter_start_date AS encounter_start_date,
                   cpe.encounter_end_date AS encounter_end_date,
                   trim(cpe.coid) AS coid,
                   trim(cpe.company_code) AS company_code,
                   trim(cpe.sub_unit_num) AS sub_unit_num,
                   trim(cpe.diagnosis_type_code) AS diagnosis_type_code,
                   trim(cpe.principal_diagnosis_code) AS principal_diagnosis_code,
                   trim(cpe.procedure_type_code) AS procedure_type_code,
                   trim(cpe.principal_procedure_code) AS principal_procedure_code,
                   cpe.principal_procedure_date AS principal_procedure_date,
                   cpe.total_billed_charge_amt AS total_billed_charge_amt,
                   cpe.patient_age AS patient_age,
                   cpe.patient_person_dw_id AS patient_person_dw_id,
                   cpe.financial_class_code AS financial_class_code,
                   trim(cps.patient_type_code_pos1) AS patient_type_code_pos1,
                   trim(cps.emergency_ind) AS emergency_ind,
                   cps.oncology_tumor_site_id AS oncology_tumor_site_id,
                   cps.oncology_detail_tumor_site_id AS oncology_detail_tumor_site_id,
                   trim(cps.robotic_ind3) AS robotic_ind3,
                   substr(trim(cps.op_product_line_desc), 1, 255) AS op_product_line_desc,
                   trim(cpf.drg_code_hcfa) AS drg_code_hcfa,
                   trim(cpf.drg_medical_surgical_ind) AS drg_medical_surgical_ind,
                   cpf.calculated_length_of_stay_num AS calculated_length_of_stay_num,
                   cpf.estimated_net_revenue_amt AS estimated_net_revenue_amt,
                   cpf.direct_contribution_margin_amt AS direct_contribution_margin_amt,
                   cpf.ebdita_amt AS ebdita_amt,
                   substr(trim(pesl.esl_level_1_desc), 1, 255) AS esl_level_1_desc,
                   substr(trim(pesl.esl_level_2_desc), 1, 255) AS esl_level_2_desc,
                   substr(trim(pesl.esl_level_3_desc), 1, 255) AS esl_level_3_desc,
                   substr(trim(pesl.esl_level_4_desc), 1, 255) AS esl_level_4_desc,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT consolidated_patient_encounter.*
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.consolidated_patient_encounter
      WHERE consolidated_patient_encounter.encounter_start_date BETWEEN DATE '2018-01-01' AND date_sub(current_date('US/Central'), interval extract(DAY
                                                                                                                                                    FROM current_date('US/Central')) DAY)
        AND upper(rtrim(consolidated_patient_encounter.encounter_database_code)) = 'EDWPF' ) AS cpe
   INNER JOIN
     (SELECT consolidated_patient_service.patient_type_code_pos1,
             consolidated_patient_service.emergency_ind,
             consolidated_patient_service.oncology_tumor_site_id,
             consolidated_patient_service.oncology_detail_tumor_site_id,
             consolidated_patient_service.robotic_ind3,
             consolidated_patient_service.op_product_line_desc,
             consolidated_patient_service.encounter_id,
             consolidated_patient_service.encounter_source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.consolidated_patient_service
      WHERE upper(rtrim(consolidated_patient_service.oncology_ind)) = 'Y'
        AND upper(rtrim(consolidated_patient_service.normal_newborn_bed_type_ind)) = 'N' ) AS cps ON cps.encounter_id = cpe.encounter_id
   AND upper(trim(cps.encounter_source_system_code)) = upper(trim(cpe.encounter_source_system_code))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.consolidated_patient_financial AS cpf ON cpe.encounter_id = cpf.encounter_id
   AND upper(trim(cpe.encounter_source_system_code)) = upper(trim(cpf.encounter_source_system_code))
   INNER JOIN
     (SELECT fact_facility.coid
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.fact_facility
      WHERE upper(rtrim(fact_facility.lob_code)) IN('HOS')
        AND upper(rtrim(fact_facility.sub_lob_code)) IN('MED')
        AND upper(rtrim(fact_facility.coid_status_code)) = 'F' ) AS ff ON upper(rtrim(cpe.coid)) = upper(rtrim(ff.coid))
   LEFT OUTER JOIN
     (SELECT pe.company_code,
             pe.coid,
             pe.patient_dw_id AS encounter_id,
             pe.pat_acct_num,
             pe.patient_type_summary_code,
             pe.esl_code,
             re.esl_level_1_desc,
             re.esl_level_2_desc,
             re.esl_level_3_desc,
             re.esl_level_4_desc,
             pe.esl_level_5_code,
             pe.esl_level_5_desc
      FROM `hca-hin-dev-cur-ops`.edwcr_base_views.patient_esl AS pe
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_esl AS re ON upper(rtrim(re.esl_code)) = upper(rtrim(pe.esl_code))
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.diagnosis_related_group AS drg ON upper(rtrim(pe.esl_level_5_code)) = upper(rtrim(drg.reimbursement_group_code))
      AND upper(rtrim(drg.reimbursement_group_name)) = 'M'
      AND drg.reimbursement_group_start_date <= current_date('US/Central')
      AND drg.reimbursement_group_end_date >= current_date('US/Central')) AS pesl ON pesl.encounter_id = cpe.encounter_id) AS ms ON mt.encounter_id = ms.encounter_id
AND mt.encounter_source_system_code = ms.encounter_source_system_code
AND (upper(coalesce(mt.empi_text, '0')) = upper(coalesce(ms.empi_text, '0'))
     AND upper(coalesce(mt.empi_text, '1')) = upper(coalesce(ms.empi_text, '1')))
AND (upper(coalesce(mt.pat_acct_num, '0')) = upper(coalesce(ms.pat_acct_num, '0'))
     AND upper(coalesce(mt.pat_acct_num, '1')) = upper(coalesce(ms.pat_acct_num, '1')))
AND (upper(coalesce(mt.medical_record_num, '0')) = upper(coalesce(ms.medical_record_num, '0'))
     AND upper(coalesce(mt.medical_record_num, '1')) = upper(coalesce(ms.medical_record_num, '1')))
AND (upper(coalesce(mt.patient_market_urn, '0')) = upper(coalesce(ms.patient_market_urn, '0'))
     AND upper(coalesce(mt.patient_market_urn, '1')) = upper(coalesce(ms.patient_market_urn, '1')))
AND (coalesce(mt.encounter_start_date, DATE '1970-01-01') = coalesce(ms.encounter_start_date, DATE '1970-01-01')
     AND coalesce(mt.encounter_start_date, DATE '1970-01-02') = coalesce(ms.encounter_start_date, DATE '1970-01-02'))
AND (coalesce(mt.encounter_end_date, DATE '1970-01-01') = coalesce(ms.encounter_end_date, DATE '1970-01-01')
     AND coalesce(mt.encounter_end_date, DATE '1970-01-02') = coalesce(ms.encounter_end_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (upper(coalesce(mt.sub_unit_num, '0')) = upper(coalesce(ms.sub_unit_num, '0'))
     AND upper(coalesce(mt.sub_unit_num, '1')) = upper(coalesce(ms.sub_unit_num, '1')))
AND (upper(coalesce(mt.diagnosis_type_code, '0')) = upper(coalesce(ms.diagnosis_type_code, '0'))
     AND upper(coalesce(mt.diagnosis_type_code, '1')) = upper(coalesce(ms.diagnosis_type_code, '1')))
AND (upper(coalesce(mt.principal_diagnosis_code, '0')) = upper(coalesce(ms.principal_diagnosis_code, '0'))
     AND upper(coalesce(mt.principal_diagnosis_code, '1')) = upper(coalesce(ms.principal_diagnosis_code, '1')))
AND (upper(coalesce(mt.procedure_type_code, '0')) = upper(coalesce(ms.procedure_type_code, '0'))
     AND upper(coalesce(mt.procedure_type_code, '1')) = upper(coalesce(ms.procedure_type_code, '1')))
AND (upper(coalesce(mt.principal_procedure_code, '0')) = upper(coalesce(ms.principal_procedure_code, '0'))
     AND upper(coalesce(mt.principal_procedure_code, '1')) = upper(coalesce(ms.principal_procedure_code, '1')))
AND (coalesce(mt.principal_procedure_date, DATE '1970-01-01') = coalesce(ms.principal_procedure_date, DATE '1970-01-01')
     AND coalesce(mt.principal_procedure_date, DATE '1970-01-02') = coalesce(ms.principal_procedure_date, DATE '1970-01-02'))
AND (coalesce(mt.total_billed_charge_amt, NUMERIC '0') = coalesce(ms.total_billed_charge_amt, NUMERIC '0')
     AND coalesce(mt.total_billed_charge_amt, NUMERIC '1') = coalesce(ms.total_billed_charge_amt, NUMERIC '1'))
AND (coalesce(mt.patient_age, 0) = coalesce(ms.patient_age, 0)
     AND coalesce(mt.patient_age, 1) = coalesce(ms.patient_age, 1))
AND (coalesce(mt.patient_person_dw_id, NUMERIC '0') = coalesce(ms.patient_person_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_person_dw_id, NUMERIC '1') = coalesce(ms.patient_person_dw_id, NUMERIC '1'))
AND (coalesce(mt.financial_class_code, 0) = coalesce(ms.financial_class_code, 0)
     AND coalesce(mt.financial_class_code, 1) = coalesce(ms.financial_class_code, 1))
AND (upper(coalesce(mt.patient_type_code_pos1, '0')) = upper(coalesce(ms.patient_type_code_pos1, '0'))
     AND upper(coalesce(mt.patient_type_code_pos1, '1')) = upper(coalesce(ms.patient_type_code_pos1, '1')))
AND (upper(coalesce(mt.emergency_ind, '0')) = upper(coalesce(ms.emergency_ind, '0'))
     AND upper(coalesce(mt.emergency_ind, '1')) = upper(coalesce(ms.emergency_ind, '1')))
AND (coalesce(mt.oncology_tumor_site_id, 0) = coalesce(ms.oncology_tumor_site_id, 0)
     AND coalesce(mt.oncology_tumor_site_id, 1) = coalesce(ms.oncology_tumor_site_id, 1))
AND (coalesce(mt.oncology_detail_tumor_site_id, 0) = coalesce(ms.oncology_detail_tumor_site_id, 0)
     AND coalesce(mt.oncology_detail_tumor_site_id, 1) = coalesce(ms.oncology_detail_tumor_site_id, 1))
AND (upper(coalesce(mt.robotic_ind3, '0')) = upper(coalesce(ms.robotic_ind3, '0'))
     AND upper(coalesce(mt.robotic_ind3, '1')) = upper(coalesce(ms.robotic_ind3, '1')))
AND (upper(coalesce(mt.op_product_line_desc, '0')) = upper(coalesce(ms.op_product_line_desc, '0'))
     AND upper(coalesce(mt.op_product_line_desc, '1')) = upper(coalesce(ms.op_product_line_desc, '1')))
AND (upper(coalesce(mt.drg_code_hcfa, '0')) = upper(coalesce(ms.drg_code_hcfa, '0'))
     AND upper(coalesce(mt.drg_code_hcfa, '1')) = upper(coalesce(ms.drg_code_hcfa, '1')))
AND (upper(coalesce(mt.drg_medical_surgical_ind, '0')) = upper(coalesce(ms.drg_medical_surgical_ind, '0'))
     AND upper(coalesce(mt.drg_medical_surgical_ind, '1')) = upper(coalesce(ms.drg_medical_surgical_ind, '1')))
AND (coalesce(mt.calculated_length_of_stay_num, NUMERIC '0') = coalesce(ms.calculated_length_of_stay_num, NUMERIC '0')
     AND coalesce(mt.calculated_length_of_stay_num, NUMERIC '1') = coalesce(ms.calculated_length_of_stay_num, NUMERIC '1'))
AND (coalesce(mt.estimated_net_revenue_amt, NUMERIC '0') = coalesce(ms.estimated_net_revenue_amt, NUMERIC '0')
     AND coalesce(mt.estimated_net_revenue_amt, NUMERIC '1') = coalesce(ms.estimated_net_revenue_amt, NUMERIC '1'))
AND (coalesce(mt.direct_contribution_margin_amt, NUMERIC '0') = coalesce(ms.direct_contribution_margin_amt, NUMERIC '0')
     AND coalesce(mt.direct_contribution_margin_amt, NUMERIC '1') = coalesce(ms.direct_contribution_margin_amt, NUMERIC '1'))
AND (coalesce(mt.ebdita_amt, NUMERIC '0') = coalesce(ms.ebdita_amt, NUMERIC '0')
     AND coalesce(mt.ebdita_amt, NUMERIC '1') = coalesce(ms.ebdita_amt, NUMERIC '1'))
AND (upper(coalesce(mt.esl_level_1_desc, '0')) = upper(coalesce(ms.esl_level_1_desc, '0'))
     AND upper(coalesce(mt.esl_level_1_desc, '1')) = upper(coalesce(ms.esl_level_1_desc, '1')))
AND (upper(coalesce(mt.esl_level_2_desc, '0')) = upper(coalesce(ms.esl_level_2_desc, '0'))
     AND upper(coalesce(mt.esl_level_2_desc, '1')) = upper(coalesce(ms.esl_level_2_desc, '1')))
AND (upper(coalesce(mt.esl_level_3_desc, '0')) = upper(coalesce(ms.esl_level_3_desc, '0'))
     AND upper(coalesce(mt.esl_level_3_desc, '1')) = upper(coalesce(ms.esl_level_3_desc, '1')))
AND (upper(coalesce(mt.esl_level_4_desc, '0')) = upper(coalesce(ms.esl_level_4_desc, '0'))
     AND upper(coalesce(mt.esl_level_4_desc, '1')) = upper(coalesce(ms.esl_level_4_desc, '1')))
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (encounter_id,
        encounter_source_system_code,
        empi_text,
        pat_acct_num,
        medical_record_num,
        patient_market_urn,
        encounter_start_date,
        encounter_end_date,
        coid,
        company_code,
        sub_unit_num,
        diagnosis_type_code,
        principal_diagnosis_code,
        procedure_type_code,
        principal_procedure_code,
        principal_procedure_date,
        total_billed_charge_amt,
        patient_age,
        patient_person_dw_id,
        financial_class_code,
        patient_type_code_pos1,
        emergency_ind,
        oncology_tumor_site_id,
        oncology_detail_tumor_site_id,
        robotic_ind3,
        op_product_line_desc,
        drg_code_hcfa,
        drg_medical_surgical_ind,
        calculated_length_of_stay_num,
        estimated_net_revenue_amt,
        direct_contribution_margin_amt,
        ebdita_amt,
        esl_level_1_desc,
        esl_level_2_desc,
        esl_level_3_desc,
        esl_level_4_desc,
        dw_last_update_date_time)
VALUES (ms.encounter_id, ms.encounter_source_system_code, ms.empi_text, ms.pat_acct_num, ms.medical_record_num, ms.patient_market_urn, ms.encounter_start_date, ms.encounter_end_date, ms.coid, ms.company_code, ms.sub_unit_num, ms.diagnosis_type_code, ms.principal_diagnosis_code, ms.procedure_type_code, ms.principal_procedure_code, ms.principal_procedure_date, ms.total_billed_charge_amt, ms.patient_age, ms.patient_person_dw_id, ms.financial_class_code, ms.patient_type_code_pos1, ms.emergency_ind, ms.oncology_tumor_site_id, ms.oncology_detail_tumor_site_id, ms.robotic_ind3, ms.op_product_line_desc, ms.drg_code_hcfa, ms.drg_medical_surgical_ind, ms.calculated_length_of_stay_num, ms.estimated_net_revenue_amt, ms.direct_contribution_margin_amt, ms.ebdita_amt, ms.esl_level_1_desc, ms.esl_level_2_desc, ms.esl_level_3_desc, ms.esl_level_4_desc, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT encounter_id,
             encounter_source_system_code
      FROM `hca-hin-dev-cur-ops`.edwcr.fact_cancer_patient_financial
      GROUP BY encounter_id,
               encounter_source_system_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.fact_cancer_patient_financial');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Fact_Cancer_Patient_Financial');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF