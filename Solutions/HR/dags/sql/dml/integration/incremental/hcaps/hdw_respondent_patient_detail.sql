

BEGIN
DECLARE DUP_COUNT INT64;
declare current_ts datetime;

set current_ts = datetime_trunc(current_datetime('US/Central'), second);

BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.respondent_patient_detail AS st SET company_code = stg.company_code, coid = stg.coid, parent_coid = stg.parent_coid, pat_acct_num = stg.pat_acct_num, patient_dw_id = stg.patient_dw_id, discharge_date = stg.discharge_date, exclusion_reason_code = stg.exclusion_reason_code, cms_exclusion_ind = stg.cms_exclusion_ind, dw_last_update_date_time = stg.dw_last_update_date_time FROM (
    SELECT
        respondent_patient_detail_wrk.respondent_id,
        respondent_patient_detail_wrk.survey_receive_date,
        respondent_patient_detail_wrk.respondent_type_code,
        respondent_patient_detail_wrk.survey_sid,
        respondent_patient_detail_wrk.company_code,
        respondent_patient_detail_wrk.coid,
        respondent_patient_detail_wrk.parent_coid,
        respondent_patient_detail_wrk.pat_acct_num,
        respondent_patient_detail_wrk.patient_dw_id,
        respondent_patient_detail_wrk.discharge_date,
        -- respondent_patient_detail_wrk.facility_claim_control_num,
        respondent_patient_detail_wrk.exclusion_reason_code,
        respondent_patient_detail_wrk.cms_exclusion_ind,
        respondent_patient_detail_wrk.final_record_ind,
        respondent_patient_detail_wrk.source_system_code,
        respondent_patient_detail_wrk.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.respondent_patient_detail_wrk
      QUALIFY row_number() OVER (PARTITION BY respondent_patient_detail_wrk.respondent_id, respondent_patient_detail_wrk.survey_receive_date, respondent_patient_detail_wrk.respondent_type_code, respondent_patient_detail_wrk.survey_sid ORDER BY respondent_patient_detail_wrk.respondent_id, respondent_patient_detail_wrk.survey_receive_date, respondent_patient_detail_wrk.respondent_type_code, respondent_patient_detail_wrk.survey_sid DESC) = 1
  ) AS stg WHERE upper(st.final_record_ind) = 'N'
   AND st.respondent_type_code = stg.respondent_type_code
   AND st.respondent_id = stg.respondent_id
   AND st.survey_sid = stg.survey_sid
   AND st.survey_receive_date = stg.survey_receive_date;

  INSERT INTO {{ params.param_hr_core_dataset_name }}.respondent_patient_detail (respondent_id, survey_receive_date, respondent_type_code, survey_sid, company_code, coid, parent_coid, pat_acct_num, patient_dw_id, discharge_date, facility_claim_control_num, exclusion_reason_code, cms_exclusion_ind, final_record_ind, source_system_code, dw_last_update_date_time)
    SELECT
        stg.respondent_id,
        stg.survey_receive_date,
        stg.respondent_type_code,
        stg.survey_sid,
        stg.company_code,
        stg.coid,
        stg.parent_coid,
        stg.pat_acct_num,
        stg.patient_dw_id,
        stg.discharge_date,
        stg.facility_claim_control_num,
        stg.exclusion_reason_code,
        stg.cms_exclusion_ind,
        stg.final_record_ind,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.respondent_patient_detail_wrk AS stg
      WHERE (stg.respondent_type_code, stg.survey_receive_date, stg.respondent_id, stg.survey_sid) NOT IN(
        SELECT AS STRUCT
            respondent_type_code,
            survey_receive_date,
            respondent_id,
            survey_sid
          FROM
            {{ params.param_hr_core_dataset_name }}.respondent_patient_detail
      )
      QUALIFY row_number() OVER (PARTITION BY stg.respondent_type_code, stg.survey_receive_date, stg.respondent_id, stg.survey_sid ORDER BY coalesce(stg.exclusion_reason_code, '0') DESC) = 1
  ;

  delete from {{ params.param_hr_stage_dataset_name }}.respondent_patient_detail_exception where true;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.respondent_patient_detail_exception (respondent_id, survey_receive_date, respondent_type_code, survey_sid, company_code, coid, parent_coid, pat_acct_num, patient_dw_id, discharge_date, facility_claim_control_num, exclusion_reason_code, cms_exclusion_ind, final_record_ind, source_system_code, dw_last_update_date_time)
    SELECT
        stg.respondent_id,
        stg.survey_receive_date,
        stg.respondent_type_code,
        stg.survey_sid,
        stg.company_code,
        stg.coid,
        stg.parent_coid,
        stg.pat_acct_num,
        stg.patient_dw_id,
        stg.discharge_date,
        stg.facility_claim_control_num,
        stg.exclusion_reason_code,
        stg.cms_exclusion_ind,
        stg.final_record_ind,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.respondent_patient_detail_wrk AS stg
      WHERE (stg.respondent_type_code, stg.survey_receive_date, stg.respondent_id, stg.survey_sid) IN(
        SELECT AS STRUCT
            stg.respondent_type_code,
            stg.survey_receive_date,
            stg.respondent_id,
            stg.survey_sid
          FROM
            {{ params.param_hr_core_dataset_name }}.respondent_patient_detail
          WHERE upper(stg.final_record_ind) = 'Y'
      )
      QUALIFY row_number() OVER (PARTITION BY stg.respondent_type_code, stg.survey_receive_date, stg.respondent_id, stg.survey_sid ORDER BY coalesce(cast(stg.exclusion_reason_code as int64), 0) DESC) = 1
  ;

  UPDATE {{ params.param_hr_core_dataset_name }}.respondent_patient_detail AS tgt SET final_record_ind = 'Y', dw_last_update_date_time = current_ts FROM (
    SELECT
        respondent_patient_detail_wrk.respondent_id
      FROM
        {{ params.param_hr_stage_dataset_name }}.respondent_patient_detail_wrk
      GROUP BY 1
  ) AS stg WHERE upper(tgt.final_record_ind) = 'N'
   AND tgt.respondent_id = stg.respondent_id
   AND 'QUARTERLY' = upper((
    SELECT
        load_type
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_load_type
  ));


  delete from {{ params.param_hr_stage_dataset_name }}.rspndnt_pat_dtl_rej where true;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.rspndnt_pat_dtl_rej (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, resp, coid, reject_reason, dw_last_update_date_time)
    SELECT DISTINCT
        hqm.hca_unique,
        hqm.surv_id,
        hqm.adj_samp,
        hqm.survey_type,
        hqm.pg_unit,
        hqm.disdate,
        hqm.survey_receive_date AS recdate,
        hqm.mde,
        hqm.question_id,
        hqm.resp,
        hqm.coid,
        'Missing COID in Gallup File' AS reject_reason,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk0 AS hqm
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON hqm.question_id = sq.question_id
        INNER JOIN {{ params.param_hr_core_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.gallup AS g ON hqm.coid = g.coid
        LEFT OUTER JOIN -- INNER JOIN EDW_PUB_VIEWS.FACT_FACILITY FF
        {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON ff.coid = hqm.coid
        LEFT OUTER JOIN -- 	ON FF.COID=G.COID
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_detail AS prd ON prd.hca_unique = hqm.hca_unique
         AND hqm.surv_id = prd.survey_id
        -- LEFT OUTER JOIN -- $NCR_STG_SCHEMA.CLINICAL_ACCTKEYS_PROD4 CA
        -- `hca-hin-dev-cur-hr`.edwpf_staging.clinical_acctkeys AS ca ON hqm.coid = ca.coid
        --  AND hqm.pat_acct_num = ca.pat_acct_num
        -- LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.ref_bp_facility AS rbf ON hqm.coid = rbf.coid
        --  AND ff.company_code = rbf.company_code
      WHERE upper(rs.survey_group_text) = 'PATIENT_SATISFACTION'
       AND sq.eff_to_date = '9999-12-31'
       AND rs.eff_to_date = '9999-12-31'
       AND g.coid IS NULL
    UNION ALL
    SELECT DISTINCT
        -- G.COID =RBF.COID
        a.hca_unique,
        a.surv_id,
        a.adj_samp,
        a.survey_type,
        a.pg_unit,
        a.disdate,
        a.survey_receive_date,
        a.mde,
        a.question_id,
        a.resp,
        a.coid,
        'Missing QUESTIONID' AS reject_reason,
       current_ts AS dw_last_update_date_time
      FROM
        (
          SELECT
              hca_pat_resp_wrk0.hca_unique,
              hca_pat_resp_wrk0.surv_id,
              hca_pat_resp_wrk0.adj_samp,
              hca_pat_resp_wrk0.survey_type,
              hca_pat_resp_wrk0.pg_unit,
              hca_pat_resp_wrk0.disdate,
              hca_pat_resp_wrk0.survey_receive_date,
              hca_pat_resp_wrk0.mde,
              hca_pat_resp_wrk0.question_id,
              hca_pat_resp_wrk0.resp,
              hca_pat_resp_wrk0.coid
            FROM
              {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk0
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
        ) AS a
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS que ON que.question_id = a.question_id
      WHERE que.question_id IS NULL
  ;

        SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT Respondent_Id ,Survey_Receive_Date ,Respondent_Type_Code ,Survey_SID 
        FROM {{ params.param_hr_core_dataset_name }}.respondent_patient_detail
      GROUP BY Respondent_Id ,Survey_Receive_Date ,Respondent_Type_Code ,Survey_SID
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table:respondent_patient_detail ');
  ELSE
  COMMIT TRANSACTION;
  END IF;
END;
