BEGIN
  DECLARE DUP_COUNT INT64;
  declare  
    current_ts datetime;
  set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk2;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk2 (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, coid, resp, ptype, qtype, category, cms_rpt, dw_last_update_date_time, flag)
    SELECT
        hca_pat_resp.hca_unique,
        hca_pat_resp.surv_id,
        hca_pat_resp.adj_samp,
        hca_pat_resp.survey_type,
        hca_pat_resp.pg_unit,
        hca_pat_resp.disdate,
        hca_pat_resp.recdate,
        hca_pat_resp.mde,
        hca_pat_resp.question_id,
        hca_pat_resp.coid,
        hca_pat_resp.resp,
        hca_pat_resp.ptype,
        hca_pat_resp.qtype,
        hca_pat_resp.category,
        hca_pat_resp.cms_rpt,
        hca_pat_resp.dw_last_update_date_time,
        'S' AS flag
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp
  ;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk2 (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, coid, resp, ptype, qtype, category, cms_rpt, dw_last_update_date_time, flag)
    SELECT
        survey_response_core_rej.hca_unique,
        survey_response_core_rej.surv_id,
        survey_response_core_rej.adj_samp,
        survey_response_core_rej.survey_type,
        survey_response_core_rej.pg_unit,
        survey_response_core_rej.disdate,
        survey_response_core_rej.recdate,
        survey_response_core_rej.mde,
        survey_response_core_rej.question_id,
        survey_response_core_rej.coid,
        survey_response_core_rej.resp,
        survey_response_core_rej.ptype,
        survey_response_core_rej.qtype,
        survey_response_core_rej.category,
        survey_response_core_rej.cms_rpt,
        survey_response_core_rej.dw_last_update_date_time,
        'R' AS flag
      FROM
        {{ params.param_hr_stage_dataset_name }}.survey_response_core_rej
      WHERE survey_response_core_rej.coid IS NOT NULL
  ;

   TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk1;

  BEGIN TRANSACTION;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk1 (survey_question_sid, survey_response_sid, respondent_id, survey_receive_date, survey_mode_code, response_value_text, survey_form_text, company_code, coid, patient_discharge_date, time_name_child, cms_submit_preliminary_ind, cms_submit_ind, adjusted_sample_ind, top_box_score_num, final_record_ind, vendor_assigned_unit_text, source_system_code, dw_last_update_date_time, flag)
    SELECT
        ee.survey_question_sid,
        ee.survey_response_sid,
        ee.surv_id,
        ee.recdate,
        ee.survey_mode_code,
        ee.response_value_text,
        ee.survey_type,
        ee.company_code,
        ee.coid,
        ee.disdate,
        ee.last_day_mon,
        ee.cms_submit_preliminary_ind,
        ee.cms_submit_ind,
        ee.adj_samp,
        ee.top_box_score_num,
        ee.final_record_ind,
        ee.pg_unit,
        ee.source_system_code,
        ee.dw_last_update_date_time,
        ee.flag
      FROM
        (
          SELECT
              sq.survey_question_sid,
              coalesce(srm.survey_response_sid, 0) AS survey_response_sid,
              hpr.surv_id,
              hpr.recdate,
              rsm.survey_mode_code,
              trim(hpr.resp) AS response_value_text,
              hpr.survey_type,
              ff.company_code,
              g.coid,
              hpr.disdate,
              last_day(DATE(hpr.disdate)) AS last_day_mon,
              -- CASE WHEN RPD.CMS_Exclusion_Ind =  'N' THEN 'Y' ELSE 'N' END AS CMS_Submit_Preliminary_Ind ,
              CASE
                WHEN trim(hpr.cms_rpt) = '1' THEN 'Y'
                ELSE 'N'
              END AS cms_submit_preliminary_ind,
              'N' AS cms_submit_ind,
              -- HPR.ADJ_SAMP,
              CASE
                WHEN upper(hpr.adj_samp) = '1' THEN 'Y'
                ELSE 'N'
              END AS adj_samp,
              CASE
                 trim(hpr.resp)
                WHEN '' THEN 0
                ELSE CAST(trim(hpr.resp) as INT64)
              END AS res,
              CASE
                WHEN CASE
                   trim(hpr.resp)
                  WHEN '' THEN 0
                  ELSE CAST(trim(hpr.resp) as INT64)
                END BETWEEN sq.top_box_num AND sq.top_box_high_num THEN 1
                ELSE 0
              END AS top_box_score_num,
              'N' AS final_record_ind,
              hpr.pg_unit,
              'H' AS source_system_code,
              current_ts AS dw_last_update_date_time,
              hpr.flag
            FROM
              {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk2 AS hpr
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON trim(hpr.question_id) = trim(sq.question_id)
               AND upper(sq.source_system_code) = 'H'
              INNER JOIN -- ON HPR.QUESTION_ID = SQ.QUESTION_ID
              {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON sq.survey_sid = rs.survey_sid
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_response_master AS srm ON srm.response_value_text = trim(hpr.resp)
               AND sq.survey_question_sid = srm.survey_question_sid
               AND upper(srm.source_system_code) = 'H'
              INNER JOIN -- ON SRM.RESPONSE_VALUE_TEXT = RES --comment 08/01/17
              {{ params.param_hr_base_views_dataset_name }}.ref_survey_mode AS rsm ON rsm.survey_mode_code = hpr.mde
               AND upper(rsm.source_system_code) = 'H'
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.respondent_patient_detail AS rpd ON rpd.respondent_id = hpr.surv_id
               AND rpd.discharge_date = hpr.disdate
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS g ON hpr.coid = g.coid
              LEFT OUTER JOIN -- ON (CASE WHEN SUBSTR(HPR.SURVEY_TYPE,2,1)='Y' THEN SUBSTR(HPR.HCA_UNIQUE,3,5) ELSE SUBSTR(HPR.HCA_UNIQUE,1,5) END =G.COID)
              {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON ff.coid = g.coid
            WHERE upper(rs.survey_group_text) = 'PATIENT_SATISFACTION'
             AND rs.eff_to_date = '9999-12-31'
             AND sq.eff_to_date = '9999-12-31'
             AND g.coid IS NOT NULL
             AND hpr.qtype NOT IN(
              -- INNER JOIN edw_pub_views_copy.LU_DATE LD
              -- ON LD.DATE_ID = HPR.DISDATE
              -- -- hdm-848-ADDED CONDITION TO FILTER THE DATA IF NOT PRESENT IN GALLUP FILE---
              'OY', 'PY'
            )
        ) AS ee
      QUALIFY row_number() OVER (PARTITION BY ee.survey_question_sid, ee.surv_id, ee.recdate, ee.survey_mode_code, ee.survey_response_sid ORDER BY ee.surv_id, ee.survey_question_sid, ee.recdate, ee.survey_mode_code, ee.survey_response_sid) = 1
  ;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk1 (survey_question_sid, survey_response_sid, respondent_id, survey_receive_date, survey_mode_code, response_value_text, survey_form_text, company_code, coid, patient_discharge_date, time_name_child, cms_submit_preliminary_ind, cms_submit_ind, adjusted_sample_ind, top_box_score_num, final_record_ind, vendor_assigned_unit_text, source_system_code, dw_last_update_date_time, flag)
    SELECT
        ee.survey_question_sid,
        ee.survey_response_sid,
        ee.surv_id,
        ee.recdate,
        ee.survey_mode_code,
        ee.response_value_text,
        ee.survey_type,
        ee.company_code,
        ee.coid,
        ee.disdate,
        ee.last_day_mon,
        ee.cms_submit_preliminary_ind,
        ee.cms_submit_ind,
        ee.adj_samp,
        ee.top_box_score_num,
        ee.final_record_ind,
        ee.pg_unit,
        ee.source_system_code,
        ee.dw_last_update_date_time,
        ee.flag
      FROM
        (
          SELECT
              sq.survey_question_sid,
              coalesce(srm.survey_response_sid, 0) AS survey_response_sid,
              hpr.surv_id,
              hpr.recdate,
              rsm.survey_mode_code,
              trim(hpr.resp) AS response_value_text,
              hpr.survey_type,
              ff.company_code,
              g.coid,
              hpr.disdate,
              last_day(DATE(hpr.disdate)) AS last_day_mon,
              -- CASE WHEN RPD.CMS_Exclusion_Ind =  'N' THEN 'Y' ELSE 'N' END AS CMS_Submit_Preliminary_Ind ,
              CASE
                WHEN trim(hpr.cms_rpt) = '1' THEN 'Y'
                ELSE 'N'
              END AS cms_submit_preliminary_ind,
              'N' AS cms_submit_ind,
              -- HPR.ADJ_SAMP,
              CASE
                WHEN upper(hpr.adj_samp) = '1' THEN 'Y'
                ELSE 'N'
              END AS adj_samp,
              CASE
                 trim(hpr.resp)
                WHEN '' THEN 0
                ELSE CAST(trim(hpr.resp) as INT64)
              END AS res,
              CASE
                WHEN CASE
                   trim(hpr.resp)
                  WHEN '' THEN 0
                  ELSE CAST(trim(hpr.resp) as INT64)
                END BETWEEN sq.top_box_num AND sq.top_box_high_num THEN 1
                ELSE 0
              END AS top_box_score_num,
              'N' AS final_record_ind,
              hpr.pg_unit,
              'H' AS source_system_code,
              current_ts AS dw_last_update_date_time,
              hpr.flag
            FROM
              {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk2 AS hpr
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON trim(hpr.question_id) = trim(sq.question_id)
               AND upper(sq.source_system_code) = 'H'
              INNER JOIN -- ON HPR.QUESTION_ID = SQ.QUESTION_ID
              {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON sq.survey_sid = rs.survey_sid
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_response_master AS srm ON srm.response_value_text = trim(hpr.resp)
               AND sq.survey_question_sid = srm.survey_question_sid
               AND upper(srm.source_system_code) = 'H'
              INNER JOIN -- ON SRM.RESPONSE_VALUE_TEXT = RES --comment 08/01/17
              {{ params.param_hr_base_views_dataset_name }}.ref_survey_mode AS rsm ON rsm.survey_mode_code = hpr.mde
               AND upper(rsm.source_system_code) = 'H'
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.respondent_patient_detail AS rpd ON rpd.respondent_id = hpr.surv_id
               AND rpd.discharge_date = hpr.disdate
              LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS g ON hpr.coid = g.coid
              LEFT OUTER JOIN -- ON (CASE WHEN SUBSTR(HPR.SURVEY_TYPE,2,1)='Y' THEN SUBSTR(HPR.HCA_UNIQUE,3,5) ELSE SUBSTR(HPR.HCA_UNIQUE,1,5) END =G.COID)
              {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON ff.coid = g.coid
            WHERE upper(rs.survey_group_text) = 'PATIENT_SATISFACTION'
             AND rs.eff_to_date = '9999-12-31'
             AND sq.eff_to_date = '9999-12-31'
             AND g.coid IS NOT NULL
             AND hpr.qtype IN(
              -- LEFT JOIN edw_pub_views_copy.LU_DATE LD
              -- ON LD.DATE_ID = HPR.DISDATE
              -- -- hdm-848-ADDED CONDITION TO FILTER THE DATA IF NOT PRESENT IN GALLUP FILE---
              'OY', 'PY'
            )
        ) AS ee
      QUALIFY row_number() OVER (PARTITION BY ee.survey_question_sid, ee.surv_id, ee.recdate, ee.survey_mode_code, ee.survey_response_sid ORDER BY ee.surv_id, ee.survey_question_sid, ee.recdate, ee.survey_mode_code, ee.survey_response_sid) = 1
  ;

/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Survey_Question_SID,Survey_Response_SID ,Respondent_Id ,Survey_Receive_Date ,Survey_Mode_Code
        from {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk1
        group by Survey_Question_SID , Survey_Response_SID ,Respondent_Id ,Survey_Receive_Date ,Survey_Mode_Code	
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: edwhr_copy.time_entry_pay_code_detail');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;
