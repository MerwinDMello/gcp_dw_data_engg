BEGIN

DECLARE DUP_COUNT INT64;
DECLARE frequency STRING;
declare  
    current_ts datetime;
  set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
SET frequency = @frequency;

BEGIN TRANSACTION;

UPDATE {{ params.param_hr_core_dataset_name  }}.survey_response AS st SET response_value_text = stg.response_value_text, survey_form_text = stg.survey_form_text, company_code = stg.company_code, coid = stg.coid, patient_discharge_date = stg.patient_discharge_date, time_name_child = stg.time_name_child, cms_submit_preliminary_ind = stg.cms_submit_preliminary_ind, cms_submit_ind = stg.cms_submit_ind, adjusted_sample_ind = stg.adjusted_sample_ind, top_box_score_num = stg.top_box_score_num, final_record_ind = stg.final_record_ind, vendor_assigned_unit_text = stg.vendor_assigned_unit_text, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time FROM (
    SELECT
        survey_response_core_wrk1.survey_question_sid,
        survey_response_core_wrk1.survey_response_sid,
        survey_response_core_wrk1.respondent_id,
        survey_response_core_wrk1.survey_receive_date,
        survey_response_core_wrk1.survey_mode_code,
        survey_response_core_wrk1.response_value_text,
        survey_response_core_wrk1.survey_form_text,
        survey_response_core_wrk1.company_code,
        survey_response_core_wrk1.coid,
        survey_response_core_wrk1.patient_discharge_date,
        survey_response_core_wrk1.time_name_child,
        survey_response_core_wrk1.cms_submit_preliminary_ind,
        survey_response_core_wrk1.cms_submit_ind,
        survey_response_core_wrk1.adjusted_sample_ind,
        survey_response_core_wrk1.top_box_score_num,
        survey_response_core_wrk1.final_record_ind,
        survey_response_core_wrk1.vendor_assigned_unit_text,
        survey_response_core_wrk1.source_system_code,
        survey_response_core_wrk1.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk1
      WHERE upper(survey_response_core_wrk1.flag) = 'R'
  ) AS stg WHERE st.survey_question_sid = stg.survey_question_sid
   AND st.respondent_id = stg.respondent_id
   AND st.survey_response_sid = stg.survey_response_sid
   AND st.survey_receive_date = stg.survey_receive_date
   AND trim(st.survey_mode_code) = trim(stg.survey_mode_code)
   AND (upper(trim(coalesce(st.survey_form_text, ''))) <> upper(trim(coalesce(stg.survey_form_text, '')))
   OR upper(trim(coalesce(st.company_code, ''))) <> upper(trim(coalesce(stg.company_code, '')))
   OR upper(trim(coalesce(st.coid, ''))) <> upper(trim(coalesce(stg.coid, '')))
   OR coalesce(st.patient_discharge_date, DATE"1800-01-01") <> coalesce(stg.patient_discharge_date, DATE'1800-01-01')
   OR upper(trim(coalesce(st.adjusted_sample_ind, ''))) <> upper(trim(coalesce(stg.adjusted_sample_ind, '')))
   AND upper(trim(coalesce(st.vendor_assigned_unit_text, ''))) <> upper(trim(coalesce(stg.vendor_assigned_unit_text, ''))));

UPDATE {{ params.param_hr_core_dataset_name  }}.survey_response AS st SET response_value_text = stg.response_value_text, survey_form_text = stg.survey_form_text, company_code = stg.company_code, coid = stg.coid, patient_discharge_date = stg.patient_discharge_date, time_name_child = stg.time_name_child, cms_submit_preliminary_ind = stg.cms_submit_preliminary_ind, cms_submit_ind = stg.cms_submit_ind, adjusted_sample_ind = stg.adjusted_sample_ind, top_box_score_num = stg.top_box_score_num, final_record_ind = stg.final_record_ind, vendor_assigned_unit_text = stg.vendor_assigned_unit_text, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time FROM (
    SELECT
        survey_response_core_wrk1.survey_question_sid,
        survey_response_core_wrk1.survey_response_sid,
        survey_response_core_wrk1.respondent_id,
        survey_response_core_wrk1.survey_receive_date,
        survey_response_core_wrk1.survey_mode_code,
        survey_response_core_wrk1.response_value_text,
        survey_response_core_wrk1.survey_form_text,
        survey_response_core_wrk1.company_code,
        survey_response_core_wrk1.coid,
        survey_response_core_wrk1.patient_discharge_date,
        survey_response_core_wrk1.time_name_child,
        survey_response_core_wrk1.cms_submit_preliminary_ind,
        survey_response_core_wrk1.cms_submit_ind,
        survey_response_core_wrk1.adjusted_sample_ind,
        survey_response_core_wrk1.top_box_score_num,
        survey_response_core_wrk1.final_record_ind,
        survey_response_core_wrk1.vendor_assigned_unit_text,
        survey_response_core_wrk1.source_system_code,
        survey_response_core_wrk1.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk1
      WHERE upper(survey_response_core_wrk1.flag) = 'S'
  ) AS stg WHERE upper(st.final_record_ind) = 'N'
   AND st.survey_question_sid = stg.survey_question_sid
   AND st.respondent_id = stg.respondent_id
   AND st.survey_response_sid = stg.survey_response_sid
   AND st.survey_receive_date = stg.survey_receive_date
   AND trim(st.survey_mode_code) = trim(stg.survey_mode_code)
   AND (upper(trim(coalesce(st.survey_form_text, ''))) <> upper(trim(coalesce(stg.survey_form_text, '')))
   OR upper(trim(coalesce(st.company_code, ''))) <> upper(trim(coalesce(stg.company_code, '')))
   OR upper(trim(coalesce(st.coid, ''))) <> upper(trim(coalesce(stg.coid, '')))
   OR coalesce(st.patient_discharge_date, DATE'1800-01-01') <> coalesce(stg.patient_discharge_date, DATE'1800-01-01')
   OR upper(trim(coalesce(st.adjusted_sample_ind, ''))) <> upper(trim(coalesce(stg.adjusted_sample_ind, '')))
   AND upper(trim(coalesce(st.vendor_assigned_unit_text, ''))) <> upper(trim(coalesce(stg.vendor_assigned_unit_text, ''))));

INSERT INTO {{ params.param_hr_core_dataset_name  }}.survey_response (survey_question_sid, survey_response_sid, respondent_id, survey_receive_date, survey_mode_code, response_value_text, survey_form_text, company_code, coid, patient_discharge_date, time_name_child, cms_submit_preliminary_ind, cms_submit_ind, adjusted_sample_ind, top_box_score_num, final_record_ind, vendor_assigned_unit_text, source_system_code, dw_last_update_date_time)
    SELECT
        stg.survey_question_sid,
        stg.survey_response_sid,
        stg.respondent_id,
        stg.survey_receive_date,
        stg.survey_mode_code,
        stg.response_value_text,
        stg.survey_form_text,
        stg.company_code,
        stg.coid,
        stg.patient_discharge_date,
        stg.time_name_child,
        stg.cms_submit_preliminary_ind,
        stg.cms_submit_ind,
        stg.adjusted_sample_ind,
        stg.top_box_score_num,
        stg.final_record_ind,
        stg.vendor_assigned_unit_text,
        stg.source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk1 AS stg
      WHERE (stg.survey_question_sid, stg.respondent_id, stg.survey_response_sid, stg.survey_receive_date, trim(stg.survey_mode_code)) NOT IN(
        SELECT AS STRUCT
            survey_response.survey_question_sid,
            survey_response.respondent_id,
            survey_response.survey_response_sid,
            survey_response.survey_receive_date,
            trim(survey_response.survey_mode_code)
          FROM
            {{ params.param_hr_base_views_dataset_name }}.survey_response
      )
  ;
/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select survey_question_sid, respondent_id ,survey_receive_date ,survey_mode_code ,survey_response_sid
        from {{ params.param_hr_core_dataset_name  }}.survey_response
        group by survey_question_sid, respondent_id ,survey_receive_date ,survey_mode_code ,survey_response_sid	
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: edwhr.survey_response');
    ELSE
      COMMIT TRANSACTION;
    END IF;


  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.survey_response_exception;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.survey_response_exception (survey_question_sid, survey_response_sid, respondent_id, survey_receive_date, survey_mode_code, response_value_text, survey_form_text, company_code, coid, patient_discharge_date, time_name_child, cms_submit_preliminary_ind, cms_submit_ind, adjusted_sample_ind, top_box_score_num, final_record_ind, vendor_assigned_unit_text, source_system_code, dw_last_update_date_time)
    SELECT
        stg.survey_question_sid,
        stg.survey_response_sid,
        stg.respondent_id,
        stg.survey_receive_date,
        stg.survey_mode_code,
        stg.response_value_text,
        stg.survey_form_text,
        stg.company_code,
        stg.coid,
        stg.patient_discharge_date,
        stg.time_name_child,
        stg.cms_submit_preliminary_ind,
        stg.cms_submit_ind,
        stg.adjusted_sample_ind,
        stg.top_box_score_num,
        stg.final_record_ind,
        stg.vendor_assigned_unit_text,
        -- '00000' AS PROCESS_LEVEL_CODE,
        -- 00000 AS LAWSON_COMPANY_NUM,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk1 AS stg
      WHERE (stg.survey_question_sid, stg.respondent_id, stg.survey_response_sid, stg.survey_receive_date, trim(stg.survey_mode_code)) IN(
        SELECT AS STRUCT
            survey_response.survey_question_sid,
            survey_response.respondent_id,
            survey_response.survey_response_sid,
            survey_response.survey_receive_date,
            trim(survey_response.survey_mode_code)
          FROM
            {{ params.param_hr_base_views_dataset_name }}.survey_response
          WHERE upper(stg.final_record_ind) = 'Y'
      )
  ;

  UPDATE {{ params.param_hr_core_dataset_name  }}.survey_response AS tgt SET cms_submit_ind = stg.cms_submit_ind, dw_last_update_date_time = current_ts FROM (
    SELECT
        hca_pat_resp.surv_id,
        max(CASE
          WHEN trim(hca_pat_resp.cms_rpt) = '1' THEN 'Y'
          ELSE 'N'
        END) AS cms_submit_ind
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp
      GROUP BY 1, upper(CASE
        WHEN trim(hca_pat_resp.cms_rpt) = '1' THEN 'Y'
        ELSE 'N'
      END)
  ) AS stg WHERE upper(source_system_code) = 'H'
   AND tgt.respondent_id = stg.surv_id
   AND 'QUARTERLY' = upper(frequency);

  UPDATE {{ params.param_hr_core_dataset_name  }}.survey_response AS tgt SET final_record_ind = 'Y', dw_last_update_date_time = current_ts FROM (
    SELECT
        hca_pat_resp.surv_id
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp
      GROUP BY 1
  ) AS stg WHERE upper(tgt.final_record_ind) = 'N'
   AND upper(source_system_code) = 'H'
   AND tgt.respondent_id = stg.surv_id
   AND 'QUARTERLY' = upper(frequency);
  

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.survey_response_core_rej;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.survey_response_core_rej (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, coid, resp, ptype, qtype, category, dw_last_update_date_time, reject_reason)
    SELECT DISTINCT
        hpr.hca_unique,
        hpr.surv_id,
        hpr.adj_samp,
        hpr.survey_type,
        hpr.pg_unit,
        hpr.disdate,
        hpr.recdate,
        hpr.mde,
        hpr.question_id,
        hpr.coid,
        hpr.resp,
        hpr.ptype,
        hpr.qtype,
        hpr.category,
        current_ts AS dw_last_update_date_time,
        'Missing COID in Gallup File' AS reject_reason
      FROM
        {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk2 AS hpr
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON trim(hpr.question_id) = trim(sq.question_id)
         AND upper(sq.source_system_code) = 'H'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON sq.survey_sid = rs.survey_sid
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
       AND g.coid IS NULL
    UNION ALL
    SELECT DISTINCT
        a.hca_unique,
        a.surv_id,
        a.adj_samp,
        a.survey_type,
        a.pg_unit,
        a.disdate,
        a.recdate,
        a.mde,
        a.question_id,
        a.coid,
        a.resp,
        a.ptype,
        a.qtype,
        a.category,
        current_ts AS dw_last_update_date_time,
        'Missing QUESTIONID' AS reject_reason
      FROM
        (
          SELECT
              survey_response_core_wrk2.hca_unique,
              survey_response_core_wrk2.surv_id,
              survey_response_core_wrk2.adj_samp,
              survey_response_core_wrk2.survey_type,
              survey_response_core_wrk2.pg_unit,
              survey_response_core_wrk2.disdate,
              survey_response_core_wrk2.recdate,
              survey_response_core_wrk2.mde,
              survey_response_core_wrk2.question_id,
              survey_response_core_wrk2.coid,
              survey_response_core_wrk2.resp,
              survey_response_core_wrk2.ptype,
              survey_response_core_wrk2.qtype,
              survey_response_core_wrk2.category
            FROM
              {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk2
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
        ) AS a
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS que ON que.question_id = a.question_id
      WHERE que.question_id IS NULL
  ;
END;