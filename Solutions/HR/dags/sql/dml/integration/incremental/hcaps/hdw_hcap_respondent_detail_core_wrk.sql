BEGIN
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, resp, dw_last_update_date_time)
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
        hca_pat_resp.resp,
        hca_pat_resp.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp
  ;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk (hca_unique, surv_id, adj_samp, survey_type, pg_unit, disdate, recdate, mde, question_id, resp, dw_last_update_date_time)
    SELECT
        respondent_detail_rej.hca_unique,
        respondent_detail_rej.surv_id,
        respondent_detail_rej.adj_samp,
        respondent_detail_rej.survey_type,
        respondent_detail_rej.pg_unit,
        respondent_detail_rej.disdate,
        respondent_detail_rej.recdate,
        respondent_detail_rej.mde,
        respondent_detail_rej.question_id,
        respondent_detail_rej.resp,
        respondent_detail_rej.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.respondent_detail_rej
  ;
END ;
