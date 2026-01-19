begin
declare
  dup_count int64;
declare  
    current_ts datetime;
set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
begin transaction;
  delete from {{ params.param_hr_core_dataset_name }}.bi_psat_dept_level_smry where 1=1;

  INSERT INTO {{ params.param_hr_core_dataset_name }}.bi_psat_dept_level_smry (psat_dept_level_sk, unique_record_text, total_response_count_num, score_numerator_num, top_box_pct_num, question_id, question_short_name, survey_category_code, survey_category_text, company_code, coid, parent_coid, facility_claim_control_num, facility_claim_control_num_name, year_id, qtr_id, month_id, month_id_desc_l, survey_sub_category_text, functional_dept_num, dept_num, source_system_code, dw_last_update_date_time)
    SELECT
        row_number() OVER (ORDER BY dt.month_id, alt.coid, cfl.dept_num, sq.question_id) AS psat_dept_level_sk,
        concat(coalesce(dt.month_id,cast('' as int64)), coalesce(alt.coid, ''), coalesce(cfl.dept_num, ''), coalesce(sq.question_id, '')) AS unique_record_text,
        count(sr.respondent_id) AS total_response_count_num,
        sum(sr.top_box_score_num) AS score_numerator_num,
        cast((sum(sr.top_box_score_num)/ count(sr.respondent_id)) * 100 as numeric)AS top_box_pct_num,
        sq.question_id,
        sq.question_short_name,
        rs.survey_category_code,
        rs.survey_category_text,
        ff.company_code,
        alt.coid,
        alt.parent_coid,
        alt.facility_claim_control_num,
        alt.facility_claim_control_num_name,
        dt.year_id,
        dt.qtr_id,
        dt.month_id,
        dt.month_id_desc_l,
        sq.survey_sub_category_text,
        cfl.functional_dept_num,
        cfl.dept_num,
        sr.source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_base_views_dataset_name }}.survey_response AS sr
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON sr.survey_question_sid = sq.survey_question_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.fixed_respondent_patient_detail AS frpd ON frpd.respondent_id = sr.respondent_id
         AND frpd.survey_sid = sq.survey_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_alt_org_hierarchy AS alt ON alt.coid = sr.coid
         AND alt.company_code = sr.company_code
        INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON ff.coid = alt.parent_coid
        INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS dt ON sr.time_name_child = dt.date_id
        LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS dt2 ON sr.patient_discharge_date = dt2.date_id
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.outbound_pat_survey_hist_dtl AS outb ON frpd.patient_dw_id = outb.patient_dw_id
        LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.facility_department_location AS cfl ON cfl.coid = alt.parent_coid
         AND cfl.location_mnemonic = outb.location_mnemonic_cs
      WHERE trim(upper(rs.survey_group_text)) = 'PATIENT_SATISFACTION'
       AND trim(upper(sq.source_system_code)) = 'H'
       AND trim(upper(rs.source_system_code)) = 'H'
       AND trim(upper(sr.source_system_code)) = 'H'
       AND date(sr.time_name_child) > '2019-06-01'
       AND trim(upper(sq.survey_sub_category_text)) <> 'DEMOGRAPHIC'
       AND sq.survey_sub_category_text IS NOT NULL
      GROUP BY 2,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  ;
set
  dup_count = (
  select
    count(*)
  from (
    select
      psat_dept_level_sk
    from
      {{ params.param_hr_core_dataset_name }}.bi_psat_dept_level_smry
    group by
      psat_dept_level_sk
    having
      count(*) > 1 ) );
if
  dup_count <> 0 then
rollback transaction; raise
using
  message = concat('duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_patient_satisfaction_domain');
  else
commit transaction;
end if
  ;
END;