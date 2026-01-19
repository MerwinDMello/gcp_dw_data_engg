CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.factsubmissions AS SELECT
    --  =============================================
    --  Author:        Cheryl Costa
    --  Create date:    01/06/2020
    --  Update:  1/10/20 to added Submission UID
    -- Update:  2/11/20 to add Submission Medium Desc and Offer_Grad Flag
    --  Update 3/24/20 to add the StepStatus_UID,
    --  Update: 4/3/20  replace EmpStatus Join with Job Schedule
    --  Update:  7/22/20 to add EEO info for Candidate
    -- -Update:  9/24/20  Use EDWHR_VIEWS.Ref_Submission_Step   Step_Short_Name in place of  Step_name for InforATS.  Adjust the Submission Status to reflect matching values from the desc field.
    -- -Update:  9/24/20  Edit EEO Joins to include field  InforATS populating
    -- -Update 10/26/20 to add Paid_Source Flag
    -- Update 3/25/2021 to count review,interview,offer time per person
    -- -Update 04/14/21 to sum the row level RIO data rather than just min/max to address those with steps out of order.
    -- -Updated 4/20/21  Modify qualify to look for the recruitment req with the offer rather than by source system
    -- -Updated 5/3/21 to add the New Grad Flag for ATS
    -- -Updated to include addional Ethnicity value location, gender update, and change to filter.
    -- - Updated 06/09/21 to remove case statement around Submission Status Name, Improve join to Offer table, and add FTE Value
    -- -Updated 4/6/22 to add the EEO values from new source
    -- --Updated 5/11/22 to include new Veteran status joins/coalesce
    -- - Updated 11/28/2022 to shift source to physicalized table created by Noah Lamborn, ITG  HDM-2116
    --  Updated 01/03/2023 added Matched_from_requisition_num
    --  Updated 10/20/2023 Add min_non_hrreview_date
    --  Description: FactSubmission
    -- Metrics:  # of applications, rejected candiate motive, sourcing
    --  =============================================
    s.candidate_profile_num AS taleo_submission_id,
    s.submission_sid,
    s.candidate_num AS taleo_candidate_id,
    s.candidate_profile_sid,
    s.candidate_sid,
    s.submission_uid,
    s.dept_sid,
    s.coid,
    s.coid_dept_uid AS coid_uid,
    last_day(DATE(s.creation_date)) AS pe_date,
    s.creation_date AS submission_create_date,
    EXTRACT(TIME FROM s.creation_date_time) AS submission_create_time,
    s.completion_date AS submission_completed_date,
    EXTRACT(TIME FROM s.completion_date_time) AS submission_completed_time,
    CASE
      WHEN s.completion_date IS NULL THEN 0
      ELSE 1
    END AS iscompleted,
    s.recruitment_source_desc AS source_desc,
    s.recruitment_source_type_desc AS source_type,
    s.recruitment_source_auto_filled_sw AS source_autofill,
    s.profile_medium_desc AS submission_medium,
    s.recruitment_requisition_num_text AS taleo_requisition_num,
    s.recruitment_requisition_sid,
    s.lawson_requisition_num,
    s.ghr_requisition_num,
    s.matched_from_requisition_num,
    s.requisition_sid,
    s.status_code AS emp_status,
    s.first_offer_start_date AS offer_start_date_time,
    s.open_fte_percent AS fte_value,
    s.job_schedule_desc,
    s.paid_source_flag AS paid_source,
    s.last_modified_date,
    s.submission_status_name AS current_submission_status,
    s.current_submission_step_name AS current_submission_step,
    s.quality_flag AS quality_ind,
    CASE
      WHEN upper(s.quality_flag) = 'QUALITY' THEN sst_measure.min_step_start
      ELSE CAST(NULL as date)
    END AS min_nonhrreview_date,
    s.motive_name AS rejection_reason,
    1 AS sub_measure_qty,
    s.new_grad_flag AS offer_grad_flag,
    s.gender_name,
    s.ethnicity_name,
    s.veteran_desc,
    s.veteran_spouse_flag,
    s.disability_flag AS disability_ind,
    s.hrr_duration_cnt AS hrr_duration,
    s.hmr_duration_cnt AS hmr_duration,
    s.interview_duration_cnt AS interview_duration,
    s.offer_duration_cnt AS offer_duration,
    s.matched_candidate_flag AS matched_candidate,
    s.candidate_intl_ext_flag AS candidate_ie_ind,
    coalesce(mat1.integrated_lob_id, mat4.integrated_lob_id, mat3.integrated_lob_id, mat2.integrated_lob_id) AS integrated_lob_id,
    s.source_system_code
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_submission s
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department dept
      ON s.dept_sid = dept.dept_sid
      AND date(dept.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department sf
      ON LEFT(dept.dept_code, 3) = sf.dept_num
      AND s.coid = sf.coid
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department df
      ON sf.functional_dept_num = df.functional_dept_num
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility ff
      ON s.coid = ff.coid

    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat1  --Process Level AND Dept Num
      ON Left(s.recruitment_requisition_num_text, 5) = mat1.process_level_code 
      AND dept.dept_code = mat1.dept_code
      AND mat1.match_level_num = 1

    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat2  --LOB and Sub LOB
      ON ff.lob_code = mat2.lob_code 
      AND ff.sub_lob_code = mat2.sub_lob_code
      AND mat2.match_level_num = 2

    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat3  --Function and Sub Function
      ON sf.functional_dept_desc = mat3.functional_dept_desc
      AND sf.sub_functional_dept_desc = mat3.sub_functional_dept_desc
      AND mat3.match_level_num = 3

    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob mat4  --Process Level
      ON Left(s.recruitment_requisition_num_text, 5) = mat4.process_level_code
      AND mat4.match_level_num = 4

     LEFT OUTER JOIN (
      SELECT DISTINCT
          fin.recruitment_requisition_sid,
          fin.submission_sid,
          min(fin.event_start) AS min_step_start,
          max(fin.event_end) AS max_step_end
        FROM
          (
            SELECT
                recruitment_requisition_sid,
                submission_sid,
                step_short_name,
                coalesce(event_date_time, creation_date_time) AS event_start,
                lag(coalesce(event_date_time, creation_date_time), 1) OVER (PARTITION BY str.candidate_profile_sid ORDER BY coalesce(event_date_time, creation_date_time) DESC) AS event_end
              FROM
                {{ params.param_hr_base_views_dataset_name }}.submission AS sub
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS str ON sub.candidate_profile_sid = str.candidate_profile_sid
                 AND date(str.valid_to_date) = '9999-12-31'
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_step AS st ON str.tracking_step_id = st.step_id
              WHERE date(str.valid_to_date) = '9999-12-31'
               AND date(sub.valid_to_date) = '9999-12-31'
               AND sub.recruitment_requisition_sid IS NOT NULL
               AND str.tracking_step_id IS NOT NULL
          ) AS fin
        WHERE upper(trim(fin.step_short_name)) <> 'HR REVIEW'
        GROUP BY 1, 2
    ) AS sst_measure ON s.recruitment_requisition_sid = sst_measure.recruitment_requisition_sid
      AND s.submission_sid = sst_measure.submission_sid
;
