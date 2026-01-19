--###############################################################################################
--#                                                                                   			#
--# Target Table - {{ params.param_hr_core_dataset_name }}.Fact_Submission            											#
--#                                                                                   			#
--# CHANGE CONTROL:                                                                   			#
--#                                                                                   			#
--# DATE          Developer     Change Comment                                        			#
--# 08/15/2022    N Lamborn     Initial Version                                        			#
--#                                                                                   			#
--#                                                                                   			#
--#  This will load {{ params.param_hr_core_dataset_name }}.Fact_Submission      													#
--###############################################################################################

DECLARE QB_Stmt STRING;

--A&C
DECLARE Start_Date DATETIME;
DECLARE End_Date DATETIME;

-- SELECT Current_Timestamp(0) INTO :Start_Date;
SET Start_date = current_datetime('US/Central');
BEGIN

/*Candidate Profile*/
CREATE TEMPORARY TABLE cp_volatile
  AS
    SELECT
        cp_with.candidate_profile_sid,
        cp_with.candidate_profile_num,
        cp_with.creation_date,
        cp_with.creation_date_time,
        cp_with.completion_date_time,
        cp_with.completion_date,
        cp_with.recruitment_source_auto_filled_sw,
        cp_with.candidate_sid,
        rpm.profile_medium_desc,
        c.candidate_num,
        c.internal_candidate_sw,
        CASE
          WHEN c.internal_candidate_sw = 0 THEN 'External'
          WHEN c.internal_candidate_sw = 1 THEN 'Internal'
          ELSE 'Unknown'
        END AS candidate_ie_ind,
        rrs.paid_source,
        rst.recruitment_source_type_desc,
        rrs.recruitment_source_desc
      FROM
        {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp_with
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS c ON cp_with.candidate_sid = c.candidate_sid
         AND DATE(c.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN (
          SELECT
              *
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_profile_medium AS rpm_0
            WHERE rpm_0.profile_medium_id IS NOT NULL
        ) AS rpm ON cp_with.profile_medium_id = rpm.profile_medium_id
        LEFT OUTER JOIN (
          SELECT
              ref_recruitment_source.recruitment_source_id,
              ref_recruitment_source.recruitment_source_desc,
              ref_recruitment_source.recruitment_source_type_id,
              CASE
                WHEN upper(trim(ref_recruitment_source.recruitment_source_desc)) IN(
                  'LINKEDIN', 'INDEED-SPONSORED', 'GOOGLE ADWORDS', 'GLASSDOOR', 'NEXXT/BEYOND', 'APPFEEDER (JOBS2CAREERS)', 'APPFEEDER (HIREDNURSES)', 'APPFEEDER (ZIPRECRUITER)', 'APPFEEDER (APPCAST)', 'APPFEEDER (NEUVOO)', 'APPFEEDER (UPWARD.NET)', 'APPFEEDER (IHIRE)', 'APPFEEDER (HEALTHJOBS)', 'APPFEEDER (ADZUNA)', 'APPFEEDER (LINKUP)'
                ) THEN 'Yes'
                ELSE 'No'
              END AS paid_source
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source
        ) AS rrs ON cp_with.recruitment_source_id = rrs.recruitment_source_id
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source_type AS rst ON rrs.recruitment_source_type_id = rst.recruitment_source_type_id
      WHERE last_day(DATE(cp_with.creation_date)) > date_add(current_date('US/Central'), interval -37 MONTH)
       AND DATE(cp_with.valid_to_date) = '9999-12-31'
;
-- -Rolling 36 months



/*Candidate Details*/
CREATE TEMPORARY TABLE candidates_volatile
  AS
    SELECT
        cand.candidate_sid,
        gen.gender_name,
        max(eth.ethnicity_name) AS ethnicity_name,
        max(vet.veteran_desc) AS veteran_desc,
        max(dis.disability_flag) AS disability_flag
      FROM
        {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        LEFT OUTER JOIN (
          SELECT
              cand_0.candidate_sid,
              cand_0.element_detail_value_text AS gender_name,
              CASE
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'GENDER_2018' THEN 1
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'EEO2B GENDER' THEN 2
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'EEO2A GENDER' THEN 3
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'EEO1 GENDER' THEN 4
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'GENDER' THEN 5
              END AS responseorder
            FROM
              {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand_0
            WHERE DATE(cand_0.valid_to_date) = '9999-12-31'
             AND upper(trim(cand_0.element_detail_type_text)) IN(
              'GENDER', 'GENDER_2018', 'EEO1 GENDER', 'EEO2A GENDER', 'EEO2B GENDER'
            )
             AND cand_0.element_detail_seq = 1
             AND cand_0.element_detail_value_text IS NOT NULL
            QUALIFY rank() OVER (PARTITION BY cand_0.candidate_sid ORDER BY cand_0.valid_from_date DESC, responseorder) = 1
        ) AS gen ON cand.candidate_sid = gen.candidate_sid
        LEFT OUTER JOIN (
          SELECT
              cand_0.candidate_sid,
              CASE
                WHEN upper(trim(cand_0.source_system_code)) = 'T'
                 AND upper(trim((cast(cand_0.element_detail_id as string)))) = '10441' THEN 'Native Hawaiian or Other Pacific Islander (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'T'
                 AND upper(trim((cast(cand_0.element_detail_id as string)))) = '10345' THEN 'Black or African American (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'T'
                 AND upper(trim((cast(cand_0.element_detail_id as string)))) = '10344' THEN 'Asian (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'T'
                 AND upper(trim((cast(cand_0.element_detail_id as string)))) = '10347' THEN 'Native American or Alaska Native (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'T'
                 AND upper(trim((cast(cand_0.element_detail_id as string)))) = '10348' THEN 'White (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'T'
                 AND upper(trim((cast(cand_0.element_detail_id as string)))) = '10440' THEN 'Two or More Races (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'T'
                 AND upper(trim((cast(cand_0.element_detail_id as string)))) = '15660' THEN 'I do not wish to disclose'
                WHEN upper(trim(cand_0.source_system_code)) = 'T'
                 AND upper(trim((cast(cand_0.element_detail_id as string)))) = '10346' THEN 'Hispanic or Latino'
                WHEN upper(trim(cand_0.source_system_code)) = 'B'
                 AND upper(trim((cand_0.element_detail_value_text))) = 'P' THEN 'Native Hawaiian or Other Pacific Islander (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'B'
                 AND upper(trim((cand_0.element_detail_value_text))) = 'B' THEN 'Black or African American (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'B'
                 AND upper(trim((cand_0.element_detail_value_text))) = 'A' THEN 'Asian (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'B'
                 AND upper(trim((cand_0.element_detail_value_text))) = 'I' THEN 'Native American or Alaska Native (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'B'
                 AND upper(trim((cand_0.element_detail_value_text))) = 'W' THEN 'White (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'B'
                 AND upper(trim((cand_0.element_detail_value_text))) = 'T' THEN 'Two or More Races (Not Hispanic or Latino)'
                WHEN upper(trim(cand_0.source_system_code)) = 'B'
                 AND upper(trim((cand_0.element_detail_value_text))) = 'X' THEN 'I do not wish to disclose'
                WHEN upper(trim(cand_0.source_system_code)) = 'B'
                 AND upper(trim((cand_0.element_detail_value_text))) = 'WITHHOLD' THEN 'I do not wish to disclose'
                WHEN upper(trim(cand_0.source_system_code)) = 'B'
                 AND upper(trim((cand_0.element_detail_value_text))) = 'H' THEN 'Hispanic or Latino'
                WHEN cand_0.element_detail_value_text IS NULL THEN CAST(NULL as STRING)
                ELSE cand_0.element_detail_value_text
              END AS ethnicity_name,
              CASE
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'RACE_ETHNIC_IDENTIFICATION_2020' THEN 1
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'RACE_ETHNIC_IDENTIFICATION_2018' THEN 2
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'RACE_ETHNIC_IDENTIFICATION' THEN 3
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'ETHNICITY' THEN 4
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'EEO2A ETHNICITY' THEN 5
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'EEO2B ETHNICITY' THEN 6
              END AS responseorder
            FROM
              {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand_0
            WHERE DATE(cand_0.valid_to_date) = '9999-12-31'
             AND upper(trim(cand_0.element_detail_type_text)) IN(
              'RACE_ETHNIC_IDENTIFICATION_2020', 'RACE_ETHNIC_IDENTIFICATION_2018', 'RACE_ETHNIC_IDENTIFICATION', 'ETHNICITY', 'EEO2A ETHNICITY', 'EEO2B ETHNICITY'
            )
             AND cand_0.element_detail_seq = 1
             AND (cand_0.element_detail_value_text IS NULL
             AND cand_0.element_detail_id IS NOT NULL
             OR cand_0.element_detail_value_text IS NOT NULL)
            QUALIFY rank() OVER (PARTITION BY cand_0.candidate_sid ORDER BY cand_0.valid_from_date DESC, responseorder) = 1
        ) AS eth ON cand.candidate_sid = eth.candidate_sid
        LEFT OUTER JOIN (
          SELECT
              canddtl.candidate_sid,
              canddtl.element_detail_type_text,
              canddtl.element_detail_id,
              canddtl.element_detail_value_text,
              canddtl.valid_from_date,
              CASE
                WHEN upper(trim((canddtl.element_detail_value_text))) LIKE 'NO%' THEN 'No'
                WHEN upper(trim((canddtl.element_detail_value_text))) = 'N' THEN 'No'
                WHEN upper(trim((canddtl.element_detail_value_text))) = 'Y' THEN 'Yes'
                WHEN upper(trim((canddtl.element_detail_value_text))) LIKE 'I AM NOT%' THEN 'No'
                WHEN upper(trim((canddtl.element_detail_value_text))) LIKE 'I AM A PROTECT%' THEN 'Yes'
                WHEN upper(trim((canddtl.element_detail_value_text))) = 'SPECIAL DISABLED VETERAN' THEN 'Yes'
                WHEN upper(trim((canddtl.element_detail_value_text))) = 'CHOICES 1 AND 2' THEN 'Yes'
                WHEN upper(trim((canddtl.element_detail_value_text))) = '(1) VIETNAM ERA VETERAN' THEN 'Yes'
                WHEN upper(trim((canddtl.element_detail_value_text))) LIKE 'YES%' THEN 'Yes'
                WHEN upper(trim((canddtl.element_detail_value_text))) LIKE '%OTHER%' THEN 'Yes'
                ELSE 'Not Disclosed'
              END AS veteran_desc,
              CASE
                WHEN upper(trim((canddtl.element_detail_type_text))) = 'OTHER_PROTECTED_VETERAN_2018' THEN 1
                WHEN upper(trim((canddtl.element_detail_type_text))) = 'VETERAN STATUS' THEN 2
                WHEN upper(trim((canddtl.element_detail_type_text))) = 'OTHER_PROTECTED_VETERAN' THEN 3
                WHEN upper(trim((canddtl.element_detail_type_text))) = 'EEO2B VETERAN' THEN 4
                WHEN upper(trim((canddtl.element_detail_type_text))) = 'EEO2A VETERAN' THEN 5
                WHEN upper(trim((canddtl.element_detail_type_text))) = 'EEO1 VETERAN' THEN 6
                WHEN upper(trim((canddtl.element_detail_type_text))) = 'EEO2A SPECIAL DISABLED VETERAN' THEN 7
                WHEN upper(trim((canddtl.element_detail_type_text))) = 'EEO2B SPECIAL DISABLED VETERAN' THEN 8
                WHEN upper(trim((canddtl.element_detail_type_text))) = 'EEO1 SPECIAL DISABLED VETERAN' THEN 9
              END AS responseorder
            FROM
              -- Used to simulate the Coalesce order from the original view DDL
              {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS canddtl
            WHERE upper(trim(canddtl.element_detail_type_text)) IN(
              'OTHER_PROTECTED_VETERAN_2018', 'VETERAN STATUS', 'OTHER_PROTECTED_VETERAN', 'EEO2B VETERAN', 'EEO2A VETERAN', 'EEO1 VETERAN', 'EEO2A SPECIAL DISABLED VETERAN', 'EEO2B SPECIAL DISABALED VETERAN', 'EEO1 SPECIAL DISABLED VETERAN'
            )
             AND DATE(canddtl.valid_to_date) = '9999-12-31'
             AND canddtl.element_detail_seq = 1
            QUALIFY rank() OVER (PARTITION BY canddtl.candidate_sid ORDER BY canddtl.valid_from_date DESC, responseorder) = 1
        ) AS vet ON cand.candidate_sid = vet.candidate_sid
        LEFT OUTER JOIN --  Vet1
        --  Vet2
        --  Vet3
        --  Vet4
        --  Vet5
        --  Vet6
        --  Vet7
        --  Vet8
        --  Vet9
        -- AND CandDtl.Element_Detail_Value_Text IS NOT NULL
        (
          SELECT
              cand_0.candidate_sid,
              cand_0.valid_from_date,
              cand_0.element_detail_type_text,
              cand_0.element_detail_value_text,
              CASE
                WHEN upper(trim((cand_0.element_detail_value_text))) LIKE 'NO%' THEN 'No'
                WHEN upper(trim((cand_0.element_detail_value_text))) LIKE 'YES%' THEN 'Yes'
                WHEN upper(trim((cand_0.element_detail_value_text))) LIKE '%WISH%' THEN 'Not Disclosed'
              END AS disability_flag,
              CASE
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'DISABILITY_2018' THEN 1
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'DISABILITY' THEN 2
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'EEO1 DISABILITY' THEN 3
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'EEO2A DISABILITY' THEN 4
                WHEN upper(trim((cand_0.element_detail_type_text))) = 'EEO2B DISABILITY' THEN 5
              END AS responseorder
            FROM
              {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand_0
            WHERE upper(trim(cand_0.element_detail_type_text)) IN(
              'DISABILITY', 'DISABILITY_2018', 'EEO1 DISABILITY', 'EEO2A DISABILITY', 'EEO2B DISABILITY'
            )
             AND DATE(cand_0.valid_to_date) = '9999-12-31'
             AND cand_0.element_detail_seq = 1
             AND cand_0.element_detail_value_text IS NOT NULL
            QUALIFY rank() OVER (PARTITION BY cand_0.candidate_sid ORDER BY cand_0.valid_from_date DESC, responseorder) = 1
        ) AS dis ON cand.candidate_sid = dis.candidate_sid
      GROUP BY 1, 2, upper(eth.ethnicity_name), upper(vet.veteran_desc), upper(dis.disability_flag)
;


CREATE TEMPORARY TABLE steps_volatile
  AS
    SELECT
        step_durations.recruitment_requisition_sid,
        step_durations.candidate_profile_sid,
        sum(CASE
          WHEN upper(trim((step_durations.step_short_name))) = 'HR REVIEW' THEN step_durations.duration
        END) AS total_hrr_duration,
        sum(CASE
          WHEN upper(trim(step_durations.step_short_name)) IN(
            'HIRING MANAGER REVIEW', 'HM REVIEW'
          ) THEN step_durations.duration
        END) AS total_hmr_duration,
        sum(CASE
          WHEN upper(trim((step_durations.step_short_name))) = 'INTERVIEW' THEN step_durations.duration
        END) AS total_invw_duration,
        sum(CASE
          WHEN upper(trim((step_durations.step_short_name))) = 'OFFER' THEN step_durations.duration
        END) AS total_offer_duration
      FROM
        (
          SELECT
              start_stop_steps.recruitment_requisition_sid,
              start_stop_steps.candidate_profile_sid,
              start_stop_steps.submission_sid,
              start_stop_steps.step_short_name,
              CASE
                WHEN date_diff(CASE
                  WHEN start_stop_steps.event_end = DATE '1800-01-01' THEN CAST(NULL as DATE)
                  ELSE start_stop_steps.event_end
                END, start_stop_steps.event_start, DAY) <= 365 THEN date_diff(start_stop_steps.event_end,start_stop_steps.event_start, HOUR) / 24
                WHEN date_diff(start_stop_steps.event_end, start_stop_steps.event_start, DAY) BETWEEN 365 AND 9999 THEN date_diff(start_stop_steps.event_end,start_stop_steps.event_start, day)
                ELSE CAST(NULL as float64)
              END AS duration
            FROM
              --  less than a year
              --  between 365 and 9999(Max size of DAY(4) interval field)
              --  NULLs and range beyond -9999 and 9999
              (
                SELECT
                    recruitment_requisition_sid,
                    sub.candidate_profile_sid,
                    submission_sid,
                    st.step_short_name,
                    /* Event Date Time is a manually entered field. It can
                    				be incorrectly input. Creation_Date_Time is the
                    				event timestamp. Event_Date_Time appears to truncate
                    				seconds leading to below note. */
                    /* Reportedly and anecdotally, this only happens at the
                    				intervew step, but this is unconfirmed at scale */
                    coalesce(event_date_time, creation_date_time) AS event_start,
                    /* Reach forward and get the next events start time
                    				as this event's end time */
                    /* If you don't also order by submission_tracking_sid this
                    				can be indeterminate because we have multiple steps
                    				that share event times. This may be alleviated by
                    				creation_date_time, but leaving it to be safe */
                    lead(coalesce(event_date_time, creation_date_time), 1) OVER (PARTITION BY str.candidate_profile_sid ORDER BY coalesce(event_date_time, creation_date_time), str.submission_tracking_sid) AS event_end
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.submission AS sub
                    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS str ON sub.candidate_profile_sid = str.candidate_profile_sid
                     AND DATE(str.valid_to_date) = '9999-12-31'
                    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_step AS st ON str.tracking_step_id = st.step_id
                     AND upper(trim(st.step_short_name)) IN(
                      'HR REVIEW', 'HIRING MANAGER REVIEW', 'HM REVIEW', 'INTERVIEW', 'OFFER'
                    )
                  WHERE DATE(str.valid_to_date) = '9999-12-31'
                   AND DATE(sub.valid_to_date) = '9999-12-31'
                   AND sub.recruitment_requisition_sid IS NOT NULL
                   AND str.tracking_step_id IS NOT NULL
                   AND coalesce(event_date_time, creation_date_time) > date_add(current_date('US/Central'), interval -37 MONTH)
              ) AS start_stop_steps
            GROUP BY 1, 2, 3, 4, start_stop_steps.event_end, start_stop_steps.event_start
        ) AS step_durations
      GROUP BY 1, 2
;
-- -Rolling 36 months

CREATE TEMPORARY TABLE sub
  AS
    SELECT DISTINCT
        cp.candidate_profile_num,
        s.submission_sid,
        cp.candidate_num,
        cp.candidate_profile_sid,
        cp.candidate_sid,
        concat(hrdr.coid, coalesce(hrdr.cost_center, hrdr.dept_code, '00000')) AS submission_uid,
        r.dept_sid,
        ff.company_code,
        hrdr.coid,
        CASE
          WHEN upper(concat(coalesce(hrdr.coid, '00000'), coalesce(hrdr.cost_center, '000'))) <> '00000000' THEN concat(hrdr.coid, hrdr.cost_center)
          ELSE CAST(NULL as STRING)
        END AS coid_dept_uid,
        cp.creation_date,
        cp.creation_date_time,
        cp.completion_date,
        cp.completion_date_time,
        cp.recruitment_source_desc,
        cp.recruitment_source_type_desc,
        cp.recruitment_source_auto_filled_sw,
        cp.profile_medium_desc,
        rr.recruitment_requisition_num_text,
        s.recruitment_requisition_sid,
        rr.lawson_requisition_num,
        s.requisition_num as ghr_requisition_num,
        s.matched_from_requisition_num,
        r.requisition_sid,
        stas.status_code,
        osd1.first_offer_start_date,
        r.open_fte_percent,
        rjs.job_schedule_desc,
        cp.paid_source AS paid_source_flag,
        s.last_modified_date,
        status.submission_status_name,
        substep.current_submission_step AS current_submission_step_name,
        substep.quality_ind AS quality_flag,
        motive.motive_name,
        o.offer_grad_flag AS new_grad_flag,
        candidates_volatile.gender_name,
        candidates_volatile.ethnicity_name,
        candidates_volatile.veteran_desc,
        spouse.answer_desc AS veteran_spouse_flag,
        candidates_volatile.disability_flag,
        steps_volatile.total_hrr_duration AS hrr_duration_cnt,
        steps_volatile.total_hmr_duration AS hmr_duration_cnt,
        steps_volatile.total_invw_duration AS interview_duration_cnt,
        steps_volatile.total_offer_duration AS offer_duration_cnt,
        s.matched_candidate_flag,
        CASE
          WHEN upper(trim(cp.recruitment_source_type_desc)) IN(
            'INTERNAL', 'INT INV TO APPLY'
          ) THEN 'Internal'
          ELSE cp.candidate_ie_ind
        END AS candidate_intl_ext_flag,
        s.dw_last_update_date_time,
        s.source_system_code
      FROM
        {{ params.param_hr_base_views_dataset_name }}.submission AS s
        INNER JOIN cp_volatile AS cp ON s.candidate_profile_sid = cp.candidate_profile_sid
        LEFT OUTER JOIN -- -spouse question
        (
          SELECT
              cqa.candidate_sid,
              ca.answer_desc
            FROM
              {{ params.param_hr_views_dataset_name }}.candidate_question_answer AS cqa
              LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.candidate_answer AS ca ON cqa.answer_sid = ca.answer_sid
               AND DATE(ca.valid_to_date) = '9999-12-31'
            WHERE DATE(cqa.valid_to_date) = '9999-12-31'
             AND cqa.question_sid = 531
            QUALIFY row_number() OVER (PARTITION BY cqa.candidate_sid ORDER BY cqa.creation_date DESC) = 1
        ) AS spouse ON s.candidate_sid = spouse.candidate_sid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_status AS status ON s.current_submission_status_id = status.submission_status_id
        LEFT OUTER JOIN (
          SELECT
              ref_submission_step.step_id,
              ref_submission_step.step_short_name,
              CASE
                WHEN upper(trim((ref_submission_step.step_short_name))) = 'BACKGR. CHECK' THEN 'Background Process'
                WHEN upper(trim((ref_submission_step.step_short_name))) = 'HM REVIEW' THEN 'Hiring Manager Review'
                WHEN upper(trim((ref_submission_step.step_short_name))) = 'NEW' THEN 'New Applicant'
                WHEN upper(trim((ref_submission_step.step_short_name))) = 'ONB NOTICE' THEN 'New Hire Notification'
                WHEN upper(trim((ref_submission_step.step_short_name))) = 'PRESCREEN' THEN 'Pre-Employment Screen'
                ELSE ref_submission_step.step_short_name
              END AS current_submission_step,
              CASE
                WHEN upper(trim(ref_submission_step.step_short_name)) NOT IN(
                  'NEW', 'NEW APPLICANT', 'HR REVIEW'
                ) THEN 'Quality'
                ELSE 'Not Quality'
              END AS quality_ind
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_submission_step
        ) AS substep ON s.current_submission_step_id = substep.step_id
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON s.current_submission_status_id = stas.status_sid
         AND DATE(stas.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN (
          SELECT
              recr.recruitment_requisition_sid,
              recr.lawson_requisition_sid,
              recr.recruitment_requisition_num_text,
              recr.lawson_requisition_num,
              recr.requisition_num,
              recr.recruitment_job_sid,
              recr.hiring_manager_user_sid,
              recr.source_system_code,
              recr.approved_sw
            FROM
              {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS recr
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s_0 ON recr.recruitment_requisition_sid = s_0.recruitment_requisition_sid
               AND DATE(s_0.valid_to_date) = '9999-12-31'
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o_0 ON s_0.submission_sid = o_0.submission_sid
               AND DATE(o_0.valid_to_date) = '9999-12-31'
            WHERE DATE(recr.valid_to_date) = '9999-12-31'
             AND recr.lawson_requisition_sid IS NOT NULL
            QUALIFY row_number() OVER (PARTITION BY recr.recruitment_requisition_sid ORDER BY o_0.capture_date DESC, recr.approved_sw DESC, o_0.sequence_num DESC) = 1
        ) AS rr ON s.recruitment_requisition_sid = rr.recruitment_requisition_sid
        LEFT OUTER JOIN /* We have to partition by recruitment requisition SID because that's
        				how we join to the submission. Otherwise, if lawson reqs get
        				re-used and you partition by it, you may not get the rec_req
        				that matches the submission and we'll drop this information */
        (
          SELECT
              r1.requisition_sid,
              r1.process_level_code,
              r1.open_fte_percent,
              d1.dept_sid,
              d1.dept_code,
              r1.lawson_company_num
            FROM
              {{ params.param_hr_base_views_dataset_name }}.requisition AS r1
              LEFT OUTER JOIN (
                SELECT
                    requisition_department.requisition_sid,
                    requisition_department.dept_sid
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.requisition_department
                  WHERE date(requisition_department.valid_to_date) = '9999-12-31'
                   AND requisition_department.dept_sid IS NOT NULL
              ) AS rd1 ON r1.requisition_sid = rd1.requisition_sid
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS d1 ON rd1.dept_sid = d1.dept_sid
               AND DATE(d1.valid_to_date) = '9999-12-31'
            WHERE DATE(r1.valid_to_date) = '9999-12-31'
        ) AS r ON rr.lawson_requisition_sid = r.requisition_sid
        LEFT OUTER JOIN (
          SELECT
              recruitment_job.recruitment_job_sid,
              recruitment_job.job_schedule_id,
              recruitment_job.valid_to_date
            FROM
              {{ params.param_hr_base_views_dataset_name }}.recruitment_job
            WHERE DATE(recruitment_job.valid_to_date) = '9999-12-31'
        ) AS rj ON rr.recruitment_job_sid = rj.recruitment_job_sid
         AND DATE(rj.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN (
          SELECT
              ref_job_schedule.job_schedule_id,
              ref_job_schedule.job_schedule_desc
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_job_schedule
        ) AS rjs ON rj.job_schedule_id = rjs.job_schedule_id
        LEFT OUTER JOIN (
          SELECT
              sst.submission_sid,
              sst.submission_state_id,
              rss.submission_state_desc
            FROM
              {{ params.param_hr_base_views_dataset_name }}.submission_state AS sst
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_state AS rss ON sst.submission_state_id = rss.submission_state_id
            WHERE DATE(sst.valid_to_date) = '9999-12-31'
        ) AS current_status ON s.submission_sid = current_status.submission_sid
        LEFT OUTER JOIN (
          SELECT
              tm.submission_tracking_sid,
              st.candidate_profile_sid,
              rm.motive_name,
              rm.motive_code
            FROM
              {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS st
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.junc_submission_tracking_motive AS tm ON st.submission_tracking_sid = tm.submission_tracking_sid
               AND DATE(tm.valid_to_date) = '9999-12-31'
              LEFT OUTER JOIN -- AND st.valid_from_date = tm.valid_from_date
              {{ params.param_hr_base_views_dataset_name }}.ref_motive AS rm ON tm.tracking_motive_id = rm.motive_id
            WHERE date(st.valid_to_date) = '9999-12-31'
             AND st.candidate_profile_sid IS NOT NULL
            QUALIFY rank() OVER (PARTITION BY st.candidate_profile_sid ORDER BY st.event_date_time DESC, tm.submission_tracking_sid DESC, tm.tracking_motive_id DESC) = 1
        ) AS motive ON cp.candidate_profile_sid = motive.candidate_profile_sid
        LEFT OUTER JOIN /* There can be multiple rejection reasons w/ same Event_Date_Time.
        			This picks the last one entered. */
        (
          SELECT
              rp1.requisition_sid,
              jp1.account_unit_num,
              jp1.gl_company_num
            FROM
              {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp1
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp1 ON rp1.position_sid = jp1.position_sid
               AND DATE(jp1.valid_to_date) = '9999-12-31'
               AND (jp1.eff_to_date) = '9999-12-31'
               AND upper(trim((jp1.active_dw_ind))) = 'Y'
            WHERE DATE(rp1.valid_to_date) = '9999-12-31'
        ) AS rp ON r.requisition_sid = rp.requisition_sid
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdr ON r.process_level_code = hrdr.process_level_code
         AND r.dept_code = hrdr.dept_code
         AND r.lawson_company_num = hrdr.lawson_company_num
         AND rp.account_unit_num = hrdr.account_unit_num
         AND rp.gl_company_num = hrdr.gl_company_num
        LEFT OUTER JOIN (
          SELECT
              o1.submission_sid,
              max(CASE
                WHEN upper(trim(cast(od1.new_grad as string))) IN(
                  '12320', '12340', '12321', '12341'
                ) THEN 'Yes'
                WHEN upper(trim(od1.grad_flagb)) IN(
                  'LOCALRNTRAININGPROGRAM', 'HWSRNTRAININGPROGRAM'
                ) THEN 'Yes'
                ELSE 'No'
              END) AS offer_grad_flag
            FROM
              (
                SELECT
                    ao.offer_sid,
                    ao.submission_sid
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.offer AS ao
                  WHERE DATE(ao.valid_to_date) = '9999-12-31'
                  QUALIFY rank() OVER (PARTITION BY ao.submission_sid ORDER BY ao.sequence_num DESC) = 1
              ) AS o1
              LEFT OUTER JOIN (
                SELECT
                    offer_detail.element_detail_id AS new_grad,
                    offer_detail.offer_sid,
                    offer_detail.element_detail_value_text AS grad_flagb
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.offer_detail
                  WHERE upper(trim(offer_detail.element_detail_type_text)) IN(
                    'NEW - GRAD NURSE RECRUITMENT TRACKING', 'GRADUATENURSERECRUITMENTTRACKING', 'RN_TRAINING_PROGRAM'
                  )
                   AND DATE(offer_detail.valid_to_date) = '9999-12-31'
                  GROUP BY 1, 3, 2, offer_detail.element_detail_type_text
              ) AS od1 ON o1.offer_sid = od1.offer_sid
            GROUP BY 1, upper(CASE
              WHEN upper(trim(cast(od1.new_grad as string))) IN(
                '12320', '12340', '12321', '12341'
              ) THEN 'Yes'
              WHEN upper(trim(od1.grad_flagb)) IN(
                'LOCALRNTRAININGPROGRAM', 'HWSRNTRAININGPROGRAM'
              ) THEN 'Yes'
              ELSE 'No'
            END)
        ) AS o ON s.submission_sid = o.submission_sid
        LEFT OUTER JOIN candidates_volatile ON cp.candidate_sid = candidates_volatile.candidate_sid
        LEFT OUTER JOIN -- Step Durations for RIO calcuation
        steps_volatile ON rr.recruitment_requisition_sid = steps_volatile.recruitment_requisition_sid
         AND cp.candidate_profile_sid = steps_volatile.candidate_profile_sid
        LEFT OUTER JOIN -- Add Company_Code
        {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON ff.coid = hrdr.coid
        --HDM-2268 Get the earliest valid Start_Date in Offer
        LEFT JOIN (
          SELECT
            submission_sid,
            start_date as first_offer_start_date
          FROM {{ params.param_hr_base_views_dataset_name }}.offer
          WHERE UPPER(source_system_code) = 'B' and start_date IS NOT NULL
          QUALIFY RANK() OVER(partition by submission_sid order by valid_from_date asc) = 1
        ) osd1
        on s.submission_sid = osd1.submission_sid
      WHERE DATE(s.valid_to_date) = '9999-12-31'
       AND s.candidate_profile_sid IS NOT NULL
;

--delete prior data
DELETE FROM {{ params.param_hr_core_dataset_name }}.fact_submission where true;

-- INSERT data to core
INSERT INTO {{ params.param_hr_core_dataset_name }}.fact_submission (
	submission_sid
	,candidate_profile_num
	,candidate_num
	,candidate_profile_sid
	,candidate_sid
	,submission_uid
	,dept_sid
	,company_code
	,coid
	,coid_dept_uid
	,creation_date
  ,creation_date_time
	,completion_date
  ,completion_date_time
	,recruitment_source_desc
	,recruitment_source_type_desc
	,recruitment_source_auto_filled_sw
	,profile_medium_desc
	,recruitment_requisition_num_text
	,recruitment_requisition_sid
	,lawson_requisition_num
  ,ghr_requisition_num
	,matched_from_requisition_num
	,requisition_sid
	,status_code
  ,first_offer_start_date
	,open_fte_percent
	,job_schedule_desc
	,paid_source_flag
	,last_modified_date
	,submission_status_name
	,current_submission_step_name
	,quality_flag
	,motive_name
	,new_grad_flag
	,gender_name
	,ethnicity_name
	,veteran_desc
	,veteran_spouse_flag
	,disability_flag
	,hrr_duration_cnt
	,hmr_duration_cnt
	,interview_duration_cnt
	,offer_duration_cnt
	,matched_candidate_flag
	,candidate_intl_ext_flag
	,source_system_code
	,dw_last_update_date_time
)

select 

	submission_sid
	,candidate_profile_num
	,candidate_num
	,candidate_profile_sid
	,candidate_sid
	,submission_uid
	,dept_sid
	,company_code
	,coid
	,coid_dept_uid
	,creation_date
  ,creation_date_time
	,completion_date
  ,completion_date_time
	,recruitment_source_desc
	,recruitment_source_type_desc
	,recruitment_source_auto_filled_sw
	,profile_medium_desc
	,recruitment_requisition_num_text
	,recruitment_requisition_sid
	,lawson_requisition_num
  ,ghr_requisition_num
	,matched_from_requisition_num
	,requisition_sid
	,status_code
  ,first_offer_start_date
	,open_fte_percent
	,job_schedule_desc
	,paid_source_flag
	,last_modified_date
	,submission_status_name
	,current_submission_step_name
	,quality_flag
	,motive_name
	,new_grad_flag
	,gender_name
	,ethnicity_name
	,veteran_desc
	,veteran_spouse_flag
	,disability_flag
	,cast(hrr_duration_cnt as int64)
	,cast(hmr_duration_cnt as int64)
	,cast(interview_duration_cnt as int64)
	,cast(offer_duration_cnt as int64)
	,matched_candidate_flag
	,candidate_intl_ext_flag
	,source_system_code
	,dw_last_update_date_time

FROM

sub;
END;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
