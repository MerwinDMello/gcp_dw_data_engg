BEGIN 
declare  dup_count int64;
CREATE TEMPORARY TABLE sub
  AS
SELECT
        hrmet.analytics_msr_sid,
        hrmet.position_sid,
        hrmet.requisition_sid,
        hrmet.date_id,
        max(concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num)) AS process_level_uid,
        hrmet.process_level_code,
        hrmet.coid,
        hrmet.company_code,
        hrdr.cost_center AS dept_num,
        max(CASE
          WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
          WHEN jobco.job_code IN(
            ndir.job_code
          ) THEN 'Leadership'
          WHEN upper(trim(jobco.job_code_desc)) = upper(trim(pct.job_code_desc))
           AND upper(trim(jobpos.position_code_desc)) = upper(trim(pct.job_title_text)) THEN 'PCT'
          ELSE 'Non-RN'
        END) AS rn_group_name,
        req.requisition_num,
        hrmet.requisition_approval_date,
        req.requisition_closed_date,
        hrmet.key_talent_id,
        concat((hrmet.position_sid), jobpos.eff_to_date) AS position_key,
        jobpos.schedule_work_code,
        hrc.hr_code_desc AS schedule_work_code_desc,
        req.open_fte_percent,
        max(CASE
          WHEN (upper(jobpos.position_code_desc) LIKE '%PRN I'
           OR upper(jobpos.position_code_desc) LIKE '%PRN')
           AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 1'
          WHEN upper(jobpos.position_code_desc) LIKE '%PRN II'
           AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 2'
          WHEN upper(jobpos.position_code_desc) LIKE '%PRN III'
           AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 3'
          ELSE CAST(NULL as STRING)
        END) AS prn_tier_text,
        max(CASE
          WHEN stas.status_code IN(
            '01', '02'
          )
           OR (upper(jobpos.position_code_desc) LIKE '%PRN II'
           OR upper(jobpos.position_code_desc) LIKE '%PRN III') THEN 'Core'
          ELSE 'Flex'
        END) AS workforce_category_text,
        hrmet.integrated_lob_id,
        stas.status_code,
        hrmet.metric_numerator_qty,
        hrmet.source_system_code,
        hrmet.dw_last_update_date_time
      FROM
        {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS hrmet
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
         AND date(dept.valid_to_date) = date'9999-12-31'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdr ON hrmet.coid = hrdr.coid
         AND hrmet.process_level_code = hrdr.process_level_code
         AND hrmet.dept_sid = hrdr.dept_sid
         AND hrmet.lawson_company_num = hrdr.lawson_company_num
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jobco ON hrmet.job_code_sid = jobco.job_code_sid
         AND date(jobco.valid_to_date) = date'9999-12-31'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jobcl ON hrmet.job_class_sid = jobcl.job_class_sid
         AND date(jobcl.valid_to_date) = date'9999-12-31'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS req ON hrmet.requisition_sid = req.requisition_sid
         AND date(req.valid_to_date) = date'9999-12-31'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS ndir ON jobco.job_code = ndir.job_code
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS reqpos ON req.requisition_sid = reqpos.requisition_sid
         AND date(reqpos.valid_to_date) = date'9999-12-31'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobpos ON reqpos.position_sid = jobpos.position_sid
         AND date(jobpos.valid_to_date) = date'9999-12-31'
         AND hrmet.date_id BETWEEN jobpos.eff_from_date AND jobpos.eff_to_date
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS hrc ON jobpos.schedule_work_code = hrc.hr_code
         AND upper(hrc.hr_type_code) = 'WS'
         AND upper(hrc.active_ind) = 'A'
        LEFT OUTER JOIN --  7/21/2021 Added by Stephen for Schedule_Work_Code_Desc (Shift Description)
        {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pct ON upper(trim(jobpos.position_code_desc)) = upper(trim(pct.job_title_text))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON req.application_status_sid = stas.status_sid
         AND date(stas.valid_to_date) = date'9999-12-31'
      WHERE (hrmet.analytics_msr_sid) = 80400
       AND hrmet.date_id > date_add(current_date('US/Central'), interval -37 MONTH)
      GROUP BY 1, 2, 3, 4, upper(concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num)), 6, 7, 8, 9, upper(CASE
        WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
        WHEN jobco.job_code IN(
          ndir.job_code
        ) THEN 'Leadership'
        WHEN upper(trim(jobco.job_code_desc)) = upper(trim(pct.job_code_desc))
         AND upper(trim(jobpos.position_code_desc)) = upper(trim(pct.job_title_text)) THEN 'PCT'
        ELSE 'Non-RN'
      END), 11, 12, 13, 14, 15, 16, 17, 18, upper(CASE
        WHEN (upper(jobpos.position_code_desc) LIKE '%PRN I'
         OR upper(jobpos.position_code_desc) LIKE '%PRN')
         AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 1'
        WHEN upper(jobpos.position_code_desc) LIKE '%PRN II'
         AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 2'
        WHEN upper(jobpos.position_code_desc) LIKE '%PRN III'
         AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 3'
        ELSE CAST(NULL as STRING)
      END), upper(CASE
        WHEN stas.status_code IN(
          '01', '02'
        )
         OR (upper(jobpos.position_code_desc) LIKE '%PRN II'
         OR upper(jobpos.position_code_desc) LIKE '%PRN III') THEN 'Core'
        ELSE 'Flex'
      END), 21, 22, 23, 24, 25
;
-- --Rolling 36 months

-- delete prior data
TRUNCATE TABLE {{ params.param_hr_core_dataset_name }}.fact_vacancy_daily;
--  INSERT data to core

BEGIN TRANSACTION;
INSERT INTO {{ params.param_hr_core_dataset_name }}.fact_vacancy_daily (analytics_msr_sid, position_sid, requisition_sid, date_id, process_level_uid, process_level_code, coid, company_code, dept_num, rn_group_name, requisition_num, requisition_approval_date, requisition_closed_date, key_talent_id, position_key, schedule_work_code, schedule_work_code_desc, open_fte_percent, prn_tier_text, workforce_category_text, integrated_lob_id, status_code, metric_numerator_qty, source_system_code, dw_last_update_date_time)
  SELECT
      sub.analytics_msr_sid,
      sub.position_sid,
      sub.requisition_sid,
      sub.date_id,
      sub.process_level_uid,
      sub.process_level_code,
      sub.coid,
      sub.company_code,
      sub.dept_num,
      sub.rn_group_name,
      sub.requisition_num,
      sub.requisition_approval_date,
      sub.requisition_closed_date,
      sub.key_talent_id,
      sub.position_key,
      sub.schedule_work_code,
      sub.schedule_work_code_desc,
      sub.open_fte_percent,
      sub.prn_tier_text,
      sub.workforce_category_text,
      sub.integrated_lob_id,
      sub.status_code,
      sub.metric_numerator_qty,
      sub.source_system_code,
      sub.dw_last_update_date_time
    FROM
      sub
;
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Analytics_Msr_SID ,Position_SID ,Requisition_SID ,Date_Id
        from  {{ params.param_hr_core_dataset_name }}.fact_vacancy_daily
        group by Analytics_Msr_SID ,Position_SID ,Requisition_SID ,Date_Id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      drop table pos_date;
      drop table min_pos;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table:  {{ params.param_hr_core_dataset_name }}.fact_vacancy_daily');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;
