BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

set
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.company_pay_grade_schedule_wrk1;

BEGIN TRANSACTION;

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.company_pay_grade_schedule_wrk1 (
    company_pay_schedule_sid,
    pay_grade_code,
    pay_step_num,
    eff_from_date,
    valid_from_date,
    eff_to_date,
    pay_schedule_code,
    grade_sequence_num,
    pay_rate_amt,
    lawson_company_num,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  cps.company_pay_schedule_sid AS company_pay_schedule_sid,
  pr.pay_grade AS pay_grade_code,
  pr.pay_step AS pay_step_num,
  pr.effect_date AS eff_from_date,
  current_ts AS valid_from_date,
  date(datetime("9999-12-31 23:59:59")) AS eff_to_date,
  pr.r_schedule AS pay_schedule_code,
  pr.grade_seq AS grade_sequence_num,
  pr.pay_rate AS pay_rate_amt,
  pr.company AS lawson_company_num,
  'Y' AS active_dw_ind,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.prsagdtl AS pr
  LEFT OUTER JOIN (
    SELECT
      DISTINCT company_pay_schedule.company_pay_schedule_sid,
      company_pay_schedule.pay_schedule_code,
      company_pay_schedule.lawson_company_num
    FROM
      {{ params.param_hr_base_views_dataset_name }}.company_pay_schedule
    WHERE
      date(company_pay_schedule.valid_to_date) = date('9999-12-31')
      AND upper(company_pay_schedule.source_system_code) = 'L'
    GROUP BY
      1,
      2,
      3
  ) AS cps ON cps.lawson_company_num = pr.company
  AND cps.pay_schedule_code = pr.r_schedule;

UPDATE
  {{ params.param_hr_core_dataset_name }}.company_pay_grade_schedule AS cpgs
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.company_pay_grade_schedule_wrk1 AS stg
WHERE
  trim(cast(cpgs.company_pay_schedule_sid as string)) = trim(cast(stg.company_pay_schedule_sid as string))
  AND trim(cpgs.pay_grade_code) = trim(stg.pay_grade_code)
  AND cpgs.source_system_code = stg.source_system_code
  AND cpgs.eff_from_date = stg.eff_from_date
  AND trim(cast(cpgs.pay_step_num as string)) = trim(cast(stg.pay_step_num as string))
  AND (
    upper(trim(coalesce(cpgs.pay_schedule_code, ''))) <> upper(trim(coalesce(stg.pay_schedule_code, '')))
    OR upper(
      trim(
        coalesce(cast(cpgs.grade_sequence_num as string), '')
      )
    ) <> upper(
      trim(
        coalesce(cast(stg.grade_sequence_num as string), '')
      )
    )
    OR upper(
      trim(coalesce(cast(cpgs.pay_rate_amt as string), ''))
    ) <> upper(
      trim(coalesce(cast(stg.pay_rate_amt as string), ''))
    )
    OR trim(cast(cpgs.lawson_company_num as string)) <> trim(cast(stg.lawson_company_num as string))
  )
  AND upper(cpgs.active_dw_ind) = 'Y'
  AND cpgs.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.company_pay_grade_schedule AS cpgs
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
WHERE
  upper(cpgs.active_dw_ind) = 'Y' 
  AND (
    (cpgs.pay_step_num),
    trim(cpgs.pay_grade_code),
    trim(cpgs.pay_schedule_code),
    cpgs.lawson_company_num,
    cpgs.eff_from_date
  ) NOT IN(
    SELECT
      AS STRUCT pay_step,
      trim(pay_grade),
      trim(r_schedule),
      company,
      effect_date
    FROM
      {{ params.param_hr_stage_dataset_name }}.prsagdtl
  )
  AND cpgs.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.company_pay_grade_schedule (
    company_pay_schedule_sid,
    pay_grade_code,
    pay_step_num,
    eff_from_date,
    valid_from_date,
    valid_to_date,
    pay_schedule_code,
    grade_sequence_num,
    pay_rate_amt,
    lawson_company_num,
    process_level_code,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  company_pay_grade_schedule_wrk1.company_pay_schedule_sid,
  company_pay_grade_schedule_wrk1.pay_grade_code,
  company_pay_grade_schedule_wrk1.pay_step_num,
  company_pay_grade_schedule_wrk1.eff_from_date,
  current_ts,
  datetime("9999-12-31 23:59:59"),
  company_pay_grade_schedule_wrk1.pay_schedule_code,
  company_pay_grade_schedule_wrk1.grade_sequence_num,
  company_pay_grade_schedule_wrk1.pay_rate_amt,
  company_pay_grade_schedule_wrk1.lawson_company_num,
  '00000' AS process_level_code,
  company_pay_grade_schedule_wrk1.active_dw_ind,
  company_pay_grade_schedule_wrk1.source_system_code,
  current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.company_pay_grade_schedule_wrk1
WHERE
  (
    trim(
      cast(
        company_pay_grade_schedule_wrk1.company_pay_schedule_sid as string
      )
    ),
    trim(company_pay_grade_schedule_wrk1.pay_grade_code),
    trim(
      cast(
        company_pay_grade_schedule_wrk1.pay_step_num as string
      )
    ),
    upper(
      trim(
        coalesce(
          cast(
            company_pay_grade_schedule_wrk1.grade_sequence_num as string
          ),
          ''
        )
      )
    ),
    upper(
      trim(
        coalesce(
          cast(
            company_pay_grade_schedule_wrk1.pay_rate_amt as string
          ),
          ''
        )
      )
    ),
    company_pay_grade_schedule_wrk1.eff_from_date
  ) NOT IN(
    SELECT
      AS STRUCT trim(cast(company_pay_schedule_sid as string)),
      trim(cast(pay_grade_code as string)),
      trim(cast(pay_step_num as string)),
      upper(
        trim(coalesce(cast(grade_sequence_num as string), ''))
      ),
      upper(trim(coalesce(cast(pay_rate_amt as string), ''))),
      eff_from_date
    FROM
      {{ params.param_hr_base_views_dataset_name }}.company_pay_grade_schedule
    WHERE
      upper(active_dw_ind) = 'Y'
      and upper(source_system_code) = 'L'
  );

SET
  DUP_COUNT = (
    SELECT
      COUNT(*)
    FROM
      (
        SELECT
          Company_Pay_Schedule_SID,
          Pay_Grade_Code,
          Pay_Step_Num,
          Eff_From_Date,
          Valid_From_Date
        FROM
          {{ params.param_hr_core_dataset_name }}.company_pay_grade_schedule
        GROUP BY
          Company_Pay_Schedule_SID,
          Pay_Grade_Code,
          Pay_Step_Num,
          Eff_From_Date,
          Valid_From_Date
        HAVING
          COUNT(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = CONCAT(
  'Duplicates are not allowed in the table:{{ params.param_hr_core_dataset_name }} .Company_Pay_Grade_Schedule'
);

ELSE COMMIT TRANSACTION;

END IF;

END;