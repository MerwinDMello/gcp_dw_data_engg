BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
BEGIN TRANSACTION;

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_pay_summary_group AS tgt USING (
    SELECT
        trim(psg.pay_sum_grp) AS pay_summary_group_code,
        psg.company AS lawson_company_num,
        trim(psg.description) AS pay_summary_group_desc,
        trim(psg.check_desc) AS pay_summary_abbreviation_desc,
        trim(psg.elig_ot_pay) AS overtime_eligibility_pay_ind,
        trim(psg.elig_ot_hrs) AS overtime_eligibility_hour_ind,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.paysumgrp AS psg
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    
      ) AS stg
  ON tgt.pay_summary_group_code = stg.pay_summary_group_code
   AND tgt.lawson_company_num = stg.lawson_company_num
     WHEN MATCHED THEN UPDATE SET pay_summary_group_desc = stg.pay_summary_group_desc, pay_summary_abbreviation_desc = stg.pay_summary_abbreviation_desc, overtime_eligibility_pay_ind = stg.overtime_eligibility_pay_ind, overtime_eligibility_hour_ind = stg.overtime_eligibility_hour_ind, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (pay_summary_group_code, lawson_company_num, pay_summary_group_desc, pay_summary_abbreviation_desc, overtime_eligibility_pay_ind, overtime_eligibility_hour_ind, source_system_code, dw_last_update_date_time) VALUES (stg.pay_summary_group_code, stg.lawson_company_num, stg.pay_summary_group_desc, stg.pay_summary_abbreviation_desc, stg.overtime_eligibility_pay_ind, stg.overtime_eligibility_hour_ind, stg.source_system_code, stg.dw_last_update_date_time)
  ;

/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Pay_Summary_Group_Code ,Lawson_Company_Num
        from {{ params.param_hr_core_dataset_name }}.ref_pay_summary_group
        group by Pay_Summary_Group_Code ,Lawson_Company_Num
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_pay_summary_group');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;