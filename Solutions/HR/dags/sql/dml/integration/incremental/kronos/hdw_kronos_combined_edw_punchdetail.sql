BEGIN
  DECLARE DUP_COUNT INT64;

/* DELETE FROM STAGING TABLE*/
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.kronos_combined_edw_punchdetail;

BEGIN TRANSACTION;
  /*  LOAD STAGING TABLE WITH REJ DATA */
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.kronos_combined_edw_punchdetail 
      SELECT
          kronos_clock_library
          ,employee_num
          ,kronos_time_id
          ,clock_code
          ,actual_time_id
          ,actual_time_out
          ,actual_elapsed_time_hours
          ,rounded_time_in
          ,rounded_time_out
          ,rounded_elapsed_time_hours
          ,approval_date
          ,approver_3_4
          ,shift_start_date
          ,pay_type
          ,long_meal
          ,other_dept
          ,out_of_ppd
          ,short_meal
          ,department
          ,hr_company
          ,process_level
          ,ppd_start_date
          ,ppd_end_date
          ,source_system_code
          ,posted_ind
          ,dw_last_update_date_time
      FROM {{ params.param_hr_stage_dataset_name }}.kronos_combined_edw_punchdetail_rej
      WHERE kronos_combined_edw_punchdetail_rej.process_level IS NOT NULL
      AND kronos_combined_edw_punchdetail_rej.hr_company IS NOT NULL
             ;

  /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Kronos_Clock_Library , Employee_Num ,Kronos_Time_Id
        from {{ params.param_hr_stage_dataset_name }}.kronos_combined_edw_punchdetail
        group by Kronos_Clock_Library ,Employee_Num ,Kronos_Time_Id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: edwhr_staging_copy.kronos_combined_edw_punchdetail');
    ELSE
      COMMIT TRANSACTION;
    END IF;

/* DELETE FROM REJ STAGING TABLE WHERE Process_Level is not null AND HR_Company is not null*/

DELETE FROM {{ params.param_hr_stage_dataset_name }}.kronos_combined_edw_punchdetail_rej WHERE kronos_combined_edw_punchdetail_rej.process_level IS NOT NULL
   AND kronos_combined_edw_punchdetail_rej.hr_company IS NOT NULL;

END;