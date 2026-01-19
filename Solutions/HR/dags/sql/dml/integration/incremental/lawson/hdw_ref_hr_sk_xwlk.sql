BEGIN  
  DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

/*  Load Work Table with working Data */

BEGIN TRANSACTION ;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_hr_sk_xwlk (hr_sk, hr_sk_type_text, hr_sk_source_text, hr_sk_generated_date_time, source_system_code, dw_last_update_date_time)
        SELECT
        ref_sk_xwlk.sk AS hr_sk,
        ref_sk_xwlk.sk_type AS hr_sk_type_text,
        ref_sk_xwlk.sk_source_txt AS hr_sk_source_text,
        ref_sk_xwlk.sk_generated_date_time AS hr_sk_generated_date_time,
        'L' AS source_system_code,
        current_datetime() AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk
      -- WHERE (sk, sk_type) NOT IN(
      --   SELECT AS STRUCT
      --       sk ,
      --       sk_type
      --     FROM
      --       {{ params.param_hr_core_dataset_name }}.ref_hr_sk_xwlk

      left outer join {{ params.param_hr_core_dataset_name }}.ref_hr_sk_xwlk b
      on ref_sk_xwlk.sk = b.hr_sk
      and ref_sk_xwlk.sk_type = b.hr_sk_type_text 
      where (b.hr_sk is null and b.hr_sk_type_text is null);
      
  
/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select hr_sk ,hr_sk_type_text
        from {{ params.param_hr_core_dataset_name }}.ref_hr_sk_xwlk
        group by hr_sk ,hr_sk_type_text	
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ef_hr_sk_xwlk');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END ;


