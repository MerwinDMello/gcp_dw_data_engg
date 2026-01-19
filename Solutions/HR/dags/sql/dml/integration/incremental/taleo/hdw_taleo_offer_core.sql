 BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = datetime_trunc(current_datetime('US/Central'), second);

  BEGIN TRANSACTION;
  
  /*  Insert the New Records/Chnages into the Target Table  */
  UPDATE {{ params.param_hr_core_dataset_name }}.offer AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = stg.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.offer_wrk AS stg WHERE tgt.offer_sid = stg.offer_sid
   AND (coalesce(tgt.offer_num, -999) <> coalesce(stg.offer_num, -999)
   OR trim(CAST(coalesce(tgt.submission_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.submission_sid, -999) as STRING))
   OR coalesce(tgt.sequence_num, -999) <> coalesce(stg.sequence_num, -999)
   OR upper(trim(coalesce(tgt.offer_name, 'X'))) <> upper(trim(coalesce(stg.offer_name, 'X')))
   OR coalesce(tgt.accept_date, DATE '1901-01-01') <> coalesce(stg.accept_date, DATE '1901-01-01')
   OR coalesce(tgt.start_date, DATE '1901-01-01') <> coalesce(stg.start_date, DATE '1901-01-01')
   OR coalesce(tgt.extend_date, DATE '1901-01-01') <> coalesce(stg.extend_date, DATE '1901-01-01')
   OR coalesce(tgt.last_modified_date, DATE '1901-01-01') <> coalesce(stg.last_modified_date, DATE '1901-01-01')
   OR coalesce(tgt.last_modified_time, TIME '00:00:00') <> coalesce(stg.last_modified_time, TIME '00:00:00')
   OR coalesce(tgt.capture_date, DATE '1901-01-01') <> coalesce(stg.capture_date, DATE '1901-01-01')
   OR trim(CAST(coalesce(tgt.salary_amt, -999) as STRING)) <> trim(CAST(coalesce(stg.salary_amt, -999) as STRING))
   OR trim(CAST(coalesce(tgt.sign_on_bonus_amt, -999) as STRING)) <> trim(CAST(coalesce(stg.sign_on_bonus_amt, -999) as STRING))
   OR trim(CAST(coalesce(tgt.salary_pay_basis_amt, -999) as STRING)) <> trim(CAST(coalesce(stg.salary_pay_basis_amt, -999) as STRING))
   OR coalesce(tgt.hours_per_day_num, -999) <> coalesce(stg.hours_per_day_num, -999)
   OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(stg.source_system_code), 'X'))
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");


  INSERT INTO {{ params.param_hr_core_dataset_name }}.offer (offer_sid, valid_from_date, valid_to_date, offer_num, submission_sid, sequence_num, offer_name, accept_date, start_date, extend_date, last_modified_date, last_modified_time, capture_date, salary_amt, sign_on_bonus_amt, salary_pay_basis_amt, hours_per_day_num, source_system_code, dw_last_update_date_time)
    SELECT
        stg.offer_sid,
        current_dt,
        DATETIME("9999-12-31 23:59:59"),
        stg.offer_num,
        stg.submission_sid,
        stg.sequence_num,
        stg.offer_name,
        stg.accept_date,
        stg.start_date,
        stg.extend_date,
        stg.last_modified_date,
        stg.last_modified_time,
        stg.capture_date,
        cast(stg.salary_amt as NUMERIC),
        cast(stg.sign_on_bonus_amt as NUMERIC),
        cast(stg.salary_pay_basis_amt as NUMERIC),
        cast(stg.hours_per_day_num as NUMERIC),
        stg.source_system_code,
        datetime_trunc(current_datetime('US/Central'), second) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.offer_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS tgt ON stg.offer_sid = tgt.offer_sid
         AND coalesce(tgt.offer_num, -999) = coalesce(stg.offer_num, -999)
         AND trim(CAST(coalesce(tgt.submission_sid, -999) as STRING)) = trim(CAST(coalesce(stg.submission_sid, -999) as STRING))
         AND coalesce(tgt.sequence_num, -999) = coalesce(stg.sequence_num, -999)
         AND upper(trim(coalesce(tgt.offer_name, 'X'))) = upper(trim(coalesce(stg.offer_name, 'X')))
         AND coalesce(tgt.accept_date, DATE '1901-01-01') = coalesce(stg.accept_date, DATE '1901-01-01')
         AND coalesce(tgt.start_date, DATE '1901-01-01') = coalesce(stg.start_date, DATE '1901-01-01')
         AND coalesce(tgt.extend_date, DATE '1901-01-01') = coalesce(stg.extend_date, DATE '1901-01-01')
         AND coalesce(tgt.last_modified_date, DATE '1901-01-01') = coalesce(stg.last_modified_date, DATE '1901-01-01')
         AND coalesce(tgt.last_modified_time, TIME '00:00:00') = coalesce(stg.last_modified_time, TIME '00:00:00')
         AND coalesce(tgt.capture_date, DATE '1901-01-01') = coalesce(stg.capture_date, DATE '1901-01-01')
         AND trim(CAST(coalesce(tgt.salary_amt, -999) as STRING)) = trim(CAST(coalesce(stg.salary_amt, -999) as STRING))
         AND trim(CAST(coalesce(tgt.sign_on_bonus_amt, -999) as STRING)) = trim(CAST(coalesce(stg.sign_on_bonus_amt, -999) as STRING))
         AND trim(CAST(coalesce(tgt.salary_pay_basis_amt, -999) as STRING)) = trim(CAST(coalesce(stg.salary_pay_basis_amt, -999) as STRING))
         AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND coalesce(tgt.hours_per_day_num, -999) = coalesce(stg.hours_per_day_num, -999)
         AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
      WHERE tgt.offer_sid IS NULL
  ;


    /* Test Unique Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            offer_sid ,valid_from_date
        from {{ params.param_hr_core_dataset_name }}.offer
        group by offer_sid ,valid_from_date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.offer');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;