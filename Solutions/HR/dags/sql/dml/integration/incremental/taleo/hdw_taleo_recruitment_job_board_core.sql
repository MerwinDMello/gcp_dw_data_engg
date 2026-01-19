

BEGIN

DECLARE dup_count INT64;
DECLARE current_dt DATETIME;
SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
BEGIN TRANSACTION;


  
  UPDATE {{ params.param_hr_core_dataset_name }}.junc_recruitment_job_board AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) FROM {{ params.param_hr_stage_dataset_name }}.recruitment_job_board_wrk AS wrk WHERE wrk.recruitment_job_sid = tgt.recruitment_job_sid
   AND wrk.job_board_id = tgt.job_board_id
   AND (wrk.posting_board_type_id <> tgt.posting_board_type_id
   OR wrk.posting_status_id <> tgt.posting_status_id
   OR wrk.posting_date <> tgt.posting_date
   OR wrk.unposting_date <> tgt.unposting_date
   OR coalesce(trim(wrk.source_system_code), '') <> coalesce(trim(tgt.source_system_code), ''))
   AND DATE(tgt.valid_to_date) = '9999-12-31';


  INSERT INTO {{ params.param_hr_core_dataset_name }}.junc_recruitment_job_board (recruitment_job_sid, job_board_id, valid_from_date, posting_board_type_id, posting_status_id, valid_to_date, posting_date, unposting_date, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.recruitment_job_sid,
        wrk.job_board_id,
        current_dt AS valid_from_date,
        wrk.posting_board_type_id,
        wrk.posting_status_id,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        wrk.posting_date,
        wrk.unposting_date,
        wrk.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.recruitment_job_board_wrk AS wrk
      WHERE (wrk.recruitment_job_sid, wrk.job_board_id, wrk.posting_board_type_id, wrk.posting_status_id, wrk.posting_date, wrk.unposting_date, wrk.source_system_code) NOT IN(
        SELECT AS STRUCT
            tgt.recruitment_job_sid,
            tgt.job_board_id,
            tgt.posting_board_type_id,
            tgt.posting_status_id,
            tgt.posting_date,
            tgt.unposting_date,
            tgt.source_system_code
          FROM
            {{ params.param_hr_base_views_dataset_name }}.junc_recruitment_job_board AS tgt
          WHERE DATE(tgt.valid_to_date) = '9999-12-31'
      )
  ;
  		SET dup_count = ( 
			select count(*)
			from (
			select
				recruitment_job_sid ,job_board_id,valid_from_date 
			from {{ params.param_hr_core_dataset_name }}.junc_recruitment_job_board
			group by recruitment_job_sid ,job_board_id,valid_from_date 
			having count(*) > 1
			)
		);
		IF dup_count <> 0 THEN
		  ROLLBACK TRANSACTION;
		  RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.junc_recruitment_job_board');
		ELSE
		  COMMIT TRANSACTION;
		END IF;
END;
