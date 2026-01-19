DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
BEGIN
  SET _ERROR_CODE = 0;
  -- No target-dialect support for source-dialect-specific SET
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/*CREATE VOLATILE TABLE current_emp AS
(
	SELECT
		emp.employee_sid,
		emp.employee_num
	FROM edw.employee emp
	WHERE emp.Valid_To_Date = '9999-12-31'*/
/*) WITH DATA
PRIMARY INDEX(employee_sid, employee_num)
ON COMMIT PRESERVE ROWS;*/
-- .IF Errorcode <> 0 THEN .QUIT Errorcode;
BEGIN
  SET _ERROR_CODE = 0;
  DROP TABLE edw.employee_work;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
-- .IF Errorcode <> 0 THEN .QUIT Errorcode;
BEGIN
  SET _ERROR_CODE = 0;
  CREATE TABLE edw.employee_work
    AS
      SELECT
          trim(stg.employee_first_name) AS first_name,
          trim(stg.employee_last_name) AS last_name,
          stg.employee_num AS employee_num
        FROM
          edw.employee_stg stg
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
-- ORDER BY 1,2,4,5,6,7,8,10,14,13
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
CALL dbadmin_procs.collect_stats_table('EDW', 'EMPLOYEE_WORK');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;