DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
BEGIN
  SET _ERROR_CODE = 0;

EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;

BEGIN
  SET _ERROR_CODE = 0;
  DROP TABLE edw.employee_work;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;

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

IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
CALL dbadmin_procs.collect_stats_table('EDW', 'EMPLOYEE_WORK');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;