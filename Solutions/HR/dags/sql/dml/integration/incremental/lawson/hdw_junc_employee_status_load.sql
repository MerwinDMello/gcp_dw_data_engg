BEGIN

/*changed for Base view */
  DECLARE DUP_COUNT INT64;
  DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.status_wk1;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.status_wk1 
    SELECT
        a.company,
        a.a_field,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              hrempusf.company,
              hrempusf.a_field
            FROM
              {{ params.param_hr_stage_dataset_name }}.hrempusf
            WHERE upper(hrempusf.field_key) = '88'
        ) AS a
  ;

/*	Truncate Wrk Table		*/

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.junc_employee_status_wrk;

/*  Insert the records into the Work Table  */

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.junc_employee_status_wrk (employee_sid, status_sid, status_type_code, status_code, employee_num, valid_from_date, valid_to_date, delete_ind, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        stg.employee_sid,
        stg.status_sid,
        stg.status_type_code,
        stg.status_code,
        stg.employee_num,
    current_ts AS valid_from_date,
datetime("9999-12-31 23:59:59") AS valid_to_date,
        stg.delete_ind,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        (
          SELECT
              coalesce(eid.employee_sid, 0) AS employee_sid,
              coalesce(sta.status_sid, 0) AS status_sid,
              'EMP' AS status_type_code,
              trim(emp.emp_status) AS status_code,
              emp.employee AS employee_num,
              CASE
                WHEN upper(emphr.delete_ind) = 'D' THEN 'D'
                ELSE 'A'
              END AS delete_ind,
              emp.company AS lawson_company_num,
              emp.process_level AS process_level_code,
              'L' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee AS emp
              LEFT OUTER JOIN (
                SELECT
                    employee.employee_sid,
                    employee.employee_num,
                    employee.lawson_company_num
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.employee
                  WHERE employee.valid_to_date = datetime("9999-12-31 23:59:59")
                  GROUP BY 1, 2, 3
              ) AS eid ON emp.employee = eid.employee_num
               AND emp.company = eid.lawson_company_num
              LEFT OUTER JOIN (
                SELECT
                    status.status_sid,
                    status.lawson_company_num,
                    status.status_type_code,
                    status.status_code
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.status
                  WHERE status.valid_to_date = datetime("9999-12-31 23:59:59")
                  GROUP BY 1, 2, 3, 4
              ) AS sta ON emp.company = sta.lawson_company_num
               AND trim(emp.emp_status) = trim(sta.status_code)
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emphr ON eid.employee_sid = emphr.employee_sid
               AND emphr.valid_to_date =datetime("9999-12-31 23:59:59")
          UNION DISTINCT
          SELECT
              coalesce(eid1.employee_sid, 0) AS employee_sid,
              coalesce(sta1.status_sid, 0) AS status_sid,
              'AUX' AS status_type_code,
              trim(hremp.a_field) AS status_code,
              hremp.employee AS employee_num,
              CASE
                WHEN upper(emphr.delete_ind) = 'D' THEN 'D'
                ELSE 'A'
              END AS delete_ind,
              emp.company AS lawson_company_num,
              emp.process_level AS process_level_code,
              'L' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee AS emp
              INNER JOIN (
                SELECT
                    hrempusf.company,
                    hrempusf.employee,
                    hrempusf.a_field AS a_field
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.hrempusf
                  WHERE trim(hrempusf.field_key) = '88'
                  GROUP BY 1, 2, 3
              ) AS hremp ON emp.company = hremp.company
               AND emp.employee = hremp.employee
              LEFT OUTER JOIN (
                SELECT
                    employee.employee_sid,
                    employee.employee_num,
                    employee.lawson_company_num
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.employee
                  WHERE employee.valid_to_date = datetime("9999-12-31 23:59:59")
				  
                  GROUP BY 1, 2, 3
              ) AS eid1 ON hremp.employee = eid1.employee_num
               AND hremp.company = eid1.lawson_company_num
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emphr ON eid1.employee_sid = emphr.employee_sid
               AND emphr.valid_to_date = datetime("9999-12-31 23:59:59")
              LEFT OUTER JOIN (
                SELECT
                    status.status_sid,
                    status.lawson_company_num,
                    status.status_code,
                    status.status_type_code
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.status
                  WHERE status.valid_to_date = datetime("9999-12-31 23:59:59")
                  GROUP BY 1, 2, 3, 4
              ) AS sta1 ON hremp.company= sta1.lawson_company_num
               AND trim(hremp.a_field) = trim(sta1.status_code)
            WHERE upper(trim(status_type_code)) = 'AUX') stg
			 QUALIFY row_number() OVER (PARTITION BY stg.employee_sid, stg.status_sid ORDER BY stg.employee_sid, stg.status_sid DESC) = 1;
          /*UNION DISTINCT
          SELECT
             
              coalesce(eid.employee_sid, 0) AS employee_sid,
              coalesce(sta.status_sid, 0) AS status_sid,
              'EMP' AS status_type_code,
              coalesce(trim(mhst.hca_empl_stts), '0') AS status_code,
              emp.employee AS employee_num,
              CASE
                WHEN upper(emphr.delete_ind) = 'D' THEN 'D'
                ELSE 'A'
              END AS delete_ind,
              emp.company AS lawson_company_num,
              concat(substr('00000', 1, 5 - length(trim(emp.process_level))), trim(emp.process_level)) AS process_level_code,
              'A' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.msh_employee_stg AS emp
              LEFT OUTER JOIN (
                SELECT
                    employee.employee_sid,
                    employee.employee_num,
                    employee.lawson_company_num
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.employee
                  WHERE upper(employee.valid_to_date) = '9999-12-31'
                  GROUP BY 1, 2, 3
              ) AS eid ON trim(emp.employee) = trim(eid.employee_num)
               AND trim(emp.company) = trim(eid.lawson_company_num)
              LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.mh_to_hca_employee_status AS mhst ON emp.emp_status = mhst.mh_empl_stts
              LEFT OUTER JOIN (
                SELECT
                    status.status_sid,
                    status.lawson_company_num,
                    status.status_type_code,
                    status.status_code
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.status
                  WHERE upper(status.valid_to_date) = '9999-12-31'
                   AND upper(status.status_type_code) = 'EMP'
                  GROUP BY 1, 2, 3, 4
              ) AS sta ON trim(emp.company) = trim(sta.lawson_company_num)
               AND trim(mhst.hca_empl_stts) = trim(sta.status_code)
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emphr ON eid.employee_sid = emphr.employee_sid
               AND emphr.valid_to_date = DATE '9999-12-31'
          UNION DISTINCT
          SELECT
              coalesce(eid.employee_sid, 0) AS employee_sid,
              coalesce(sta.status_sid, 0) AS status_sid,
              'AUX' AS status_type_code,
              coalesce(trim(mhst.hca_empl_aux_stts), '0') AS status_code,
              emp.employee AS employee_num,
              CASE
                WHEN upper(emphr.delete_ind) = 'D' THEN 'D'
                ELSE 'A'
              END AS delete_ind,
              emp.company AS lawson_company_num,
              concat(substr('00000', 1, 5 - length(trim(emp.process_level))), trim(emp.process_level)) AS process_level_code,
              'A' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.msh_employee_stg AS emp
              LEFT OUTER JOIN (
                SELECT
                    employee.employee_sid,
                    employee.employee_num,
                    employee.lawson_company_num
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.employee
                  WHERE upper(employee.valid_to_date) = '9999-12-31'
                  GROUP BY 1, 2, 3
              ) AS eid ON trim(emp.employee) = trim(eid.employee_num)
               AND trim(emp.company) = trim(eid.lawson_company_num)
              LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.mh_to_hca_employee_status AS mhst ON emp.emp_status = mhst.mh_empl_stts
              LEFT OUTER JOIN (
                SELECT
                    status.status_sid,
                    status.lawson_company_num,
                    status.status_type_code,
                    status.status_code
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.status
                  WHERE upper(status.valid_to_date) = '9999-12-31'
                   AND upper(status.status_type_code) = 'AUX'
                  GROUP BY 1, 2, 3, 4
              ) AS sta ON trim(emp.company) = trim(sta.lawson_company_num)
               AND trim(mhst.hca_empl_aux_stts) = trim(sta.status_code)
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emphr ON eid.employee_sid = emphr.employee_sid
               AND emphr.valid_to_date = DATE '9999-12-31'
        ) AS stg*/
      

BEGIN TRANSACTION ;
  UPDATE {{ params.param_hr_core_dataset_name }}.junc_employee_status AS tgt SET 
  valid_to_date = current_ts- INTERVAL 1 SECOND, dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.junc_employee_status_wrk AS stg WHERE tgt.employee_sid = stg.employee_sid
   AND tgt.status_sid = stg.status_sid
   AND tgt.source_system_code = stg.source_system_code
   AND (upper(trim(tgt.status_code)) <> upper(trim(coalesce(stg.status_code, '')))
   OR trim(tgt.process_level_code) <> trim(coalesce(stg.process_level_code, '')))
   AND tgt.valid_to_date = datetime("9999-12-31 23:59:59") 
   AND tgt.source_system_code = 'L';

  INSERT INTO {{ params.param_hr_core_dataset_name }}.junc_employee_status (employee_sid, status_sid, status_type_code, status_code, employee_num, valid_from_date, valid_to_date, delete_ind, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        junc_employee_status_wrk.employee_sid,
        junc_employee_status_wrk.status_sid,
        junc_employee_status_wrk.status_type_code,
        junc_employee_status_wrk.status_code,
        junc_employee_status_wrk.employee_num,
        junc_employee_status_wrk.valid_from_date,
        junc_employee_status_wrk.valid_to_date,
        junc_employee_status_wrk.delete_ind,
        junc_employee_status_wrk.lawson_company_num,
        junc_employee_status_wrk.process_level_code,
        junc_employee_status_wrk.source_system_code,
        junc_employee_status_wrk.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.junc_employee_status_wrk
      WHERE (junc_employee_status_wrk.employee_sid||junc_employee_status_wrk.status_sid||upper(trim(junc_employee_status_wrk.status_type_code))||upper(trim(junc_employee_status_wrk.status_code))||junc_employee_status_wrk.employee_num||junc_employee_status_wrk.source_system_code) NOT IN(
        SELECT 
            junc_employee_status.employee_sid
            ||junc_employee_status.status_sid
            ||upper(trim(junc_employee_status.status_type_code))
            ||upper(trim(junc_employee_status.status_code))
            ||junc_employee_status.employee_num
            ||junc_employee_status.source_system_code
          FROM
            {{ params.param_hr_base_views_dataset_name }}.junc_employee_status
          WHERE junc_employee_status.valid_to_date = datetime("9999-12-31 23:59:59")
      )
  ;

  UPDATE {{ params.param_hr_core_dataset_name }}.junc_employee_status AS tgt 
  SET valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = current_ts WHERE tgt.valid_to_date =datetime("9999-12-31 23:59:59")
   AND (tgt.lawson_company_num||tgt.employee_num||upper(trim(tgt.status_code))||upper(trim(tgt.process_level_code))||tgt.source_system_code) NOT IN(
    SELECT 
       junc_employee_status_wrk.lawson_company_num
        ||junc_employee_status_wrk.employee_num
        ||upper(trim(junc_employee_status_wrk.status_code))
        ||upper(trim(junc_employee_status_wrk.process_level_code))
        ||junc_employee_status_wrk.source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.junc_employee_status_wrk
  ) AND tgt.source_system_code = 'L';

  UPDATE {{ params.param_hr_core_dataset_name }}.junc_employee_status AS tgt SET delete_ind = emp.delete_ind, 
  valid_to_date = current_ts - interval 1 SECOND, dw_last_update_date_time = current_ts FROM (
    SELECT
        employee.employee_sid,
        employee.delete_ind
      FROM
        {{ params.param_hr_base_views_dataset_name }}.employee
      WHERE upper(employee.delete_ind) = 'D'
      GROUP BY 1, 2
  ) AS emp WHERE tgt.employee_sid = emp.employee_sid
   AND upper(tgt.delete_ind) = 'A'
   AND tgt.source_system_code = 'L';

  UPDATE {{ params.param_hr_core_dataset_name }}.junc_employee_status AS tgt SET delete_ind = emp.delete_ind,
  dw_last_update_date_time = current_ts FROM (
    SELECT
        employee.employee_sid,
        employee.delete_ind
      FROM
        {{ params.param_hr_base_views_dataset_name }}.employee
      WHERE upper(employee.delete_ind) = 'A'
      GROUP BY 1, 2
  ) AS emp WHERE tgt.employee_sid = emp.employee_sid
   AND upper(tgt.delete_ind) = 'D'
   AND tgt.source_system_code = 'L';
   
    /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select employee_sid ,status_sid ,valid_from_date
        from {{ params.param_hr_core_dataset_name }}.junc_employee_status
        group by employee_sid ,status_sid ,valid_from_date		
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.junc_employee_status');
    ELSE
      COMMIT TRANSACTION;
    END IF; 
END ;
