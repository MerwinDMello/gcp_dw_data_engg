--#####################################################################################
--#                                                                                   #
--# Target Table - {{ params.param_hr_core_dataset_name }}.Employee_Roster                                              #
--#                                                                                   #
--# CHANGE CONTROL:                                                                   #
--#                                                                                   #
--# DATE          Developer     Change Comment                                        #
--# 01/29/2020    M LaFever   Initial Query                                           #
--# 06/17/2020   A.Goforth-Howser  Remove Clear QB statement                          #                                                                                    
--# 07/16/2020   Shachindra Pandey : Updated to Load new table {{ params.param_hr_core_dataset_name }}.Employee_Roster  #
--# 7/20/2020    Ashley Goforth-Howser   A&C                                          #
--# 8/17/2020   Shachindra Pandey - Made changes to adress related column             #
--# 8/24/2020    Shachindra Pandey - Added Coalesce to home address related columns   #
--# 1/4/2022    Joseph VanHook - Expanded to two years plus current                   #
--# 7/26/2022   Jose Huertas - Added the field County_Name                            #
--# 8/19/2022   Jose Huertas - Added the field Work_County_Name                       #
--# 10/27/2022  Jose Huertas - Updated CASE Statement (Line 400)                      #
--# Created for HR Insights reporting                                                 #
--#                                                                                   #
--#                                                                                   #
--#                                                                                   #
--#                                                                                   #  
--#                                                                                   #
--#  This will load {{ params.param_hr_core_dataset_name }}.Employee_Roster                                             #
--#####################################################################################


--FIRST STMT
DECLARE QB_Stmt STRING;

--A&C
DECLARE Start_Date DATETIME;
DECLARE End_Date DATETIME;
-- SELECT Current_Timestamp(0) INTO :Start_Date;

SET End_Date = Current_datetime('US/Central');

--Set Queryband

BEGIN

CREATE TEMPORARY TABLE hr_value
  AS
    SELECT
        a.employee_num,
        a.eff_from,
        CASE
          WHEN lead(a.eff_from, 1) OVER (PARTITION BY a.employee_num, a.lawson_element_num ORDER BY a.lawson_element_num, a.eff_from) - 1 IS NULL THEN parse_date('%Y-%m-%d', '9999-12-31')
          ELSE lead(a.eff_from, 1) OVER (PARTITION BY a.employee_num, a.lawson_element_num ORDER BY a.lawson_element_num, a.eff_from) - 1
        END AS eff_to,
        a.lawson_element_num,
        a.lawson_element_desc,
        a.lawson_element_value
      FROM
        (
          SELECT
              hreh.employee_num,
              hreh.hr_employee_element_date AS eff_from,
              hreh.lawson_element_num,
              rle.lawson_element_desc,
              CASE
                WHEN upper(trim(rle.lawson_element_type_flag)) = 'N' THEN cast(hreh.hr_employee_value_num as string)
                WHEN upper(trim(rle.lawson_element_type_flag)) = 'D' THEN cast(hreh.hr_employee_value_date as string)
                ELSE hreh.hr_employee_value_alphanumeric_text
              END AS lawson_element_value,
              hreh.sequence_num,
              row_number() OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num, hreh.hr_employee_element_date ORDER BY hreh.hr_employee_element_date, hreh.sequence_num DESC) AS row_count
            FROM
              {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hreh
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_lawson_element AS rle ON hreh.lawson_element_num = rle.lawson_element_num
            WHERE date(hreh.valid_to_date) = '9999-12-31'
             AND upper(trim(cast(hreh.lawson_element_num as string))) IN(
              '2', '3', '4', '5', '156', '6', '7', '8', '9', '59', '1611', '729', '568'
            )
            QUALIFY row_count = 1
        ) AS a
        INNER JOIN -- ------ FILTERED THE VALUES USED IN BELOW INSERT (IN JOINS)------
        {{ params.param_pub_views_dataset_name }}.lu_date AS lud ON lud.date_id = current_date('US/Central')
      WHERE a.row_count = 1
      QUALIFY eff_to >= date_add(current_date('US/Central'), interval -24 MONTH)
;

CREATE TEMPORARY TABLE sup
  AS
    SELECT DISTINCT
        scd.lawson_company_num,
        scd.supervisor_sid,
        scd.supervisor_code,
        CASE
          WHEN scd.employee_num = 0 THEN NULL
          ELSE scd.employee_num
        END AS sup_eid,
        CASE
          WHEN scd.employee_num = 0 THEN NULL
          ELSE see.employee_34_login_code
        END AS sup_34_id,
        concat(trim(cse.employee_last_name), ', ', trim(cse.employee_first_name)) AS sup_name,
        scd.supervisor_desc,
        cse.email_text,
        corp_sup.reporting_supervisor_sid AS rpt_to_5950,
        scd.reporting_supervisor_sid AS rpt_to_other,
        coalesce(corp_sup.reporting_supervisor_sid, scd.reporting_supervisor_sid) AS reporting_supervisor_sid,
        scd.valid_from_date,
        scd.valid_to_date
      FROM
        {{ params.param_hr_base_views_dataset_name }}.supervisor AS scd
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS see ON scd.employee_sid = see.employee_sid
         AND date(see.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS cse ON scd.employee_sid = cse.employee_sid
         AND date(cse.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN (
          SELECT DISTINCT
              scd_0.employee_num,
              scd_0.reporting_supervisor_sid
            FROM
              {{ params.param_hr_base_views_dataset_name }}.supervisor AS scd_0
              INNER JOIN (
                SELECT DISTINCT
                    fscd.employee_num
                  FROM
                    -- 139434
                    {{ params.param_hr_base_views_dataset_name }}.supervisor AS fscd
                  WHERE fscd.lawson_company_num <> 5950
                   AND fscd.employee_num <> 0
              ) AS x ON scd_0.employee_num = x.employee_num
            WHERE scd_0.lawson_company_num = 5950
             AND scd_0.employee_num <> 0
        ) AS corp_sup ON scd.employee_num = corp_sup.employee_num
;


-- Delete from table
DELETE FROM {{ params.param_hr_core_dataset_name }}.employee_roster WHERE employee_roster.date_id >= date_add(current_date('US/Central'), interval -24 MONTH);

-- Create metric data 
INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_roster (date_id, dw_last_update_date_time, active_dw_ind, group_code, group_name, division_code, division_name, market_code, market_name, lob_code, lob_name, sub_lob_code, sub_lob_name, functional_dept_num, functional_dept_desc, sub_functional_dept_num, sub_functional_dept_desc, key_talent_group_text, ilob_category_desc, ilob_sub_category_desc, business_unit_name, business_unit_segment_name, hr_company_sid, company_code, lawson_company_num, company_name, coid, coid_name, process_level_sid, process_level_code, process_level_name, dept_num, dept_sid, dept_code, dept_name, location_code, location_desc, addr_line_1_text, addr_line_2_text, city_name, state_code, zip_code, county_name, employee_preferred_name, employee_first_name, employee_middle_initial_text, employee_last_name, employee_sid, employee_num, employee_34_login_code, fte_percent, employee_status_sid, employee_status_code, employee_status_desc, auxiliary_status_sid, auxiliary_status_code, auxiliary_status_desc, hire_date, termination_date, adjusted_hire_date, anniversary_date, service_year_num, job_experience_date, rn_experience_date, birth_date, age_num, marital_status_code, ethnic_origin_code, ethnic_desc, gender_code, veteran_ind, veteran_desc, disability_ind, employee_ssn, employee_home_phone_num, employee_work_phone_num, email_text, work_addr_line_1_text, work_addr_line_2_text, work_city_name, work_state_code, work_zip_code, work_county_name, remote_flag, pay_rate_amt, salary_amt, job_class_sid, job_class_code, job_class_desc, job_code_sid, job_code, job_code_desc, position_sid, position_code, position_code_desc, position_level_sequence_num, work_schedule_code, work_schedule_desc, pay_grade_code, pay_grade_schedule_code, overtime_plan_code, overtime_exempt_ind, union_code, union_desc, supervisor_sid, supervisor_code, immediate_supervisor_ind, supervisor_name, supervisor_employee_id, supervisor_34_id, supervisor_position, supervisor_email_text, disaster_team_code, source_system_code)
  SELECT DISTINCT
      fhrm.date_id,
      datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
      CASE
        WHEN ((ee.termination_date) = '1800-01-01'
         OR (ee.termination_date) = '1700-01-01')
         AND fhrm.date_id = current_date('US/Central') THEN 'Y'
        ELSE 'N'
      END AS active_dw_ind,
      ff.group_code,
      ff.group_name,
      ff.division_code,
      ff.division_name,
      ff.market_code,
      ff.market_name,
      ff.lob_code,
      ff.lob_name,
      ff.sub_lob_code,
      ff.sub_lob_name,
      fhrm.functional_dept_num,
      fdept.functional_dept_desc,
      fhrm.sub_functional_dept_num,
      fdept.sub_functional_dept_desc,
      -- -alias name changed
      rkeyt.key_talent_group_text AS key_talent,
      ilob.category_desc AS ilob_category_desc,
      ilob.sub_category_desc AS ilob_sub_category_desc,
      rho.business_unit_name AS hrops_business_unit,
      rho.business_unit_segment_name AS hrops_business_segment,
      hrc.hr_company_sid,
      fhrm.company_code,
      fhrm.lawson_company_num,
      hrc.company_name,
      fhrm.coid,
      ff.coid_name,
      pl.process_level_sid,
      fhrm.process_level_code,
      pl.process_level_name,
      xwalk.dept_num AS cost_center,
      fhrm.dept_sid,
      dept.dept_code,
      dept.dept_name,
      fhrm.location_code AS employee_location_code,
      rl.location_desc,
      CASE
        WHEN addr1.lawson_element_value IS NULL THEN addr.addr_line_1_text
        WHEN addr1.lawson_element_value = addr.addr_line_1_text THEN addr1.lawson_element_value
        ELSE addr.addr_line_1_text
      END AS addr_line_1_text,
      CASE
        WHEN addr2.lawson_element_value IS NULL THEN addr.addr_line_2_text
        WHEN addr2.lawson_element_value = addr.addr_line_2_text THEN addr2.lawson_element_value
        ELSE addr.addr_line_2_text
      END AS addr_line_2_text,
      CASE
        WHEN city.lawson_element_value IS NULL THEN addr.city_name
        WHEN city.lawson_element_value = addr.city_name THEN city.lawson_element_value
        ELSE addr.city_name
      END AS city_name,
      CASE
        WHEN state.lawson_element_value IS NULL THEN addr.state_code
        WHEN state.lawson_element_value = addr.state_code THEN state.lawson_element_value
        ELSE addr.state_code
      END AS state_code,
      CASE
        WHEN zip.lawson_element_value IS NULL THEN addr.zip_code
        WHEN zip.lawson_element_value = addr.zip_code THEN zip.lawson_element_value
        ELSE addr.zip_code
      END AS zip_code,
      CASE
        WHEN county.lawson_element_value IS NULL THEN addr.county_name
        WHEN county.lawson_element_value = addr.county_name THEN county.lawson_element_value
        ELSE addr.county_name
      END AS county_name,
      pname.lawson_element_value AS preferred_name,
      coalesce(fname.lawson_element_value, epers.employee_first_name) AS employee_first_name,
      coalesce(mname.lawson_element_value, epers.employee_middle_name) AS employee_middle_initial_text,
      coalesce(lname.lawson_element_value, epers.employee_last_name) AS employee_last_name,
      fhrm.employee_sid,
      fhrm.employee_num,
      ee.employee_34_login_code,
      ep.fte_percent,
      fhrm.employee_status_sid,
      empstat.status_code AS employee_status_code,
      empstat.status_desc AS employee_status_description,
      fhrm.auxiliary_status_sid,
      auxstat.status_code AS auxiliary_status_code,
      auxstat.status_desc AS auxiliary_status_description,
      ee.hire_date,
      CASE
        WHEN (ee.termination_date) = '1800-01-01'
         OR (ee.termination_date) = '1700-01-01' THEN NULL
        ELSE ee.termination_date
      END AS termination_date,
      ee.adjusted_hire_date,
      ee.anniversary_date,
      cast(floor(DATE_DIFF((date(fhrm.Date_Id) ) , date(ee.Anniversary_Date), DAY) / NUMERIC '365' ) as numeric) AS length_of_service,
      edje.detail_value_date AS job_experience_date,
      edne.detail_value_date AS rn_experience_date,
      epers.birth_date,
      cast(floor(DATE_DIFF(fhrm.date_id , epers.birth_date,DAY) / NUMERIC '365') as int64) AS employee_age,
      marital_status.lawson_element_value AS marital_status_code,
      epers.ethnic_origin_code AS ethnic_code,
      rhrde.demographic_desc AS ethnic_description,
      epers.gender_code,
      epers.veteran_ind AS veteran_code,
      rhrdv.demographic_desc AS veteran_description,
      epers.disability_ind,
      epers.employee_ssn,
      epers.employee_home_phone_num,
      epers.employee_work_phone_num,
      epers.email_text,
      locaddr.addr_line_1_text AS work_addr1,
      locaddr.addr_line_2_text AS work_addr2,
      locaddr.city_name AS work_city,
      locaddr.state_code AS work_state,
      locaddr.zip_code AS work_zip,
      locaddr.county_name AS work_county_name,
      CASE
        WHEN remote_flag.lawson_element_value = '1'
         OR upper(trim(ep.schedule_work_code)) = 'WFH' THEN 'Y'
        ELSE 'N'
      END AS remote_flag,
      ep.pay_rate_amt,
      cast(salary.lawson_element_value as numeric) AS salary_amt,
      fhrm.job_class_sid,
      jcl.job_class_code,
      jcl.job_class_desc,
      fhrm.job_code_sid,
      jcd.job_code,
      jcd.job_code_desc,
      fhrm.position_sid,
      jp.position_code,
      jp.position_code_desc,
      ep.position_level_sequence_num AS position_level,
      ep.schedule_work_code AS work_schedule_code,
      rhrcws.hr_code_desc AS work_schedule_description,
      jp.pay_grade_code,
      jp.pay_grade_schedule_code,
      jp.overtime_plan_code,
      jp.overtime_exempt_ind,
      jp.union_code,
      rhrcun.hr_code_desc AS union_description,
      jp.supervisor_sid,
      supv.supervisor_code,
      CASE
        WHEN coalesce(s1.sup_eid, 0) = 0 THEN 'N'
        ELSE 'Y'
      END AS immediate_supervisor_ind,
      coalesce(s1.sup_name, s2.sup_name, s3.sup_name) AS supervisor_name,
      coalesce(s1.sup_eid, s2.sup_eid, s3.sup_eid) AS supervisor_employee_id,
      coalesce(s1.sup_34_id, s2.sup_34_id, s3.sup_34_id) AS supervisor_34_id,
      coalesce(s1.supervisor_desc, s2.supervisor_desc, s3.supervisor_desc) AS supervisor_position,
      coalesce(s1.email_text, s2.email_text, s3.email_text) AS supervisor_email,
      ecd.employee_code AS disaster_team_code,
      fhrm.source_system_code
    FROM
      {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS fhrm
      INNER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS lud ON lud.date_id = current_date('US/Central')
      LEFT OUTER JOIN -- HDM-2040
      -- Begin Historical Joins--
      hr_value AS lname ON fhrm.employee_num = lname.employee_num
       AND fhrm.date_id BETWEEN lname.eff_from AND lname.eff_to
       AND (lname.lawson_element_num) = 2
      LEFT OUTER JOIN hr_value AS fname ON fhrm.employee_num = fname.employee_num
       AND fhrm.date_id BETWEEN fname.eff_from AND fname.eff_to
       AND (fname.lawson_element_num) = 3
      LEFT OUTER JOIN hr_value AS mname ON fhrm.employee_num = mname.employee_num
       AND fhrm.date_id BETWEEN mname.eff_from AND mname.eff_to
       AND (mname.lawson_element_num) = 4
      LEFT OUTER JOIN hr_value AS pname ON fhrm.employee_num = pname.employee_num
       AND fhrm.date_id BETWEEN pname.eff_from AND pname.eff_to
       AND (pname.lawson_element_num) = 156
      LEFT OUTER JOIN hr_value AS addr1 ON fhrm.employee_num = addr1.employee_num
       AND fhrm.date_id BETWEEN addr1.eff_from AND addr1.eff_to
       AND (addr1.lawson_element_num) = 5
      LEFT OUTER JOIN hr_value AS addr2 ON fhrm.employee_num = addr2.employee_num
       AND fhrm.date_id BETWEEN addr2.eff_from AND addr2.eff_to
       AND (addr2.lawson_element_num) =6
      LEFT OUTER JOIN hr_value AS city ON fhrm.employee_num = city.employee_num
       AND fhrm.date_id BETWEEN city.eff_from AND city.eff_to
       AND (city.lawson_element_num) = 7
      LEFT OUTER JOIN hr_value AS state ON fhrm.employee_num = state.employee_num
       AND fhrm.date_id BETWEEN state.eff_from AND state.eff_to
       AND (state.lawson_element_num) = 8
      LEFT OUTER JOIN hr_value AS zip ON fhrm.employee_num = zip.employee_num
       AND fhrm.date_id BETWEEN zip.eff_from AND zip.eff_to
       AND (zip.lawson_element_num) = 9
      LEFT OUTER JOIN hr_value AS county ON fhrm.employee_num = county.employee_num
       AND fhrm.date_id BETWEEN county.eff_from AND county.eff_to
       AND (county.lawson_element_num) = 568
      LEFT OUTER JOIN hr_value AS marital_status ON fhrm.employee_num = marital_status.employee_num
       AND fhrm.date_id BETWEEN marital_status.eff_from AND marital_status.eff_to
       AND (marital_status.lawson_element_num) = 59
      LEFT OUTER JOIN hr_value AS remote_flag ON fhrm.employee_num = remote_flag.employee_num
       AND fhrm.date_id BETWEEN remote_flag.eff_from AND remote_flag.eff_to
       AND (remote_flag.lawson_element_num) = 1611
      LEFT OUTER JOIN hr_value AS salary ON fhrm.employee_num = salary.employee_num
       AND fhrm.date_id BETWEEN salary.eff_from AND salary.eff_to
       AND (salary.lawson_element_num) = 729
      LEFT OUTER JOIN -- End Historical Joins--
      -- Begin Descriptive Joins--
      {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep ON fhrm.employee_sid = ep.employee_sid
       AND fhrm.position_sid = ep.position_sid
       AND date(ep.valid_to_date) = '9999-12-31'
       AND fhrm.date_id BETWEEN ep.eff_from_date AND ep.eff_to_date
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON fhrm.position_sid = jp.position_sid
       AND date(jp.valid_to_date) = '9999-12-31'
       AND fhrm.date_id BETWEEN jp.eff_from_date AND jp.eff_to_date
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON fhrm.dept_sid = dept.dept_sid
       AND date(dept.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jcl ON fhrm.job_class_sid = jcl.job_class_sid
       AND date(jcl.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jcd ON fhrm.job_code_sid = jcd.job_code_sid
       AND date(jcd.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_location AS rl ON fhrm.location_code = rl.location_code
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON ep.account_unit_num = xwalk.account_unit_num
       AND ep.gl_company_num = xwalk.gl_company_num
       AND date(xwalk.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON fhrm.coid = ff.coid
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS empstat ON fhrm.employee_status_sid = empstat.status_sid
       AND date(empstat.valid_to_date) = '9999-12-31'
       AND upper(trim(empstat.status_type_code)) = 'EMP'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS auxstat ON fhrm.auxiliary_status_sid = auxstat.status_sid
       AND date(auxstat.valid_to_date) = '9999-12-31'
       AND upper(trim(auxstat.status_type_code)) = 'AUX'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS pl ON fhrm.lawson_company_num = pl.lawson_company_num
       AND fhrm.process_level_code = pl.process_level_code
       AND date(pl.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS epers ON fhrm.employee_sid = epers.employee_sid
       AND date(epers.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS ee ON fhrm.employee_sid = ee.employee_sid
       AND date(ee.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_company AS hrc ON fhrm.lawson_company_num = hrc.lawson_company_num
       AND date(hrc.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN (
        SELECT
            addr_0.location_code,
            addr_0.addr_line_1_text,
            addr_0.addr_line_2_text,
            addr_0.city_name,
            addr_0.state_code,
            addr_0.zip_code,
            addr_0.county_name,
            date(addr_0.dw_last_update_date_time) AS eff_from_date,
            CASE
              WHEN date_sub(lead(date(addr_0.dw_last_update_date_time), 1) OVER (PARTITION BY addr_0.location_code ORDER BY unix_date(date(addr_0.dw_last_update_date_time))), interval 1 DAY) IS NULL THEN parse_date('%Y-%m-%d', '9999-12-31')
              ELSE date_sub(lead(date(addr_0.dw_last_update_date_time), 1) OVER (PARTITION BY addr_0.location_code ORDER BY unix_date(date( addr_0.dw_last_update_date_time))), interval 1 DAY)
            END AS eff_to_date
          FROM
            {{ params.param_hr_base_views_dataset_name }}.address AS addr_0
          WHERE upper(trim(addr_0.addr_type_code)) = 'LOC'
      ) AS locaddr ON fhrm.location_code = locaddr.location_code
       AND fhrm.date_id BETWEEN locaddr.eff_from_date AND locaddr.eff_to_date
      LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS fdept ON fhrm.coid = fdept.coid
       AND xwalk.dept_num = fdept.dept_num
       AND fhrm.functional_dept_num = fdept.functional_dept_num
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.supervisor AS supv ON jp.supervisor_sid = supv.supervisor_sid
       AND fhrm.date_id BETWEEN supv.valid_from_date AND supv.valid_to_date
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_detail AS edje ON fhrm.employee_sid = edje.employee_sid
       AND date(edje.valid_to_date) = '9999-12-31'
       AND upper(trim(edje.employee_detail_code)) = '92'
      LEFT OUTER JOIN -- Job Experience Date--
      {{ params.param_hr_base_views_dataset_name }}.employee_detail AS edne ON fhrm.employee_sid = edne.employee_sid
       AND date(edne.valid_to_date) = '9999-12-31'
       AND upper(trim(edne.employee_detail_code)) = '59'
      LEFT OUTER JOIN -- Nurse Experience Date--
      {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS rhrcws ON ep.schedule_work_code = rhrcws.hr_code
       AND upper(trim(rhrcws.hr_type_code)) = 'WS'
       AND upper(trim(rhrcws.active_ind)) = 'A'
      LEFT OUTER JOIN -- Work Schedule Desc--
      {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS rhrcun ON jp.union_code = rhrcun.hr_code
       AND upper(trim(rhrcun.hr_type_code)) = 'UN'
       AND upper(trim(rhrcun.active_ind)) = 'A'
      LEFT OUTER JOIN -- Union Desc--
      {{ params.param_hr_base_views_dataset_name }}.ref_hr_demographic AS rhrde ON epers.ethnic_origin_code = rhrde.demographic_code
       AND upper(trim(rhrde.demographic_type_code)) = 'ET'
       AND upper(trim(rhrde.active_flag)) = 'A'
      LEFT OUTER JOIN -- Ethnic Desc--
      {{ params.param_hr_base_views_dataset_name }}.ref_hr_demographic AS rhrdv ON epers.veteran_ind = rhrdv.demographic_code
       AND upper(trim(rhrdv.demographic_type_code)) = 'VS'
       AND upper(trim(rhrdv.active_flag)) = 'A'
      LEFT OUTER JOIN -- Vet Desc--
      -- End Descriptive Joins--
      -- Begin Supervisor Hierarchy Joins--
      sup AS s1 ON s1.supervisor_sid = jp.supervisor_sid
       AND fhrm.date_id BETWEEN s1.valid_from_date AND s1.valid_to_date
      LEFT OUTER JOIN sup AS s2 ON s2.supervisor_sid = s1.reporting_supervisor_sid
       AND fhrm.date_id BETWEEN s2.valid_from_date AND s2.valid_to_date
      LEFT OUTER JOIN sup AS s3 ON s3.supervisor_sid = s2.reporting_supervisor_sid
       AND fhrm.date_id BETWEEN s3.valid_from_date AND s3.valid_to_date
      LEFT OUTER JOIN sup AS s4 ON s4.supervisor_sid = s3.reporting_supervisor_sid
       AND fhrm.date_id BETWEEN s4.valid_from_date AND s4.valid_to_date
      LEFT OUTER JOIN /*LEFT JOIN sup s5
                              ON s5.supervisor_sid = s4.reporting_supervisor_sid
                              AND fhrm.Date_Id BETWEEN s5.valid_from_date AND s5.valid_to_date*/
      -- End Supervisor Hierarchy Joins--
      -- Begin Key Talent Joins--
      {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt ON fhrm.key_talent_id = rkeyt.key_talent_id
      LEFT OUTER JOIN -- End Key Talent Joins--
      -- Begin ILOB Joins--
      {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS ilob ON fhrm.integrated_lob_id = ilob.integrated_lob_id
      LEFT OUTER JOIN -- End ILOB Joins--
      -- Begin HR Ops Rollup Joins-
      {{ params.param_hr_base_views_dataset_name }}.ref_hr_operations AS rho ON fhrm.process_level_code = rho.process_level_code
      LEFT OUTER JOIN -- End HR Ops Rollup Join--
      -- Begin Address Join--
      {{ params.param_hr_base_views_dataset_name }}.junc_employee_address AS jea ON epers.employee_sid = jea.employee_sid
       AND date(jea.valid_to_date) = '9999-12-31'
       AND upper(trim(jea.addr_type_code)) = 'EMP'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.address AS addr ON jea.addr_sid = addr.addr_sid
       AND jea.addr_type_code = addr.addr_type_code
       AND upper(trim(addr.addr_type_code)) = 'EMP'
      LEFT OUTER JOIN -- Disaster Team Code--
      (
        SELECT
            ecd_0.employee_sid,
            ecd_0.employee_code
          FROM
            {{ params.param_hr_base_views_dataset_name }}.employee_code_detail AS ecd_0
          WHERE date(ecd_0.valid_to_date) = '9999-12-31'
           AND upper(trim(ecd_0.employee_type_code)) = 'OA'
           AND upper(trim(ecd_0.employee_code)) IN(
            'O-DISASTMA', 'O-DISASTMB', 'O-DISASTMC'
          )
      ) AS ecd ON fhrm.employee_sid = ecd.employee_sid
    WHERE (fhrm.analytics_msr_sid) = 80100
     AND fhrm.date_id BETWEEN date_add(current_date('US/Central'), interval -24 MONTH) AND current_date('US/Central')
     AND fhrm.employee_num <> 0
    QUALIFY row_number() OVER (PARTITION BY fhrm.employee_sid, fhrm.position_sid, fhrm.date_id ORDER BY fhrm.employee_num DESC) = 1
;

END;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
