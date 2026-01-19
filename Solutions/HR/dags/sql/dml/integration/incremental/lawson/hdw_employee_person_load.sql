BEGIN
DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);

CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}','employee',"COALESCE(Company,0)||'-'||COALESCE(Employee,0)", 'Employee');

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_person_wrk;

INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_person_wrk (sk, hr_company_sid, home_phone_country_code, employee_num, employee_ssn, lawson_company_num, employee_first_name, employee_last_name, employee_middle_name, employee_home_phone_num, employee_work_phone_num, employee_sector_code, ethnic_origin_code, anniversary_date, gender_code, birth_date, email_text, veteran_ind, creation_date, benefit_salary_amt, eff_from_date, eff_to_date, active_dw_ind, process_level_code, source_system_code, handicap_id, badge_code, dw_last_update_date_time)
    SELECT
        e.employee_sid,
        c.hr_company_sid,
        paemp.hm_phone_cntry,
        emp.employee,
        trim(replace(emp.fica_nbr, '-', '')) AS employee_ssn,
        emp.company,
        trim(emp.first_name),
        trim(emp.last_name),
        trim(emp.middle_name),
        paemp.hm_phone_nbr,
        paemp.wk_phone_nbr,
        max(CASE
          WHEN zv.company IS NOT NULL THEN 'CORP'
          ELSE 'FLD'
        END) AS employee_sector_code,
        paemp.eeo_class,
        emp.annivers_date,
        paemp.sex,
        paemp.birthdate,
        case when ascii(emp.email_address) = 0 then ''
        else emp.email_address end,
        paemp.veteran AS veteran_ind,
        emp.creation_date,
        paemp.ben_salary_1 AS benefit_salary_amt,
        current_date() AS eff_from_date,
        DATE'9999-12-31' AS eff_to_date,
        'Y' AS active_dw_ind,
        trim(emp.process_level) AS process_level_code,
        'L' AS source_system_code,
        trim(paemp.handicap_id) AS handicap_id,
        nullif(ltrim(paemp.security_nbr),'') AS badge_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee AS emp
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS e 
        ON emp.company = e.lawson_company_num
         AND emp.employee = e.employee_num
         AND date(e.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.paemployee AS paemp ON emp.employee = paemp.employee
         AND emp.company = paemp.company
        LEFT OUTER JOIN (
          SELECT
              zvcmmstr.company
            FROM
              {{ params.param_hr_stage_dataset_name }}.zvcmmstr
            WHERE upper(zvcmmstr.hca_lob) = 'COR'
            GROUP BY 1
        ) AS zv ON emp.company = zv.company
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_company AS c ON emp.company = c.lawson_company_num
         AND date(c.valid_to_date) = '9999-12-31'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS ep ON e.employee_sid = ep.employee_sid
         AND date(ep.valid_to_date) = '9999-12-31'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,(CASE
          WHEN zv.company IS NOT NULL THEN 'CORP'
          ELSE 'FLD'
        END) , 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, paemp.handicap_id, 27, 24;
    

UPDATE {{ params.param_hr_core_dataset_name }}.employee_person AS ep 
SET valid_to_date = current_ts - INTERVAL 1 SECOND, 
active_dw_ind = 'N', 
dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND) 
FROM {{ params.param_hr_stage_dataset_name }}.employee_person_wrk AS stg 
WHERE date(ep.valid_to_date) = '9999-12-31'
AND ep.employee_sid = stg.sk
AND upper(ep.active_dw_ind) = 'Y'
AND upper(ep.delete_ind) = 'A'
AND stg.eff_from_date <> date(ep.valid_from_date)
AND stg.source_system_code = ep.source_system_code
AND ep.source_system_code = 'L'
AND (upper(coalesce(trim(stg.home_phone_country_code), '')) <> upper(coalesce(trim(ep.home_phone_country_code), ''))
OR coalesce(stg.hr_company_sid, 0) <> coalesce(ep.hr_company_sid, 0)
OR coalesce(stg.employee_num, 0) <> coalesce(ep.employee_num, 0)
OR upper(coalesce(trim(stg.employee_first_name), '')) <> upper(coalesce(trim(ep.employee_first_name), ''))
OR upper(coalesce(trim(stg.employee_ssn), '')) <> upper(coalesce(trim(ep.employee_ssn), ''))
OR upper(coalesce(trim(stg.employee_last_name), '')) <> upper(coalesce(trim(ep.employee_last_name), ''))
OR upper(coalesce(trim(stg.employee_middle_name), '')) <> upper(coalesce(trim(ep.employee_middle_name), ''))
OR upper(coalesce(trim(stg.employee_home_phone_num), '')) <> upper(coalesce(trim(ep.employee_home_phone_num), ''))
OR upper(coalesce(trim(stg.employee_work_phone_num), '')) <> upper(coalesce(trim(ep.employee_work_phone_num), ''))
OR upper(coalesce(trim(stg.employee_sector_code), '')) <> upper(coalesce(trim(ep.employee_sector_code), ''))
OR upper(coalesce(trim(stg.ethnic_origin_code), '')) <> upper(coalesce(trim(ep.ethnic_origin_code), ''))
OR coalesce(stg.lawson_company_num, 0) <> coalesce(ep.lawson_company_num, 0)
OR upper(coalesce(trim(stg.gender_code), '')) <> upper(coalesce(trim(ep.gender_code), ''))
OR coalesce(stg.birth_date, '1900-01-01') <> coalesce(ep.birth_date, '1900-01-01')
OR upper(coalesce(trim(stg.email_text),'')) <> upper(coalesce(trim(ep.email_text),''))
OR upper(coalesce(trim(stg.veteran_ind), '')) <> upper(coalesce(trim(ep.veteran_ind), ''))
OR stg.benefit_salary_amt <> ep.benefit_salary_amt
OR upper(coalesce(trim(stg.process_level_code), '')) <> upper(coalesce(trim(ep.process_level_code), ''))
OR coalesce(trim(stg.handicap_id), '~') <> coalesce(trim(ep.disability_ind), '~')
OR upper(coalesce(trim(stg.badge_code), '')) <> upper(coalesce(trim(ep.badge_code), '')));


BEGIN TRANSACTION;

INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_person 
(employee_sid, 
home_phone_country_code, 
employee_num, employee_ssn, lawson_company_num, hr_company_sid, employee_first_name, employee_last_name, employee_middle_name, employee_home_phone_num, employee_work_phone_num, employee_sector_code, ethnic_origin_code, gender_code, birth_date, email_text, veteran_ind, benefit_salary_amt, valid_from_date, valid_to_date, delete_ind, active_dw_ind, process_level_code, source_system_code, disability_ind, badge_code, dw_last_update_date_time)
SELECT
-- Eff_From_Date,
-- Eff_To_Date,
stg.sk,
stg.home_phone_country_code,
stg.employee_num,
stg.employee_ssn,
stg.lawson_company_num,
stg.hr_company_sid,
stg.employee_first_name,
stg.employee_last_name,
stg.employee_middle_name,
stg.employee_home_phone_num,
stg.employee_work_phone_num,
stg.employee_sector_code,
stg.ethnic_origin_code,
stg.gender_code,
stg.birth_date,
stg.email_text,
stg.veteran_ind,
stg.benefit_salary_amt,
current_ts,
DATETIME("9999-12-31 23:59:59"),
'A' AS delete_ind,
stg.active_dw_ind,
stg.process_level_code,
stg.source_system_code,
stg.handicap_id AS disability_ind,
stg.badge_code,
stg.dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.employee_person_wrk AS stg
LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS tgt ON tgt.employee_sid = stg.sk
AND date(tgt.valid_to_date) = '9999-12-31'
WHERE tgt.employee_sid IS NULL;

SET DUP_COUNT = (
select count(*)
from (
select
employee_sid,valid_from_date
from {{params.param_hr_core_dataset_name}}.employee_person
 group by employee_sid,valid_from_date
having count(*) > 1
)
);
IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = concat('Duplicates are not allowed in the table');
ELSE
COMMIT TRANSACTION;
END IF;

/*  UPDATE  DELETE_IND FLAG FOR DELETED RECORDS*/
UPDATE {{ params.param_hr_core_dataset_name }}.employee_person AS empl 
SET delete_ind = 'D', active_dw_ind = 'N', valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND) WHERE upper(empl.delete_ind) = 'A'
AND (empl.lawson_company_num, empl.employee_num, empl.source_system_code) NOT IN(
SELECT AS STRUCT
emp.company,
emp.employee,
emp.source_system_code
FROM
{{ params.param_hr_stage_dataset_name }}.employee emp
)
AND date(empl.valid_to_date) = '9999-12-31'
AND empl.source_system_code = 'L';


/*  UPDATE  DELETE_IND FLAG FOR RECORDS*/
UPDATE {{ params.param_hr_core_dataset_name }}.employee_person AS empl 
SET delete_ind = 'A', dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND) 
WHERE upper(empl.delete_ind) = 'D'
AND (empl.lawson_company_num, empl.employee_num, empl.source_system_code) IN(
SELECT DISTINCT AS STRUCT
emp.company,
emp.employee,
emp.source_system_code
FROM
{{ params.param_hr_stage_dataset_name }}.employee emp
)
AND date(empl.valid_to_date) = '9999-12-31'
AND empl.source_system_code = 'L';

END


