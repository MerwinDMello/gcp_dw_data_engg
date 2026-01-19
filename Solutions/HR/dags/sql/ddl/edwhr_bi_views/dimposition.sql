create or replace view {{ params.param_hr_bi_views_dataset_name }}.dimposition as SELECT DISTINCT
    concat(jpos.position_sid, jpos.eff_to_date) AS position_key,
    jpos.location_code,
    -- This Location_Code from Job_Position relates ONLY to the position and does not always align with Location_Code from Employee_Position (which is in Fact_HR_Metric Headcount)
    jpos.position_code,
    jpos.position_code_desc AS job_title,
    jcode.job_code,
    jcode.job_code_desc,
    jclass.job_class_code,
    jclass.job_class_desc,
    jpos.position_sid,
    CASE
      WHEN upper(ff.lob_code) = 'HOS' THEN ndir.director_grouping_desc
      ELSE NULL
    END AS nursing_director_specialty,
    CASE
      WHEN CASE
        WHEN upper(ff.lob_code) = 'HOS' THEN ndir.director_grouping_desc
        ELSE NULL
      END IS NOT NULL THEN 'Nursing Leadership'
      WHEN pct.job_title_text IS NOT NULL THEN 'PCT'
      ELSE 'Others'
    END AS position_group,
    coalesce(rdi1.leadership_level_code, rdi3.leadership_level_code, rdi2.leadership_level_code, CASE
      WHEN jpos.pay_grade_code IN(
        'CEO', 'EVP', 'GCF', 'GPR', 'O40', 'SV1', 'SV2', 'SV3', 'SV4', 'SV5', 'SV6', 'GC2', 'GP1'
      ) THEN 'L1'
      WHEN jpos.lawson_company_num = 5920
       AND upper(jcode.job_code_desc) = 'LEADERSHIP DEVELOPMENT' THEN 'L3'
      WHEN jpos.pay_grade_code IN(
        -- 1/13/22 Added by Stephen to show Leadership Development Job Codes as L3
        'C34', 'C35', 'C36', 'C37', 'C38', 'C88', 'O34', 'O35', 'O36', 'O37', 'O38', 'O39', 'O40'
      )
       AND jpos.lawson_company_num IN (
        5920, 5959, 5950
      ) THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'AVP%' THEN 'L3'
      WHEN upper(jpos.pay_grade_schedule_code) = 'CORP OS' THEN 'L2'
      WHEN upper(jpos.pay_grade_schedule_code) = 'FAC OS' THEN 'L2'
      WHEN upper(jpos.pay_grade_schedule_code) = 'DIV EXEC' THEN 'L2'
      WHEN jcode.job_code_desc IN(
        'Controller', 'Asst Controller'
      ) THEN 'L3'
      WHEN upper(jcode.job_code_desc) LIKE 'COO%' THEN 'L2'
      WHEN upper(jcode.job_code_desc) LIKE 'CFO%' THEN 'L2'
      WHEN upper(jcode.job_code_desc) LIKE 'CEO%' THEN 'L2'
      WHEN upper(jcode.job_code_desc) LIKE 'CNO%' THEN 'L2'
      WHEN upper(jcode.job_code_desc) LIKE 'CMO%' THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'SVP%' THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'COO%' THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'CIO%' THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'CMO%' THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'CEO%' THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'CFO%' THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'DIV CFO%' THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'DIV CEO%' THEN 'L2'
      WHEN upper(jcode.job_code_desc) LIKE 'DIV CMO%' THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'DIV CNE%' THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'DIV PRESIDENT%' THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE '%DIR%'
       OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%'
       OR upper(jpos.position_code_desc) LIKE 'DIV AVP%'
       OR upper(jpos.position_code_desc) LIKE 'DISA%' THEN 'L3'
      WHEN (upper(jcode.job_code_desc) LIKE '%DIR%'
       OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%')
       AND upper(jpos.pay_grade_schedule_code) LIKE 'NATLDIR%' THEN 'L3'
      WHEN jpos.lawson_company_num = 5920
       AND upper(jcode.job_code) = '01722'
       AND upper(jpos.position_code_desc) LIKE 'VP HR%' THEN 'L3'
      WHEN jpos.lawson_company_num IN (
        5920, 5959, 5950
      )
       AND (upper(jpos.position_code_desc) LIKE 'VP%'
       OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
       OR upper(jpos.position_code_desc) LIKE 'RVP%'
       OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
       OR upper(jpos.position_code_desc) LIKE 'GROUP VP%') THEN 'L2'
      WHEN upper(jpos.position_code_desc) LIKE 'VP%'
       OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
       OR upper(jpos.position_code_desc) LIKE 'RVP%'
       OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
       OR upper(jpos.position_code_desc) LIKE 'GROUP VP%' THEN 'L3'
      WHEN upper(jpos.position_code_desc) LIKE '%MGR%'
       OR upper(jpos.position_code_desc) LIKE '%MANAGER%' THEN 'L4'
      WHEN upper(jpos.position_code_desc) LIKE 'SUP%'
       OR upper(jpos.position_code_desc) LIKE '%SUPERVISOR%' THEN 'L4'
      WHEN upper(jcode.job_code_desc) = 'RECRUITING LEADERSHIP'
       AND upper(jpos.position_code_desc) LIKE '%LEAD%'
       AND upper(jpos.pay_grade_schedule_code) LIKE 'NONPEP%' THEN 'L4'
      ELSE CAST(NULL as STRING)
    END, rdi4.leadership_level_code, CAST(NULL as STRING)) AS leadership_level,
    coalesce(rdi1.leadership_role_name, rdi3.leadership_role_name, rdi2.leadership_role_name, CASE
      WHEN jpos.pay_grade_code IN(
        'CEO', 'EVP', 'GCF', 'GPR', 'O40', 'SV1', 'SV2', 'SV3', 'SV4', 'SV5', 'SV6', 'GC2', 'GP1'
      ) THEN 'SVP'
      WHEN upper(jcode.job_code_desc) LIKE 'SVP%' THEN 'SVP'
      WHEN upper(jcode.job_code_desc) LIKE 'CEO%' THEN 'CEO'
      WHEN upper(jcode.job_code_desc) LIKE 'CNO%' THEN 'CNO'
      WHEN upper(jcode.job_code_desc) LIKE 'COO%' THEN 'COO'
      WHEN upper(jcode.job_code_desc) LIKE 'CFO%' THEN 'CFO'
      WHEN upper(jcode.job_code_desc) LIKE 'HOSP CMO%' THEN 'CMO'
      WHEN upper(jcode.job_code_desc) LIKE 'GROUP VP%' THEN 'VP'
      WHEN upper(jpos.position_code_desc) LIKE 'GROUP VP%' THEN 'VP'
      WHEN upper(jpos.position_code_desc) LIKE 'EVP%' THEN 'VP'
      WHEN upper(jcode.job_code_desc) LIKE 'PRESIDENT%' THEN 'VP'
      WHEN upper(jcode.job_code_desc) = 'CONTROLLER' THEN 'VP'
      WHEN upper(jcode.job_code_desc) = 'ASST CONTROLLER' THEN 'Directors'
      WHEN upper(jpos.position_code_desc) LIKE 'COO%' THEN 'COO'
      WHEN upper(jpos.position_code_desc) LIKE 'CIO%' THEN 'CIO'
      WHEN upper(jpos.position_code_desc) LIKE 'CMO%' THEN 'CMO'
      WHEN upper(jpos.position_code_desc) LIKE 'CEO%' THEN 'CEO'
      WHEN upper(jpos.position_code_desc) LIKE 'CFO%' THEN 'CFO'
      WHEN upper(jpos.position_code_desc) LIKE 'DIV CFO%' THEN 'Div CFO'
      WHEN upper(jpos.position_code_desc) LIKE 'DIV CEO%' THEN 'Div CEO'
      WHEN upper(jcode.job_code_desc) LIKE 'DIV CMO%' THEN 'Div CMO'
      WHEN upper(jpos.position_code_desc) LIKE 'DIV CNE%' THEN 'Div CNE'
      WHEN upper(jpos.position_code_desc) LIKE 'DIV PRESIDENT%' THEN 'Div President'
      WHEN upper(jpos.position_code_desc) LIKE 'VP%'
       OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
       OR upper(jpos.position_code_desc) LIKE 'RVP%'
       OR upper(jpos.position_code_desc) LIKE 'DIV VP%' THEN 'VP'
      WHEN upper(jpos.position_code_desc) LIKE 'AVP%'
       OR upper(jpos.position_code_desc) LIKE 'DIV AVP%' THEN 'AVP'
      WHEN upper(jpos.position_code_desc) LIKE 'DIV DIR%' THEN 'Div Directors'
      WHEN upper(jpos.position_code_desc) LIKE 'DIR DIV%' THEN 'Div Directors'
      WHEN upper(jpos.position_code_desc) LIKE 'ADMIN DIR%' THEN 'Admin Directors'
      WHEN upper(jpos.position_code_desc) LIKE '%DIR%'
       OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%'
       OR upper(jpos.position_code_desc) LIKE 'DISA%' THEN 'Directors'
      WHEN (upper(jcode.job_code_desc) LIKE '%DIR%'
       OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%')
       AND upper(jpos.pay_grade_schedule_code) LIKE 'NATLDIR%' THEN 'Directors'
      WHEN upper(jpos.position_code_desc) LIKE '%MGR%'
       OR upper(jpos.position_code_desc) LIKE '%MANAGER%' THEN 'Managers'
      WHEN upper(jpos.position_code_desc) LIKE 'SUP%'
       OR upper(jpos.position_code_desc) LIKE '%SUPERVISOR%' THEN 'Supervisors'
      WHEN upper(jcode.job_code_desc) = 'RECRUITING LEADERSHIP'
       AND upper(jpos.position_code_desc) LIKE '%LEAD%'
       AND upper(jpos.pay_grade_schedule_code) LIKE 'NONPEP%' THEN 'Supervisors'
      WHEN jpos.pay_grade_code IN(
        'C34', 'C35', 'C36', 'C37', 'C38', 'C88', 'O34', 'O35', 'O36', 'O37', 'O38', 'O39', 'O40'
      )
       AND jpos.lawson_company_num IN (
        5920, 5959, 5950
      ) THEN 'Senior Leadership'
      WHEN upper(jpos.pay_grade_schedule_code) = 'CORP OS' THEN 'Corporate Officer'
      WHEN upper(jpos.pay_grade_schedule_code) = 'FAC OS' THEN 'Facility Leadership'
      WHEN upper(jpos.pay_grade_schedule_code) = 'DIV EXEC' THEN 'Div Leadership'
      ELSE CAST(NULL as STRING)
    END, rdi4.leadership_role_name, CAST(NULL as STRING)) AS leadership_role,
    CASE
      WHEN upper(coalesce(rdi1.leadership_level_code, rdi3.leadership_level_code, rdi2.leadership_level_code, CASE
        WHEN jpos.pay_grade_code IN(
          'CEO', 'EVP', 'GCF', 'GPR', 'O40', 'SV1', 'SV2', 'SV3', 'SV4', 'SV5', 'SV6', 'GC2', 'GP1'
        ) THEN 'L1'
        WHEN jpos.lawson_company_num = 5920
         AND upper(jcode.job_code_desc) = 'LEADERSHIP DEVELOPMENT' THEN 'L3'
        WHEN jpos.pay_grade_code IN(
          'C34', 'C35', 'C36', 'C37', 'C38', 'C88', 'O34', 'O35', 'O36', 'O37', 'O38', 'O39', 'O40'
        )
         AND jpos.lawson_company_num IN (
          5920, 5959, 5950
        ) THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'AVP%' THEN 'L3'
        WHEN upper(jpos.pay_grade_schedule_code) = 'CORP OS' THEN 'L2'
        WHEN upper(jpos.pay_grade_schedule_code) = 'FAC OS' THEN 'L2'
        WHEN upper(jpos.pay_grade_schedule_code) = 'DIV EXEC' THEN 'L2'
        WHEN jcode.job_code_desc IN(
          'Controller', 'Asst Controller'
        ) THEN 'L3'
        WHEN upper(jcode.job_code_desc) LIKE 'COO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CFO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CEO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CNO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'SVP%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'COO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CIO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CEO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CFO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CFO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CEO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'DIV CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CNE%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV PRESIDENT%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE '%DIR%'
         OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%'
         OR upper(jpos.position_code_desc) LIKE 'DIV AVP%'
         OR upper(jpos.position_code_desc) LIKE 'DISA%' THEN 'L3'
        WHEN (upper(jcode.job_code_desc) LIKE '%DIR%'
         OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%')
         AND upper(jpos.pay_grade_schedule_code) LIKE 'NATLDIR%' THEN 'L3'
        WHEN jpos.lawson_company_num = 5920
         AND upper(jcode.job_code) = '01722'
         AND upper(jpos.position_code_desc) LIKE 'VP HR%' THEN 'L3'
        WHEN jpos.lawson_company_num IN (
          5920, 5959, 5950
        )
         AND (upper(jpos.position_code_desc) LIKE 'VP%'
         OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
         OR upper(jpos.position_code_desc) LIKE 'RVP%'
         OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
         OR upper(jpos.position_code_desc) LIKE 'GROUP VP%') THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'VP%'
         OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
         OR upper(jpos.position_code_desc) LIKE 'RVP%'
         OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
         OR upper(jpos.position_code_desc) LIKE 'GROUP VP%' THEN 'L3'
        WHEN upper(jpos.position_code_desc) LIKE '%MGR%'
         OR upper(jpos.position_code_desc) LIKE '%MANAGER%' THEN 'L4'
        WHEN upper(jpos.position_code_desc) LIKE 'SUP%'
         OR upper(jpos.position_code_desc) LIKE '%SUPERVISOR%' THEN 'L4'
        WHEN upper(jcode.job_code_desc) = 'RECRUITING LEADERSHIP'
         AND upper(jpos.position_code_desc) LIKE '%LEAD%'
         AND upper(jpos.pay_grade_schedule_code) LIKE 'NONPEP%' THEN 'L4'
        ELSE CAST(NULL as STRING)
      END, rdi4.leadership_level_code, CAST(NULL as STRING))) = 'L1' THEN 'Executive Leadership'
      WHEN upper(coalesce(rdi1.leadership_level_code, rdi3.leadership_level_code, rdi2.leadership_level_code, CASE
        WHEN jpos.pay_grade_code IN(
          'CEO', 'EVP', 'GCF', 'GPR', 'O40', 'SV1', 'SV2', 'SV3', 'SV4', 'SV5', 'SV6', 'GC2', 'GP1'
        ) THEN 'L1'
        WHEN jpos.lawson_company_num = 5920
         AND upper(jcode.job_code_desc) = 'LEADERSHIP DEVELOPMENT' THEN 'L3'
        WHEN jpos.pay_grade_code IN(
          'C34', 'C35', 'C36', 'C37', 'C38', 'C88', 'O34', 'O35', 'O36', 'O37', 'O38', 'O39', 'O40'
        )
         AND jpos.lawson_company_num IN (
          5920, 5959, 5950
        ) THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'AVP%' THEN 'L3'
        WHEN upper(jpos.pay_grade_schedule_code) = 'CORP OS' THEN 'L2'
        WHEN upper(jpos.pay_grade_schedule_code) = 'FAC OS' THEN 'L2'
        WHEN upper(jpos.pay_grade_schedule_code) = 'DIV EXEC' THEN 'L2'
        WHEN jcode.job_code_desc IN(
          'Controller', 'Asst Controller'
        ) THEN 'L3'
        WHEN upper(jcode.job_code_desc) LIKE 'COO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CFO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CEO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CNO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'SVP%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'COO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CIO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CEO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CFO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CFO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CEO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'DIV CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CNE%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV PRESIDENT%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE '%DIR%'
         OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%'
         OR upper(jpos.position_code_desc) LIKE 'DIV AVP%'
         OR upper(jpos.position_code_desc) LIKE 'DISA%' THEN 'L3'
        WHEN (upper(jcode.job_code_desc) LIKE '%DIR%'
         OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%')
         AND upper(jpos.pay_grade_schedule_code) LIKE 'NATLDIR%' THEN 'L3'
        WHEN jpos.lawson_company_num = 5920
         AND upper(jcode.job_code) = '01722'
         AND upper(jpos.position_code_desc) LIKE 'VP HR%' THEN 'L3'
        WHEN jpos.lawson_company_num IN (
          5920, 5959, 5950
        )
         AND (upper(jpos.position_code_desc) LIKE 'VP%'
         OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
         OR upper(jpos.position_code_desc) LIKE 'RVP%'
         OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
         OR upper(jpos.position_code_desc) LIKE 'GROUP VP%') THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'VP%'
         OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
         OR upper(jpos.position_code_desc) LIKE 'RVP%'
         OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
         OR upper(jpos.position_code_desc) LIKE 'GROUP VP%' THEN 'L3'
        WHEN upper(jpos.position_code_desc) LIKE '%MGR%'
         OR upper(jpos.position_code_desc) LIKE '%MANAGER%' THEN 'L4'
        WHEN upper(jpos.position_code_desc) LIKE 'SUP%'
         OR upper(jpos.position_code_desc) LIKE '%SUPERVISOR%' THEN 'L4'
        WHEN upper(jcode.job_code_desc) = 'RECRUITING LEADERSHIP'
         AND upper(jpos.position_code_desc) LIKE '%LEAD%'
         AND upper(jpos.pay_grade_schedule_code) LIKE 'NONPEP%' THEN 'L4'
        ELSE CAST(NULL as STRING)
      END, rdi4.leadership_level_code, CAST(NULL as STRING))) = 'L2' THEN 'Senior Leadership'
      WHEN upper(coalesce(rdi1.leadership_level_code, rdi3.leadership_level_code, rdi2.leadership_level_code, CASE
        WHEN jpos.pay_grade_code IN(
          'CEO', 'EVP', 'GCF', 'GPR', 'O40', 'SV1', 'SV2', 'SV3', 'SV4', 'SV5', 'SV6', 'GC2', 'GP1'
        ) THEN 'L1'
        WHEN jpos.lawson_company_num = 5920
         AND upper(jcode.job_code_desc) = 'LEADERSHIP DEVELOPMENT' THEN 'L3'
        WHEN jpos.pay_grade_code IN(
          'C34', 'C35', 'C36', 'C37', 'C38', 'C88', 'O34', 'O35', 'O36', 'O37', 'O38', 'O39', 'O40'
        )
         AND jpos.lawson_company_num IN (
          5920, 5959, 5950
        ) THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'AVP%' THEN 'L3'
        WHEN upper(jpos.pay_grade_schedule_code) = 'CORP OS' THEN 'L2'
        WHEN upper(jpos.pay_grade_schedule_code) = 'FAC OS' THEN 'L2'
        WHEN upper(jpos.pay_grade_schedule_code) = 'DIV EXEC' THEN 'L2'
        WHEN jcode.job_code_desc IN(
          'Controller', 'Asst Controller'
        ) THEN 'L3'
        WHEN upper(jcode.job_code_desc) LIKE 'COO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CFO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CEO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CNO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'SVP%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'COO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CIO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CEO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CFO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CFO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CEO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'DIV CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CNE%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV PRESIDENT%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE '%DIR%'
         OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%'
         OR upper(jpos.position_code_desc) LIKE 'DIV AVP%'
         OR upper(jpos.position_code_desc) LIKE 'DISA%' THEN 'L3'
        WHEN (upper(jcode.job_code_desc) LIKE '%DIR%'
         OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%')
         AND upper(jpos.pay_grade_schedule_code) LIKE 'NATLDIR%' THEN 'L3'
        WHEN jpos.lawson_company_num = 5920
         AND upper(jcode.job_code) = '01722'
         AND upper(jpos.position_code_desc) LIKE 'VP HR%' THEN 'L3'
        WHEN jpos.lawson_company_num IN (
          5920, 5959, 5950
        )
         AND (upper(jpos.position_code_desc) LIKE 'VP%'
         OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
         OR upper(jpos.position_code_desc) LIKE 'RVP%'
         OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
         OR upper(jpos.position_code_desc) LIKE 'GROUP VP%') THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'VP%'
         OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
         OR upper(jpos.position_code_desc) LIKE 'RVP%'
         OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
         OR upper(jpos.position_code_desc) LIKE 'GROUP VP%' THEN 'L3'
        WHEN upper(jpos.position_code_desc) LIKE '%MGR%'
         OR upper(jpos.position_code_desc) LIKE '%MANAGER%' THEN 'L4'
        WHEN upper(jpos.position_code_desc) LIKE 'SUP%'
         OR upper(jpos.position_code_desc) LIKE '%SUPERVISOR%' THEN 'L4'
        WHEN upper(jcode.job_code_desc) = 'RECRUITING LEADERSHIP'
         AND upper(jpos.position_code_desc) LIKE '%LEAD%'
         AND upper(jpos.pay_grade_schedule_code) LIKE 'NONPEP%' THEN 'L4'
        ELSE CAST(NULL as STRING)
      END, rdi4.leadership_level_code, CAST(NULL as STRING))) = 'L3' THEN 'Mid-Level Leadership'
      WHEN upper(coalesce(rdi1.leadership_level_code, rdi3.leadership_level_code, rdi2.leadership_level_code, CASE
        WHEN jpos.pay_grade_code IN(
          'CEO', 'EVP', 'GCF', 'GPR', 'O40', 'SV1', 'SV2', 'SV3', 'SV4', 'SV5', 'SV6', 'GC2', 'GP1'
        ) THEN 'L1'
        WHEN jpos.lawson_company_num = 5920
         AND upper(jcode.job_code_desc) = 'LEADERSHIP DEVELOPMENT' THEN 'L3'
        WHEN jpos.pay_grade_code IN(
          'C34', 'C35', 'C36', 'C37', 'C38', 'C88', 'O34', 'O35', 'O36', 'O37', 'O38', 'O39', 'O40'
        )
         AND jpos.lawson_company_num IN (
          5920, 5959, 5950
        ) THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'AVP%' THEN 'L3'
        WHEN upper(jpos.pay_grade_schedule_code) = 'CORP OS' THEN 'L2'
        WHEN upper(jpos.pay_grade_schedule_code) = 'FAC OS' THEN 'L2'
        WHEN upper(jpos.pay_grade_schedule_code) = 'DIV EXEC' THEN 'L2'
        WHEN jcode.job_code_desc IN(
          'Controller', 'Asst Controller'
        ) THEN 'L3'
        WHEN upper(jcode.job_code_desc) LIKE 'COO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CFO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CEO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CNO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'SVP%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'COO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CIO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CEO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CFO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CFO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CEO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'DIV CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CNE%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV PRESIDENT%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE '%DIR%'
         OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%'
         OR upper(jpos.position_code_desc) LIKE 'DIV AVP%'
         OR upper(jpos.position_code_desc) LIKE 'DISA%' THEN 'L3'
        WHEN (upper(jcode.job_code_desc) LIKE '%DIR%'
         OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%')
         AND upper(jpos.pay_grade_schedule_code) LIKE 'NATLDIR%' THEN 'L3'
        WHEN jpos.lawson_company_num = 5920
         AND upper(jcode.job_code) = '01722'
         AND upper(jpos.position_code_desc) LIKE 'VP HR%' THEN 'L3'
        WHEN jpos.lawson_company_num IN (
          5920, 5959, 5950
        )
         AND (upper(jpos.position_code_desc) LIKE 'VP%'
         OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
         OR upper(jpos.position_code_desc) LIKE 'RVP%'
         OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
         OR upper(jpos.position_code_desc) LIKE 'GROUP VP%') THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'VP%'
         OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
         OR upper(jpos.position_code_desc) LIKE 'RVP%'
         OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
         OR upper(jpos.position_code_desc) LIKE 'GROUP VP%' THEN 'L3'
        WHEN upper(jpos.position_code_desc) LIKE '%MGR%'
         OR upper(jpos.position_code_desc) LIKE '%MANAGER%' THEN 'L4'
        WHEN upper(jpos.position_code_desc) LIKE 'SUP%'
         OR upper(jpos.position_code_desc) LIKE '%SUPERVISOR%' THEN 'L4'
        WHEN upper(jcode.job_code_desc) = 'RECRUITING LEADERSHIP'
         AND upper(jpos.position_code_desc) LIKE '%LEAD%'
         AND upper(jpos.pay_grade_schedule_code) LIKE 'NONPEP%' THEN 'L4'
        ELSE CAST(NULL as STRING)
      END, rdi4.leadership_level_code, CAST(NULL as STRING))) = 'L4' THEN 'First Level Leadership'
      WHEN upper(coalesce(rdi1.leadership_level_code, rdi3.leadership_level_code, rdi2.leadership_level_code, CASE
        WHEN jpos.pay_grade_code IN(
          'CEO', 'EVP', 'GCF', 'GPR', 'O40', 'SV1', 'SV2', 'SV3', 'SV4', 'SV5', 'SV6', 'GC2', 'GP1'
        ) THEN 'L1'
        WHEN jpos.lawson_company_num = 5920
         AND upper(jcode.job_code_desc) = 'LEADERSHIP DEVELOPMENT' THEN 'L3'
        WHEN jpos.pay_grade_code IN(
          'C34', 'C35', 'C36', 'C37', 'C38', 'C88', 'O34', 'O35', 'O36', 'O37', 'O38', 'O39', 'O40'
        )
         AND jpos.lawson_company_num IN (
          5920, 5959, 5950
        ) THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'AVP%' THEN 'L3'
        WHEN upper(jpos.pay_grade_schedule_code) = 'CORP OS' THEN 'L2'
        WHEN upper(jpos.pay_grade_schedule_code) = 'FAC OS' THEN 'L2'
        WHEN upper(jpos.pay_grade_schedule_code) = 'DIV EXEC' THEN 'L2'
        WHEN jcode.job_code_desc IN(
          'Controller', 'Asst Controller'
        ) THEN 'L3'
        WHEN upper(jcode.job_code_desc) LIKE 'COO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CFO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CEO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CNO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'SVP%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'COO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CIO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CEO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'CFO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CFO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CEO%' THEN 'L2'
        WHEN upper(jcode.job_code_desc) LIKE 'DIV CMO%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV CNE%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'DIV PRESIDENT%' THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE '%DIR%'
         OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%'
         OR upper(jpos.position_code_desc) LIKE 'DIV AVP%'
         OR upper(jpos.position_code_desc) LIKE 'DISA%' THEN 'L3'
        WHEN (upper(jcode.job_code_desc) LIKE '%DIR%'
         OR upper(jpos.position_code_desc) LIKE '%DIRECTOR%')
         AND upper(jpos.pay_grade_schedule_code) LIKE 'NATLDIR%' THEN 'L3'
        WHEN jpos.lawson_company_num = 5920
         AND upper(jcode.job_code) = '01722'
         AND upper(jpos.position_code_desc) LIKE 'VP HR%' THEN 'L3'
        WHEN jpos.lawson_company_num IN (
          5920, 5959, 5950
        )
         AND (upper(jpos.position_code_desc) LIKE 'VP%'
         OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
         OR upper(jpos.position_code_desc) LIKE 'RVP%'
         OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
         OR upper(jpos.position_code_desc) LIKE 'GROUP VP%') THEN 'L2'
        WHEN upper(jpos.position_code_desc) LIKE 'VP%'
         OR upper(jpos.position_code_desc) LIKE 'VICE PRESIDENT%'
         OR upper(jpos.position_code_desc) LIKE 'RVP%'
         OR upper(jpos.position_code_desc) LIKE 'DIV VP%'
         OR upper(jpos.position_code_desc) LIKE 'GROUP VP%' THEN 'L3'
        WHEN upper(jpos.position_code_desc) LIKE '%MGR%'
         OR upper(jpos.position_code_desc) LIKE '%MANAGER%' THEN 'L4'
        WHEN upper(jpos.position_code_desc) LIKE 'SUP%'
         OR upper(jpos.position_code_desc) LIKE '%SUPERVISOR%' THEN 'L4'
        WHEN upper(jcode.job_code_desc) = 'RECRUITING LEADERSHIP'
         AND upper(jpos.position_code_desc) LIKE '%LEAD%'
         AND upper(jpos.pay_grade_schedule_code) LIKE 'NONPEP%' THEN 'L4'
        ELSE CAST(NULL as STRING)
      END, rdi4.leadership_level_code, CAST(NULL as STRING))) = 'L5' THEN 'Individual Contributors'
      ELSE CAST(NULL as STRING)
    END AS leadership_level_desc
  FROM
    {{ params.param_hr_views_dataset_name }}.job_position AS jpos 
    LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.job_code AS jcode ON jpos.job_code_sid = jcode.job_code_sid
     AND DATE(jcode.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.job_class AS jclass ON jcode.job_class_sid = jclass.job_class_sid
     AND DATE(jclass.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.ref_nursing_director AS ndir ON jcode.job_code = ndir.job_code
    LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.ref_patient_care_position AS pct ON upper(trim(pct.job_title_text)) = upper(trim(jpos.position_code_desc))
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON jpos.process_level_code = xwalk.process_level_code
     AND jpos.account_unit_num = xwalk.account_unit_num
     AND DATE(xwalk.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON ff.coid = xwalk.coid
    LEFT OUTER JOIN --  Begin Leadership Level Joins --
    {{ params.param_hr_views_dataset_name }}.ref_diversity_inclusion AS rdi1 ON jclass.job_class_code = rdi1.job_class_code
     AND rdi1.match_level_num = 1
    LEFT OUTER JOIN --  Match level 1 is Job Class Code --
    {{ params.param_hr_views_dataset_name }}.ref_diversity_inclusion AS rdi2 ON ff.lob_code = rdi2.lob_code
     AND jclass.job_class_code = rdi2.job_class_code
     AND jcode.job_code = rdi2.job_code
     AND rdi2.match_level_num = 2
    LEFT OUTER JOIN --  Match level 2 is LOB, Job Code --
    {{ params.param_hr_views_dataset_name }}.ref_diversity_inclusion AS rdi3 ON ff.lob_code = rdi3.lob_code
     AND jcode.job_code = rdi3.job_code
     AND upper(trim(jcode.job_code_desc)) = upper(trim(rdi3.leadership_level_desc))
     AND rdi3.match_level_num = 3
    LEFT OUTER JOIN --  Match level 3 is LOB, Job Code, Job Code Desc --
    {{ params.param_hr_views_dataset_name }}.ref_diversity_inclusion AS rdi4 ON ff.lob_code = rdi4.lob_code
     AND jcode.job_code = rdi4.job_code
     AND upper(trim(jpos.position_code_desc)) = upper(trim(rdi4.leadership_level_desc))
     AND rdi4.match_level_num = 4
  WHERE DATE(jpos.valid_to_date) = DATE('9999-12-31')
   AND jcode.job_code IS NOT NULL
