TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.enwisen_audit; 
INSERT INTO {{ params.param_hr_stage_dataset_name }}.enwisen_audit
(
  hrconumber,
  applicantnumber,
  employeeid,
  processlevel,
  processleveldesc,
  requisitionnumber,
  userid,
  firstname,
  lastname,
  ssn,
  hrstatus,
  basedate,
  startdate,
  tourname,
  tourstatus,
  tourpercent,
  workflowname,
  workflowstatus,
  approvaldate,
  datastatus,
  workflowapprover,
  approverlastname,
  approverfirstname,
  lob,
  dw_last_update_date_time
)
SELECT
  hrconumber,
  applicantnumber,
  employeeid,
  processlevel,
  processleveldesc,
  requisitionnumber,
  userid,
  firstname,
  lastname,
  ssn,
  hrstatus,
  basedate,
  startdate,
  tourname,
  tourstatus,
  tourpercent,
  NULLIF(Trim(workflowname),'') as workflowname,
  NULLIF(Trim(workflowstatus),'') as workflowstatus,
  approvaldate,
  datastatus,
  NULLIF(Trim(workflowapprover),'') as workflowapprover,
  approverlastname,
  approverfirstname,
  lob,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM {{ params.param_hr_stage_dataset_name }}.enwisen_audit_ext
WHERE SAFE_CAST(hrconumber AS INT64) IS NOT NULL
AND SAFE_CAST(employeeid AS INT64) IS NOT NULL
AND SAFE_CAST(applicantnumber AS INT64) IS NOT NULL
AND SAFE_CAST(requisitionnumber AS INT64) IS NOT NULL
;
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.enwisen_audit_rej; 
INSERT INTO {{ params.param_hr_stage_dataset_name }}.enwisen_audit_rej
(
  hrconumber,
  applicantnumber,
  employeeid,
  processlevel,
  processleveldesc,
  requisitionnumber,
  userid,
  firstname,
  lastname,
  ssn,
  hrstatus,
  basedate,
  startdate,
  tourname,
  tourstatus,
  tourpercent,
  workflowname,
  workflowstatus,
  approvaldate,
  datastatus,
  workflowapprover,
  approverlastname,
  approverfirstname,
  lob,
  dw_last_update_date_time
)
SELECT
  hrconumber,
  applicantnumber,
  employeeid,
  processlevel,
  processleveldesc,
  requisitionnumber,
  userid,
  firstname,
  lastname,
  ssn,
  hrstatus,
  basedate,
  startdate,
  tourname,
  tourstatus,
  tourpercent,
  NULLIF(Trim(workflowname),'') as workflowname,
  NULLIF(Trim(workflowstatus),'') as workflowstatus,
  approvaldate,
  datastatus,
  NULLIF(Trim(workflowapprover),'') as workflowapprover,
  approverlastname,
  approverfirstname,
  lob,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM {{ params.param_hr_stage_dataset_name }}.enwisen_audit_ext
WHERE SAFE_CAST(hrconumber AS INT64) IS NULL
OR SAFE_CAST(employeeid AS INT64) IS NULL
OR SAFE_CAST(applicantnumber AS INT64) IS NULL
OR SAFE_CAST(requisitionnumber AS INT64) IS NULL
;