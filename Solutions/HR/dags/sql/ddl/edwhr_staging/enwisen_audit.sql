CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.enwisen_audit
(
  hrconumber STRING,
  applicantnumber STRING,
  employeeid STRING,
  processlevel STRING,
  processleveldesc STRING,
  requisitionnumber STRING,
  userid STRING,
  firstname STRING,
  lastname STRING,
  ssn STRING,
  hrstatus STRING,
  basedate STRING,
  startdate STRING,
  tourname STRING,
  tourstatus STRING,
  tourpercent STRING,
  workflowname STRING,
  workflowstatus STRING,
  approvaldate STRING,
  datastatus STRING,
  workflowapprover STRING,
  approverlastname STRING,
  approverfirstname STRING,
  lob STRING,
  dw_last_update_date_time DATETIME
)
;