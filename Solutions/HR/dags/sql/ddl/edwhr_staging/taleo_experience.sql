CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_experience
(
  file_date DATE,
  taleo_number INT64,
  profileinformation_number INT64,
  begindate DATE,
  currentemployer STRING,
  displaysequence STRING,
  enddate DATE,
  lastmodifieddate DATETIME,
  namewhileemployed STRING,
  otheremployername STRING,
  otherjobtitle STRING,
  permissiontocontact STRING,
  reasonforleaving STRING,
  responsibility STRING,
  supervisor STRING,
  supervisoremail STRING,
  supervisorphone STRING,
  supervisortitle STRING,
  employer_number NUMERIC(29),
  jobfunction_number NUMERIC(29),
  location_number NUMERIC(29),
  dw_last_update_date_time DATETIME
)
;