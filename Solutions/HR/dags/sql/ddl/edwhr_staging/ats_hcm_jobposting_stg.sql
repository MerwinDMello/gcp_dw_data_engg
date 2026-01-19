CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ats_hcm_jobposting_stg
(
  _action STRING,
  createstamp STRING,
  hrorganization STRING,
  jobboard STRING,
  jobboardkey STRING,
  jobposting INT64,
  jobpostingkey STRING,
  jobpostingrule STRING,
  jobrequisition INT64,
  jobrequisitionkey STRING,
  locationofjobdescriptionforsort STRING,
  locationofjobkey STRING,
  locationofjoblocation1 STRING,
  locationofjoblocation2 STRING,
  locationofjoblocation3 STRING,
  locationofjoblocation4 STRING,
  postingdaterangebegin STRING,
  postingdaterangeend STRING,
  postingstatus INT64,
  postingstatus_state STRING,
  repset_variation_id INT64,
  salaryrangebeginningpay FLOAT64,
  salaryrangeendingpay FLOAT64,
  salaryrangepayrangecurrencycode STRING,
  updatestamp STRING,
  infor_lastmodified STRING,
  dw_last_update_date_time DATETIME
)
OPTIONS(
  
);