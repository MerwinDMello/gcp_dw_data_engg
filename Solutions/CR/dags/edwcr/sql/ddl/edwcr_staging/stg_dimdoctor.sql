CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimdoctor (
dimsiteid STRING
, dimdoctorid STRING
, dimlocationid STRING
, dimlookupid_resourcetype STRING
, doctorfirstname STRING
, doctorlastname STRING
, doctorfullname STRING
, doctoraliasname STRING
, doctorhonorific STRING
, doctornamesuffix STRING
, doctorspecialty STRING
, doctorid STRING
, resourcetypenum STRING
, resourceobjectstatus STRING
, schedulable STRING
, doctorcompleteaddress STRING
, isprimarydoctoraddress STRING
, doctoraddresstype STRING
, doctoraddresscomment STRING
, doctorprimaryphonenumber STRING
, doctorsecondaryphonenumber STRING
, doctorpagernumber STRING
, doctorfaxnumber STRING
, doctoremailaddress STRING
, doctororiginationdate STRING
, doctorterminationdate STRING
, doctorinstitution STRING
, doctorcomment STRING
, ctrresourceser STRING
, logid STRING
, runid STRING
, ctrstkh_id STRING
, dw_last_update_date_time DATETIME
)
  ;
