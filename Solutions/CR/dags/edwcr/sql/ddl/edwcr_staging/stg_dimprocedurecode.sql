CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.stg_dimprocedurecode (
dimsiteid STRING
, dimprocedurecodeid STRING
, procedurecode STRING
, procedurecodetype STRING
, procedurecodedescription STRING
, description STRING
, procedurebillingcode STRING
, complexity STRING
, procedurecoderevcount STRING
, exportable STRING
, ctrprocedurecodeser STRING
, logid STRING
, runid STRING
, billingcodeeffectivedate STRING
, billingcodeestcost STRING
, billingcodebillprice STRING
, billingcodetype STRING
, activeind STRING
, changedate STRING
, dimdateid_changedate STRING
, facilitybillcode STRING
, dimlookupid_proccodeobjectstat STRING
, moroindicator STRING
, dw_last_update_date_time DATETIME
)
  ;
