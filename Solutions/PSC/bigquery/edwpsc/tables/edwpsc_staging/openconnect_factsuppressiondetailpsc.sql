CREATE TABLE IF NOT EXISTS edwpsc_staging.openconnect_factsuppressiondetailpsc
(
  openconnectsuppressiondetailkey INT64 NOT NULL,
  messagecreateddate DATETIME,
  routestep STRING,
  statusreason STRING,
  statusmessage STRING,
  deleteflag INT64,
  dwlastupdateddate DATETIME,
  sourceaprimarykeyvalue STRING,
  sendingapplication STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;