CREATE TABLE IF NOT EXISTS edwpsc.openconnect_factsuppressiondetail
(
  openconnectsuppressiondetailkey INT64 NOT NULL,
  messagecreateddate DATETIME,
  routestep STRING,
  statusreason STRING,
  statusmessage STRING,
  deleteflag INT64,
  dwlastupdateddate DATETIME,
  sourceaprimarykeyvalue INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;