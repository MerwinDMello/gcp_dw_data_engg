CREATE TABLE IF NOT EXISTS edwpsc.ccu_factcodingstatusmeditechexpanse
(
  ccucodingstatuskey INT64 NOT NULL,
  regionkey INT64,
  worklistuserkey INT64,
  worklistdatetime DATETIME,
  workliststatus STRING,
  codingstatus STRING,
  codingstatusdate DATETIME,
  codingstatususerkey INT64,
  sourceaprimarykeyvalue STRING,
  sourcearecordlastupdated DATETIME,
  sourcebprimarykeyvalue STRING,
  sourcebrecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;