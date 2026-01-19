CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factencountermeditechexpansenotes
(
  encountermtxnoteskey INT64 NOT NULL,
  sourceid STRING,
  visitid STRING,
  codingaccountvisitdatetimeid DATETIME,
  codinghistoryurnid INT64,
  rowupdatedatetime DATETIME,
  codinghistorydatetime DATETIME,
  codinghistorytype STRING,
  codinghistoryuser_unvuserid STRING,
  codinghistoryoldvalue STRING,
  codinghistorynewvalue STRING,
  codinghistoryinformationonly STRING,
  codinghistoryadditionaldata STRING,
  regionid INT64,
  accountnumber STRING,
  textseqid INT64,
  textid NUMERIC(29),
  textline STRING,
  texttimestamp STRING,
  sourcesystemcode STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  insertedby INT64,
  inserteddtm DATETIME,
  modifiedby INT64,
  modifieddtm DATETIME
)
;