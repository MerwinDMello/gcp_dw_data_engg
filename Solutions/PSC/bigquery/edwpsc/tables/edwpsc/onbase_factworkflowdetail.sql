CREATE TABLE IF NOT EXISTS edwpsc.onbase_factworkflowdetail
(
  documenthandle INT64,
  keyworddocumenthandle INT64,
  keywordentrydate DATE,
  keywordexitdate DATE,
  keywordlifecyclename STRING,
  keyworddocumenttype STRING,
  keywordqueuename STRING,
  keyworduserid STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  onbaseworkflowdetailkey INT64 NOT NULL,
  dwlastupdatedatetime DATETIME
)
;