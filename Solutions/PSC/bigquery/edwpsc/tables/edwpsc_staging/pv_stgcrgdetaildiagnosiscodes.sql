CREATE TABLE IF NOT EXISTS edwpsc_staging.pv_stgcrgdetaildiagnosiscodes
(
  crgheader_diagnosiscodepk STRING NOT NULL,
  crgheader_diagnosiscodepk_txt STRING NOT NULL,
  crgdetailpk STRING NOT NULL,
  crgdetailpk_txt STRING NOT NULL,
  createdon DATETIME NOT NULL,
  createdby STRING NOT NULL,
  priority INT64,
  regionkey INT64 NOT NULL,
  ts BYTES,
  inserteddtm DATETIME NOT NULL,
  modifieddtm DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcephysicaldeleteflag BOOL NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  modifiedby STRING
)
;