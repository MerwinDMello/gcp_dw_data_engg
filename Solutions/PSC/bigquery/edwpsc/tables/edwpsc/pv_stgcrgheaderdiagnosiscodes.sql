CREATE TABLE IF NOT EXISTS edwpsc.pv_stgcrgheaderdiagnosiscodes
(
  crgheaderdiagnosiscodepk STRING NOT NULL,
  crgheaderdiagnosiscodepk_txt STRING NOT NULL,
  crgheaderpk STRING NOT NULL,
  crgheaderpk_txt STRING NOT NULL,
  icd9code STRING,
  icd10code STRING,
  snomed STRING,
  priority INT64 NOT NULL,
  createdon DATETIME NOT NULL,
  createdby STRING NOT NULL,
  icd9override BOOL,
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