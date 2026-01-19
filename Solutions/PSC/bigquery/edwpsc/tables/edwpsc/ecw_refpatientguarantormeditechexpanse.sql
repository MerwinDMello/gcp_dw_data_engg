CREATE TABLE IF NOT EXISTS edwpsc.ecw_refpatientguarantormeditechexpanse
(
  patientguarantorkey INT64 NOT NULL,
  regionkey INT64,
  guarantornumber STRING,
  relationshipcode STRING,
  relationshipname STRING,
  guarantorlastname STRING,
  guarantornamefirst STRING,
  guarantornamemiddle STRING,
  guarantoraddressline1 STRING,
  guarantoraddressline2 STRING,
  guarantorgeographykey INT64,
  sourceaprimarykeyvalue STRING,
  sourcearecordlastupdated DATETIME,
  sourcebprimarykeyvalue STRING,
  sourcebrecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (patientguarantorkey) NOT ENFORCED
)
;