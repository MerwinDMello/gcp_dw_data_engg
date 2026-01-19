CREATE TABLE IF NOT EXISTS edwpsc.ecw_refuserproductivity_history
(
  userproductivitykey INT64 NOT NULL,
  userproductivityname STRING NOT NULL,
  userproductivitytype STRING NOT NULL,
  userproductivitydesc STRING NOT NULL,
  userproductivityquery STRING NOT NULL,
  userproductivitycreatedby STRING NOT NULL,
  userproductivitycreateddatekey DATE NOT NULL,
  userproductivitylastmodifiedby STRING,
  userproductivitylastprocesseddate DATETIME,
  userproductivitylasterrormessage STRING,
  enabled INT64,
  developeremail STRING,
  sysstarttime DATETIME NOT NULL,
  sysendtime DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL
)
;