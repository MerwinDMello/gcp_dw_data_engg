CREATE TABLE IF NOT EXISTS edwpsc.ecw_refusersmeditechexpanse
(
  userkey INT64 NOT NULL,
  regionkey INT64,
  username STRING,
  userfirstname STRING,
  userlastname STRING,
  usermiddlename STRING,
  userfullname STRING,
  userprofileid STRING,
  userproviderid STRING,
  deleteflag INT64,
  sourceprimarykeyvalue STRING,
  sourcearecordlastupdated DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (userkey) NOT ENFORCED
)
;