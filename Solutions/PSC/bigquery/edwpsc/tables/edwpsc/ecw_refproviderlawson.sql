CREATE TABLE IF NOT EXISTS edwpsc.ecw_refproviderlawson
(
  providerlawsonkey INT64 NOT NULL,
  providerlawsonemployeenumber STRING,
  providerlawsonlastname STRING,
  providerlawsonfirstname STRING,
  providerlawsonmiddlename STRING,
  providerlawsonempstatus STRING,
  providerlawsondepartment STRING,
  providerlawsonjobcode STRING,
  providerlawsondescription STRING,
  providerlawsonannivershired DATE,
  providerlawsonnewhiredate DATE,
  providerlawsonprmcertid STRING,
  coid STRING,
  providerlawsonuserid STRING,
  providerlawsonterminationdate DATE,
  sourceprimarykeyvalue INT64,
  sourcearecordlastupdated DATETIME NOT NULL,
  sourcebrecordlastupdated DATETIME NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  deleteflag INT64
)
;