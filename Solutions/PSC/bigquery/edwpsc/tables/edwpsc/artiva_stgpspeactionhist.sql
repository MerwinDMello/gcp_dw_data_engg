CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspeactionhist
(
  pspeacthactid STRING,
  pspeacthaviid NUMERIC(29),
  pspeacthdte DATETIME,
  pspeacthid NUMERIC(29) NOT NULL,
  pspeacthnote STRING,
  pspeacthnoteline STRING,
  pspeacthperfid STRING,
  pspeacthpoolid STRING,
  pspeacthppiid STRING,
  pspeacthresid STRING,
  pspeacthstat STRING,
  pspeacthtime DATETIME,
  pspeacthuserid STRING,
  lastnote STRING,
  lastnotecnt INT64,
  lastnoteuserid STRING,
  lastnotedatetime DATETIME,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (pspeacthid) NOT ENFORCED
)
;