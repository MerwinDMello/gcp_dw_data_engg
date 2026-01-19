CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspeppihist
(
  pspeppihaction STRING,
  pspeppihaviid NUMERIC(29),
  pspeppihdate DATETIME,
  pspeppihfromamtval NUMERIC(33, 4),
  pspeppihfromcalc STRING,
  pspeppihfromdateval DATETIME,
  pspeppihfromval STRING,
  pspeppihkey NUMERIC(29) NOT NULL,
  pspeppihlpool STRING,
  pspeppihperfid STRING,
  pspeppihppiid NUMERIC(29),
  pspeppihtime DATETIME,
  pspeppihtoamtval NUMERIC(33, 4),
  pspeppihtocalc STRING,
  pspeppihtodateval DATETIME,
  pspeppihtoval STRING,
  pspeppihuserid STRING,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (pspeppihkey) NOT ENFORCED
)
;