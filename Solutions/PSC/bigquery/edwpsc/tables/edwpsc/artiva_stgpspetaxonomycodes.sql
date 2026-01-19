CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspetaxonomycodes
(
  pspetaxdesc STRING,
  pspetaxkey STRING NOT NULL,
  pspetaxmcrspccode STRING,
  pspetaxmcrsuptypdesc STRING,
  PRIMARY KEY (pspetaxkey) NOT ENFORCED
)
;