CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspespataxonpiovr
(
  pspestnkey STRING NOT NULL,
  pspestnnpi STRING,
  pspestnspaid NUMERIC(29),
  pspestntaxonomy STRING,
  PRIMARY KEY (pspestnkey) NOT ENFORCED
)
;