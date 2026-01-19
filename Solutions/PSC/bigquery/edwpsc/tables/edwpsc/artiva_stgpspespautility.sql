CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspespautility
(
  pspespacounty STRING,
  pspespacrtdte DATETIME,
  pspespacrttime DATETIME,
  pspespakey NUMERIC(29) NOT NULL,
  pspespalbn STRING,
  pspespaprocessflg STRING,
  pspespaspecialty STRING,
  pspespasspdivision STRING,
  pspespastate STRING,
  pspespataxid STRING,
  pspespataxonomy STRING,
  PRIMARY KEY (pspespakey) NOT ENFORCED
)
;