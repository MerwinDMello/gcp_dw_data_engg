CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspeppiactionplan
(
  pspeppiapincflag STRING,
  pspeppiapkey NUMERIC(29) NOT NULL,
  pspeppiaporigdte DATETIME,
  pspeppiappanels STRING,
  pspeppiappayid STRING,
  pspeppiapppicrtdte DATETIME,
  pspeppiapppiid NUMERIC(29),
  pspeppiapspatmpid NUMERIC(29),
  pspeppiapsubdte DATETIME,
  pspeppiaprejreason STRING,
  PRIMARY KEY (pspeppiapkey) NOT ENFORCED
)
;