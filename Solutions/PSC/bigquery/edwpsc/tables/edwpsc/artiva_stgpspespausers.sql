CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspespausers
(
  pspesparelukey NUMERIC(29) NOT NULL,
  pspespareluser STRING,
  pspespareluspaid NUMERIC(29),
  PRIMARY KEY (pspesparelukey) NOT ENFORCED
)
;