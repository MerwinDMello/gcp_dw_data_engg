CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspespautilpayors
(
  pspespatmppayid STRING,
  pspespatmppaykey STRING NOT NULL,
  pspespatmppayprocflg STRING,
  pspespatmppayspaid NUMERIC(29) NOT NULL,
  pspespatmppayovrflg STRING,
  PRIMARY KEY (pspespatmppaykey) NOT ENFORCED
)
;