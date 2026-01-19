CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspecpidpay
(
  pspecpkey NUMERIC(29) NOT NULL,
  pspecpcpidid STRING,
  pspecppayid STRING,
  PRIMARY KEY (pspecpkey) NOT ENFORCED
)
;