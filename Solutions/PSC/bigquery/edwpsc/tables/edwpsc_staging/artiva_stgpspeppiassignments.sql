CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeppiassignments
(
  pspeasgaddpayid STRING,
  pspeasgassoccoid STRING,
  pspeasgassoctaxid STRING,
  pspeasggafid STRING,
  pspeasggin STRING,
  pspeasgkey NUMERIC(29) NOT NULL,
  pspeasgorigenasgkey STRING,
  pspeasgpayorid STRING,
  pspeasgperfid STRING,
  pspeasgphysaddr STRING,
  pspeasgstdte DATETIME,
  PRIMARY KEY (pspeasgkey) NOT ENFORCED
)
;