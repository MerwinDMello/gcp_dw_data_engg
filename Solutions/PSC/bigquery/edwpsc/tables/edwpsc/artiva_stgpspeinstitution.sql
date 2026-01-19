CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspeinstitution
(
  pspeinaddr1 STRING,
  pspeinaddr2 STRING,
  pspeinaffiliations STRING,
  pspeinboards STRING,
  pspeincity STRING,
  pspeincontact STRING,
  pspeincountry STRING,
  pspeineducation STRING,
  pspeinfax STRING,
  pspeinhomepgurl STRING,
  pspeinid STRING,
  pspeininsurance STRING,
  pspeinlicenses STRING,
  pspeinkey STRING NOT NULL,
  pspeinname STRING,
  pspeinplans STRING,
  pspeinprimphone STRING,
  pspeinst STRING,
  pspeintype STRING,
  pspeinzip STRING,
  PRIMARY KEY (pspeinkey) NOT ENFORCED
)
;