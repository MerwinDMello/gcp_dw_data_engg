CREATE OR REPLACE VIEW edwpsc_views.`ecw_factdailycash`
AS SELECT
  `ecw_factdailycash`.dailycashkey,
  `ecw_factdailycash`.dateposted,
  `ecw_factdailycash`.sourcesystem,
  `ecw_factdailycash`.icccoidflag,
  `ecw_factdailycash`.paymenttype,
  `ecw_factdailycash`.amount,
  `ecw_factdailycash`.pcnr2moprior,
  `ecw_factdailycash`.totalbankdays,
  `ecw_factdailycash`.runningtotalbankdays,
  `ecw_factdailycash`.insertedby,
  `ecw_factdailycash`.inserteddtm,
  `ecw_factdailycash`.modifiedby,
  `ecw_factdailycash`.modifieddtm,
  `ecw_factdailycash`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_factdailycash`
;