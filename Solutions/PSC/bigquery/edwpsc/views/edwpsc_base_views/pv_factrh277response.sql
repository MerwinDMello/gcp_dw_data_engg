CREATE OR REPLACE VIEW edwpsc_base_views.`pv_factrh277response`
AS SELECT
  `pv_factrh277response`.rh277responsekey,
  `pv_factrh277response`.claimkey,
  `pv_factrh277response`.claimnumber,
  `pv_factrh277response`.regionkey,
  `pv_factrh277response`.coid,
  `pv_factrh277response`.importdatekey,
  `pv_factrh277response`.rh277responseclaimid,
  `pv_factrh277response`.rh277responsepatientcontrolnbr,
  `pv_factrh277response`.rh277responsedateimportedkey,
  `pv_factrh277response`.rh277responserespondingpayer,
  `pv_factrh277response`.rh277responsestatusctgycode,
  `pv_factrh277response`.rh277responsestatuscode,
  `pv_factrh277response`.rh277filename,
  `pv_factrh277response`.sourceprimarykeyvalue,
  `pv_factrh277response`.dwlastupdatedatetime,
  `pv_factrh277response`.sourcesystemcode,
  `pv_factrh277response`.insertedby,
  `pv_factrh277response`.inserteddtm,
  `pv_factrh277response`.modifiedby,
  `pv_factrh277response`.modifieddtm
  FROM
    edwpsc.`pv_factrh277response`
;