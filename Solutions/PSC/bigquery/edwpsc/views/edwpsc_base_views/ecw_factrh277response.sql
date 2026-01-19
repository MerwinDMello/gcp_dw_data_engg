CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factrh277response`
AS SELECT
  `ecw_factrh277response`.rh277responsekey,
  `ecw_factrh277response`.claimkey,
  `ecw_factrh277response`.claimnumber,
  `ecw_factrh277response`.coid,
  `ecw_factrh277response`.importdatekey,
  `ecw_factrh277response`.rh277responseclaimid,
  `ecw_factrh277response`.rh277responsepatientcontrolnbr,
  `ecw_factrh277response`.rh277responsedateimportedkey,
  `ecw_factrh277response`.rh277responserespondingpayer,
  `ecw_factrh277response`.rh277responsestatusctgycode,
  `ecw_factrh277response`.rh277responsestatuscode,
  `ecw_factrh277response`.sourceprimarykeyvalue,
  `ecw_factrh277response`.dwlastupdatedatetime,
  `ecw_factrh277response`.sourcesystemcode,
  `ecw_factrh277response`.insertedby,
  `ecw_factrh277response`.inserteddtm,
  `ecw_factrh277response`.modifiedby,
  `ecw_factrh277response`.modifieddtm,
  `ecw_factrh277response`.fullclaimnumber,
  `ecw_factrh277response`.regionkey
  FROM
    edwpsc.`ecw_factrh277response`
;