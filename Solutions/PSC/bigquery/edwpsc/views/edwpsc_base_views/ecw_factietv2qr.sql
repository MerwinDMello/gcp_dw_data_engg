CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factietv2qr`
AS SELECT
  `ecw_factietv2qr`.ietv2qrkey,
  `ecw_factietv2qr`.rownumber,
  `ecw_factietv2qr`.correspondenceid,
  `ecw_factietv2qr`.content,
  `ecw_factietv2qr`.datecreated,
  `ecw_factietv2qr`.usercreated,
  `ecw_factietv2qr`.correspondencesubjectid,
  `ecw_factietv2qr`.claimcaseid,
  `ecw_factietv2qr`.claimnumber,
  `ecw_factietv2qr`.claimkey,
  `ecw_factietv2qr`.inbound,
  `ecw_factietv2qr`.resolution,
  `ecw_factietv2qr`.resolutionid,
  `ecw_factietv2qr`.resolutiondate,
  `ecw_factietv2qr`.resolutionuserid,
  `ecw_factietv2qr`.insertedby,
  `ecw_factietv2qr`.inserteddtm,
  `ecw_factietv2qr`.modifiedby,
  `ecw_factietv2qr`.modifieddtm,
  `ecw_factietv2qr`.fullclaimnumber,
  `ecw_factietv2qr`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_factietv2qr`
;