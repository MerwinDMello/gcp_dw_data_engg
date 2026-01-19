CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factrh1301`
AS SELECT
  `ecw_factrh1301`.rh1301key,
  `ecw_factrh1301`.claimkey,
  `ecw_factrh1301`.claimnumber,
  `ecw_factrh1301`.coid,
  `ecw_factrh1301`.importdatekey,
  `ecw_factrh1301`.rh1301claimid,
  `ecw_factrh1301`.rh1301claimdatekey,
  `ecw_factrh1301`.rh1301totalamt,
  `ecw_factrh1301`.rh1301insurancebilledname,
  `ecw_factrh1301`.rh1301errorfieldname,
  `ecw_factrh1301`.rh1301errorindex,
  `ecw_factrh1301`.rh1301errordata,
  `ecw_factrh1301`.rh1301errordescription,
  `ecw_factrh1301`.rh1301stmtthrudatekey,
  `ecw_factrh1301`.sourceprimarykeyvalue,
  `ecw_factrh1301`.dwlastupdatedatetime,
  `ecw_factrh1301`.sourcesystemcode,
  `ecw_factrh1301`.insertedby,
  `ecw_factrh1301`.inserteddtm,
  `ecw_factrh1301`.modifiedby,
  `ecw_factrh1301`.modifieddtm,
  `ecw_factrh1301`.fullclaimnumber,
  `ecw_factrh1301`.regionkey
  FROM
    edwpsc.`ecw_factrh1301`
;