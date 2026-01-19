CREATE OR REPLACE VIEW edwpsc_base_views.`epic_factrh1301`
AS SELECT
  `epic_factrh1301`.rh1301key,
  `epic_factrh1301`.claimkey,
  `epic_factrh1301`.claimnumber,
  `epic_factrh1301`.regionkey,
  `epic_factrh1301`.coid,
  `epic_factrh1301`.importdatekey,
  `epic_factrh1301`.rh1301claimid,
  `epic_factrh1301`.rh1301claimdatekey,
  `epic_factrh1301`.rh1301totalamt,
  `epic_factrh1301`.rh1301insurancebilledname,
  `epic_factrh1301`.rh1301errorfieldname,
  `epic_factrh1301`.rh1301errorindex,
  `epic_factrh1301`.rh1301errordata,
  `epic_factrh1301`.rh1301errordescription,
  `epic_factrh1301`.rh1301stmtthrudatekey,
  `epic_factrh1301`.rh1301filename,
  `epic_factrh1301`.sourceprimarykeyvalue,
  `epic_factrh1301`.dwlastupdatedatetime,
  `epic_factrh1301`.sourcesystemcode,
  `epic_factrh1301`.insertedby,
  `epic_factrh1301`.inserteddtm,
  `epic_factrh1301`.modifiedby,
  `epic_factrh1301`.modifieddtm
  FROM
    edwpsc.`epic_factrh1301`
;