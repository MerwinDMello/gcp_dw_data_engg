CREATE OR REPLACE VIEW edwpsc_views.`pv_factrh1301`
AS SELECT
  `pv_factrh1301`.rh1301key,
  `pv_factrh1301`.claimkey,
  `pv_factrh1301`.claimnumber,
  `pv_factrh1301`.regionkey,
  `pv_factrh1301`.coid,
  `pv_factrh1301`.importdatekey,
  `pv_factrh1301`.rh1301claimid,
  `pv_factrh1301`.rh1301claimdatekey,
  `pv_factrh1301`.rh1301totalamt,
  `pv_factrh1301`.rh1301insurancebilledname,
  `pv_factrh1301`.rh1301errorfieldname,
  `pv_factrh1301`.rh1301errorindex,
  `pv_factrh1301`.rh1301errordata,
  `pv_factrh1301`.rh1301errordescription,
  `pv_factrh1301`.rh1301stmtthrudatekey,
  `pv_factrh1301`.rh1301filename,
  `pv_factrh1301`.sourceprimarykeyvalue,
  `pv_factrh1301`.dwlastupdatedatetime,
  `pv_factrh1301`.sourcesystemcode,
  `pv_factrh1301`.insertedby,
  `pv_factrh1301`.inserteddtm,
  `pv_factrh1301`.modifiedby,
  `pv_factrh1301`.modifieddtm
  FROM
    edwpsc_base_views.`pv_factrh1301`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factrh1301`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;