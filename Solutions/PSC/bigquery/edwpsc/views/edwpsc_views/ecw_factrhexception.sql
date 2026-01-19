CREATE OR REPLACE VIEW edwpsc_views.`ecw_factrhexception`
AS SELECT
  `ecw_factrhexception`.rhexceptionkey,
  `ecw_factrhexception`.claimkey,
  `ecw_factrhexception`.claimnumber,
  `ecw_factrhexception`.coid,
  `ecw_factrhexception`.importdatekey,
  `ecw_factrhexception`.rhexceptionclaimid,
  `ecw_factrhexception`.rhexceptionlastname,
  `ecw_factrhexception`.rhexceptionfirstname,
  `ecw_factrhexception`.rhexceptionmi,
  `ecw_factrhexception`.rhexceptionclaimdatekey,
  `ecw_factrhexception`.rhexceptioncontrolnumber,
  `ecw_factrhexception`.rhexceptiontotalamt,
  `ecw_factrhexception`.rhexceptionpayorname,
  `ecw_factrhexception`.rhexceptionattphys,
  `ecw_factrhexception`.rhexceptionfieldname,
  `ecw_factrhexception`.rhexceptionerrdata,
  `ecw_factrhexception`.rhexceptiondescription,
  `ecw_factrhexception`.rhexceptionstmtthrudatekey,
  `ecw_factrhexception`.rhexceptionrootid,
  `ecw_factrhexception`.sourceprimarykeyvalue,
  `ecw_factrhexception`.dwlastupdatedatetime,
  `ecw_factrhexception`.sourcesystemcode,
  `ecw_factrhexception`.insertedby,
  `ecw_factrhexception`.inserteddtm,
  `ecw_factrhexception`.modifiedby,
  `ecw_factrhexception`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_factrhexception`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factrhexception`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;