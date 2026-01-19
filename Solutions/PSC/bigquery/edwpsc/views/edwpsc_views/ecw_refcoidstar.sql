CREATE OR REPLACE VIEW edwpsc_views.`ecw_refcoidstar`
AS SELECT
  `ecw_refcoidstar`.coidstarkey,
  `ecw_refcoidstar`.coid,
  `ecw_refcoidstar`.coidname,
  `ecw_refcoidstar`.coidspecialty,
  `ecw_refcoidstar`.coidcompanycode,
  `ecw_refcoidstar`.coidunitnumber,
  `ecw_refcoidstar`.pscrevcyclemgmt,
  `ecw_refcoidstar`.groupkey,
  `ecw_refcoidstar`.groupname,
  `ecw_refcoidstar`.divisionkey,
  `ecw_refcoidstar`.divisionname,
  `ecw_refcoidstar`.marketkey,
  `ecw_refcoidstar`.marketname,
  `ecw_refcoidstar`.statekey,
  `ecw_refcoidstar`.hcaps300,
  `ecw_refcoidstar`.centerkey,
  `ecw_refcoidstar`.centerdescription,
  `ecw_refcoidstar`.serviceline,
  `ecw_refcoidstar`.coidlob,
  `ecw_refcoidstar`.coidsublob,
  `ecw_refcoidstar`.lobname,
  `ecw_refcoidstar`.sublobname,
  `ecw_refcoidstar`.traumaburnflag,
  `ecw_refcoidstar`.dwlastupdatedatetime,
  `ecw_refcoidstar`.insertedby,
  `ecw_refcoidstar`.inserteddtm,
  `ecw_refcoidstar`.modifiedby,
  `ecw_refcoidstar`.modifieddtm,
  `ecw_refcoidstar`.coidwithleadingzero,
  `ecw_refcoidstar`.deleteflag,
  `ecw_refcoidstar`.divisioncode,
  `ecw_refcoidstar`.groupcode,
  `ecw_refcoidstar`.hospitalcoid,
  `ecw_refcoidstar`.hospitalname,
  `ecw_refcoidstar`.marketcode,
  `ecw_refcoidstar`.ppmsflag
  FROM
    edwpsc_base_views.`ecw_refcoidstar`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_refcoidstar`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;