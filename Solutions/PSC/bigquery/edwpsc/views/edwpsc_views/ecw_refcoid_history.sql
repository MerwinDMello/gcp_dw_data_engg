CREATE OR REPLACE VIEW edwpsc_views.`ecw_refcoid_history`
AS SELECT
  `ecw_refcoid_history`.coid,
  `ecw_refcoid_history`.coidname,
  `ecw_refcoid_history`.statekey,
  `ecw_refcoid_history`.coidconsolidationindicator,
  `ecw_refcoid_history`.coidcompanycode,
  `ecw_refcoid_history`.coidunitnumber,
  `ecw_refcoid_history`.coidlob,
  `ecw_refcoid_history`.coidsublob,
  `ecw_refcoid_history`.marketkey,
  `ecw_refcoid_history`.dwlastupdatedatetime,
  `ecw_refcoid_history`.sourcesystemcode,
  `ecw_refcoid_history`.insertedby,
  `ecw_refcoid_history`.inserteddtm,
  `ecw_refcoid_history`.modifiedby,
  `ecw_refcoid_history`.modifieddtm,
  `ecw_refcoid_history`.lobname,
  `ecw_refcoid_history`.sublobname,
  `ecw_refcoid_history`.deleteflag,
  `ecw_refcoid_history`.coidstatflag,
  `ecw_refcoid_history`.ppmsflag,
  `ecw_refcoid_history`.centerkey,
  `ecw_refcoid_history`.coidspecialty,
  `ecw_refcoid_history`.coidsamestorecode,
  `ecw_refcoid_history`.coidsamestoreflag,
  `ecw_refcoid_history`.pscrevcyclemgmt,
  `ecw_refcoid_history`.sysstarttime,
  `ecw_refcoid_history`.sysendtime,
  `ecw_refcoid_history`.coidwithleadingzero
  FROM
    edwpsc_base_views.`ecw_refcoid_history`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_refcoid_history`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;