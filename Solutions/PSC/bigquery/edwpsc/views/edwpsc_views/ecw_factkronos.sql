CREATE OR REPLACE VIEW edwpsc_views.`ecw_factkronos`
AS SELECT
  `ecw_factkronos`.kronoskey,
  `ecw_factkronos`.employeedailytimelogsk,
  `ecw_factkronos`.coid,
  `ecw_factkronos`.companycode,
  `ecw_factkronos`.employeesk,
  `ecw_factkronos`.vendoremployeesrcsyskey,
  `ecw_factkronos`.companyid,
  `ecw_factkronos`.employee34id,
  `ecw_factkronos`.npi,
  `ecw_factkronos`.deptcode,
  `ecw_factkronos`.jobcode,
  `ecw_factkronos`.employeeactuallogindatetime,
  `ecw_factkronos`.employeeactuallogoutdatetime,
  `ecw_factkronos`.employeeactualelapsedtimecnt,
  `ecw_factkronos`.employeeroundedlogindatetime,
  `ecw_factkronos`.employeeroundedlogoutdatetime,
  `ecw_factkronos`.employeeroundedelapsedtimecnt,
  `ecw_factkronos`.paysmrygroupcode,
  `ecw_factkronos`.paycodehourcnt,
  `ecw_factkronos`.dataservercode,
  `ecw_factkronos`.systemcode,
  `ecw_factkronos`.sourcelastupdatedatetime,
  `ecw_factkronos`.deptdesc,
  `ecw_factkronos`.dwlastupdatedatetime,
  `ecw_factkronos`.sourcesystemcode,
  `ecw_factkronos`.insertedby,
  `ecw_factkronos`.inserteddtm,
  `ecw_factkronos`.modifiedby,
  `ecw_factkronos`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_factkronos`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factkronos`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;