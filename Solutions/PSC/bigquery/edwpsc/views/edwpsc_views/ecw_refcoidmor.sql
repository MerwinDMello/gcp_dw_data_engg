CREATE OR REPLACE VIEW edwpsc_views.`ecw_refcoidmor`
AS SELECT
  `ecw_refcoidmor`.coid,
  `ecw_refcoidmor`.gldepartmentnumber,
  `ecw_refcoidmor`.groupname,
  `ecw_refcoidmor`.groupcode,
  `ecw_refcoidmor`.divisionname,
  `ecw_refcoidmor`.divisioncode,
  `ecw_refcoidmor`.marketname,
  `ecw_refcoidmor`.marketcode,
  `ecw_refcoidmor`.coidname,
  `ecw_refcoidmor`.coidwithname,
  `ecw_refcoidmor`.centerdescription,
  `ecw_refcoidmor`.coidlob,
  `ecw_refcoidmor`.sublobname,
  `ecw_refcoidmor`.coidstartdatekey,
  `ecw_refcoidmor`.coidtermdatekey,
  `ecw_refcoidmor`.hospitalcoid,
  `ecw_refcoidmor`.hospitalname,
  `ecw_refcoidmor`.providername,
  `ecw_refcoidmor`.providerstartdatekey,
  `ecw_refcoidmor`.providertermdatekey,
  `ecw_refcoidmor`.specialtycategory,
  `ecw_refcoidmor`.specialtycode,
  `ecw_refcoidmor`.specialtyname,
  `ecw_refcoidmor`.specialtytype,
  `ecw_refcoidmor`.coidprogramtype,
  `ecw_refcoidmor`.coidlargepractice,
  `ecw_refcoidmor`.provideroriginalstartstatus,
  `ecw_refcoidmor`.provideroriginalstartdate,
  `ecw_refcoidmor`.appspecialtycategory,
  `ecw_refcoidmor`.dwlastupdatedatetime,
  `ecw_refcoidmor`.sourcesystemcode,
  `ecw_refcoidmor`.insertedby,
  `ecw_refcoidmor`.inserteddtm,
  `ecw_refcoidmor`.modifiedby,
  `ecw_refcoidmor`.modifieddtm,
  `ecw_refcoidmor`.hashcode
  FROM
    edwpsc_base_views.`ecw_refcoidmor`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_refcoidmor`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;