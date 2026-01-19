CREATE OR REPLACE VIEW edwpsc_views.`ecw_refcoidgldept`
AS SELECT
  `ecw_refcoidgldept`.coid,
  `ecw_refcoidgldept`.gldepartmentnumber,
  `ecw_refcoidgldept`.groupname,
  `ecw_refcoidgldept`.groupcode,
  `ecw_refcoidgldept`.divisionname,
  `ecw_refcoidgldept`.divisioncode,
  `ecw_refcoidgldept`.marketname,
  `ecw_refcoidgldept`.marketcode,
  `ecw_refcoidgldept`.coidname,
  `ecw_refcoidgldept`.coidwithname,
  `ecw_refcoidgldept`.centerdescription,
  `ecw_refcoidgldept`.coidlob,
  `ecw_refcoidgldept`.sublobname,
  `ecw_refcoidgldept`.coidstartdatekey,
  `ecw_refcoidgldept`.coidtermdatekey,
  `ecw_refcoidgldept`.hospitalcoid,
  `ecw_refcoidgldept`.hospitalname,
  `ecw_refcoidgldept`.providername,
  `ecw_refcoidgldept`.providerstartdatekey,
  `ecw_refcoidgldept`.providertermdatekey,
  `ecw_refcoidgldept`.specialtycategory,
  `ecw_refcoidgldept`.specialtycode,
  `ecw_refcoidgldept`.specialtyname,
  `ecw_refcoidgldept`.specialtytype,
  `ecw_refcoidgldept`.coidprogramtype,
  `ecw_refcoidgldept`.coidlargepractice,
  `ecw_refcoidgldept`.provideroriginalstartstatus,
  `ecw_refcoidgldept`.provideroriginalstartdate,
  `ecw_refcoidgldept`.appspecialtycategory,
  `ecw_refcoidgldept`.dwlastupdatedatetime,
  `ecw_refcoidgldept`.sourcesystemcode,
  `ecw_refcoidgldept`.insertedby,
  `ecw_refcoidgldept`.inserteddtm,
  `ecw_refcoidgldept`.modifiedby,
  `ecw_refcoidgldept`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refcoidgldept`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_refcoidgldept`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;