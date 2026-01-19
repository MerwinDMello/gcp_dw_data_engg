CREATE OR REPLACE VIEW edwpsc_views.`ecw_factmeditechorauth`
AS SELECT
  `ecw_factmeditechorauth`.meditechorauthkey,
  `ecw_factmeditechorauth`.meditechcocid,
  `ecw_factmeditechorauth`.accountnumber,
  `ecw_factmeditechorauth`.proceduredate,
  `ecw_factmeditechorauth`.proceduretime,
  `ecw_factmeditechorauth`.cptcode,
  `ecw_factmeditechorauth`.patientlastname,
  `ecw_factmeditechorauth`.patientfirstname,
  `ecw_factmeditechorauth`.patientmiddlename,
  `ecw_factmeditechorauth`.patientsex,
  `ecw_factmeditechorauth`.patientdob,
  `ecw_factmeditechorauth`.patienttypecode,
  `ecw_factmeditechorauth`.surgeonnpi,
  `ecw_factmeditechorauth`.surgeonname,
  `ecw_factmeditechorauth`.primaryinsurance,
  `ecw_factmeditechorauth`.primaryinsauthnumber,
  `ecw_factmeditechorauth`.secondarinsauthnumber,
  `ecw_factmeditechorauth`.financialclass,
  `ecw_factmeditechorauth`.financialclassdesc,
  `ecw_factmeditechorauth`.procedureesttime,
  `ecw_factmeditechorauth`.appointmentortype,
  `ecw_factmeditechorauth`.ptlocation,
  `ecw_factmeditechorauth`.appointmentstatus,
  `ecw_factmeditechorauth`.dwlastupdatedatetime,
  `ecw_factmeditechorauth`.sourcesystemcode,
  `ecw_factmeditechorauth`.sourceaprimarykeyvalue,
  `ecw_factmeditechorauth`.insertedby,
  `ecw_factmeditechorauth`.inserteddtm,
  `ecw_factmeditechorauth`.modifiedby,
  `ecw_factmeditechorauth`.modifieddtm,
  `ecw_factmeditechorauth`.admitorderresponsetext,
  `ecw_factmeditechorauth`.coid,
  `ecw_factmeditechorauth`.facilityname
  FROM
    edwpsc_base_views.`ecw_factmeditechorauth`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factmeditechorauth`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;