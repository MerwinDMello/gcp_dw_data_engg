CREATE OR REPLACE VIEW edwpsc_views.`ecw_refnbtallocglphysiciancurredw`
AS SELECT
  `ecw_refnbtallocglphysiciancurredw`.coid,
  `ecw_refnbtallocglphysiciancurredw`.coidname,
  `ecw_refnbtallocglphysiciancurredw`.providerdepartmentno,
  `ecw_refnbtallocglphysiciancurredw`.providerfirstname,
  `ecw_refnbtallocglphysiciancurredw`.providerlastname,
  `ecw_refnbtallocglphysiciancurredw`.practicename,
  `ecw_refnbtallocglphysiciancurredw`.overheaddepartmentno,
  `ecw_refnbtallocglphysiciancurredw`.ancillarydepartmentno,
  `ecw_refnbtallocglphysiciancurredw`.practiceestablishedid,
  `ecw_refnbtallocglphysiciancurredw`.coiddepartmentspecialtycode,
  `ecw_refnbtallocglphysiciancurredw`.provideroriginalstartdate,
  `ecw_refnbtallocglphysiciancurredw`.budgetstartdate,
  `ecw_refnbtallocglphysiciancurredw`.actiondate,
  `ecw_refnbtallocglphysiciancurredw`.ohallocation,
  `ecw_refnbtallocglphysiciancurredw`.coiddepartmentfte,
  `ecw_refnbtallocglphysiciancurredw`.coiddepartmentisbudgetcc,
  `ecw_refnbtallocglphysiciancurredw`.coiddepartmentproviderstatusid,
  `ecw_refnbtallocglphysiciancurredw`.coiddepartmentproviderstatus,
  `ecw_refnbtallocglphysiciancurredw`.terminationdate,
  `ecw_refnbtallocglphysiciancurredw`.coiddeprprovassignedstartdate,
  `ecw_refnbtallocglphysiciancurredw`.projectedterminationdate,
  `ecw_refnbtallocglphysiciancurredw`.glperiod,
  `ecw_refnbtallocglphysiciancurredw`.allocationtypedescription,
  `ecw_refnbtallocglphysiciancurredw`.allocationtypeid,
  `ecw_refnbtallocglphysiciancurredw`.compensationtypedescription,
  `ecw_refnbtallocglphysiciancurredw`.compensationtypeid,
  `ecw_refnbtallocglphysiciancurredw`.providerid,
  `ecw_refnbtallocglphysiciancurredw`.appspecialtycategoryid,
  `ecw_refnbtallocglphysiciancurredw`.insertedby,
  `ecw_refnbtallocglphysiciancurredw`.inserteddtm,
  `ecw_refnbtallocglphysiciancurredw`.modifiedby,
  `ecw_refnbtallocglphysiciancurredw`.modifieddtm,
  `ecw_refnbtallocglphysiciancurredw`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_refnbtallocglphysiciancurredw`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(CAST(`ecw_refnbtallocglphysiciancurredw`.coid AS STRING), ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;