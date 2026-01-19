CREATE OR REPLACE VIEW edwpsc_views.`ecw_refproviderlawson`
AS SELECT
  `ecw_refproviderlawson`.providerlawsonkey,
  `ecw_refproviderlawson`.providerlawsonemployeenumber,
  `ecw_refproviderlawson`.providerlawsonlastname,
  `ecw_refproviderlawson`.providerlawsonfirstname,
  `ecw_refproviderlawson`.providerlawsonmiddlename,
  `ecw_refproviderlawson`.providerlawsonempstatus,
  `ecw_refproviderlawson`.providerlawsondepartment,
  `ecw_refproviderlawson`.providerlawsonjobcode,
  `ecw_refproviderlawson`.providerlawsondescription,
  `ecw_refproviderlawson`.providerlawsonannivershired,
  `ecw_refproviderlawson`.providerlawsonnewhiredate,
  `ecw_refproviderlawson`.providerlawsonprmcertid,
  `ecw_refproviderlawson`.coid,
  `ecw_refproviderlawson`.providerlawsonuserid,
  `ecw_refproviderlawson`.providerlawsonterminationdate,
  `ecw_refproviderlawson`.sourceprimarykeyvalue,
  `ecw_refproviderlawson`.sourcearecordlastupdated,
  `ecw_refproviderlawson`.sourcebrecordlastupdated,
  `ecw_refproviderlawson`.dwlastupdatedatetime,
  `ecw_refproviderlawson`.sourcesystemcode,
  `ecw_refproviderlawson`.insertedby,
  `ecw_refproviderlawson`.inserteddtm,
  `ecw_refproviderlawson`.modifiedby,
  `ecw_refproviderlawson`.modifieddtm,
  `ecw_refproviderlawson`.deleteflag
  FROM
    edwpsc_base_views.`ecw_refproviderlawson`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_refproviderlawson`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;