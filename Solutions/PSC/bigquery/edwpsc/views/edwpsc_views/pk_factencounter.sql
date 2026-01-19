CREATE OR REPLACE VIEW edwpsc_views.`pk_factencounter`
AS SELECT
  `pk_factencounter`.pkencounterkey,
  `pk_factencounter`.pkregionname,
  `pk_factencounter`.patientlastname,
  `pk_factencounter`.patientfirstname,
  `pk_factencounter`.patientmiddlename,
  `pk_factencounter`.patientmrn,
  `pk_factencounter`.source,
  `pk_factencounter`.visitlocationunit,
  `pk_factencounter`.visitlocationroom,
  `pk_factencounter`.visitlocationfacility,
  `pk_factencounter`.pkfinancialnumber,
  `pk_factencounter`.appointmentadmitdate,
  `pk_factencounter`.dischargedate,
  `pk_factencounter`.createddate,
  `pk_factencounter`.deleteflag,
  `pk_factencounter`.status,
  `pk_factencounter`.sourceaprimarykeyvalue,
  `pk_factencounter`.dwlastupdatedatetime,
  `pk_factencounter`.sourcesystemcode,
  `pk_factencounter`.insertedby,
  `pk_factencounter`.inserteddtm,
  `pk_factencounter`.modifiedby,
  `pk_factencounter`.modifieddtm,
  `pk_factencounter`.encounterpos,
  `pk_factencounter`.coid,
  `pk_factencounter`.attendingproviderlastname,
  `pk_factencounter`.attendingproviderfirstname,
  `pk_factencounter`.attendingprovidernpi,
  `pk_factencounter`.erproviderlastname,
  `pk_factencounter`.erproviderfirstname,
  `pk_factencounter`.erprovidernpi,
  `pk_factencounter`.accountbasedencounterid,
  `pk_factencounter`.practiceid,
  `pk_factencounter`.postype,
  `pk_factencounter`.primaryinsurancename,
  `pk_factencounter`.secondaryinsurancename
  FROM
    edwpsc_base_views.`pk_factencounter`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pk_factencounter`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;