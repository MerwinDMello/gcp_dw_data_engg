CREATE OR REPLACE VIEW edwpsc_views.`ecw_factencountermedaxionworkqueue`
AS SELECT
  `ecw_factencountermedaxionworkqueue`.dateofservice,
  `ecw_factencountermedaxionworkqueue`.dayssincedateofservice,
  `ecw_factencountermedaxionworkqueue`.currentqueueentrydate,
  `ecw_factencountermedaxionworkqueue`.daysincurrentqueue,
  `ecw_factencountermedaxionworkqueue`.casestatus,
  `ecw_factencountermedaxionworkqueue`.casenumberfin,
  `ecw_factencountermedaxionworkqueue`.patientnumbermrn,
  `ecw_factencountermedaxionworkqueue`.patientfirst3,
  `ecw_factencountermedaxionworkqueue`.patientlast3,
  `ecw_factencountermedaxionworkqueue`.patientdob,
  `ecw_factencountermedaxionworkqueue`.providername,
  `ecw_factencountermedaxionworkqueue`.supervisorname,
  `ecw_factencountermedaxionworkqueue`.payer,
  `ecw_factencountermedaxionworkqueue`.location,
  `ecw_factencountermedaxionworkqueue`.workqueuecategory,
  `ecw_factencountermedaxionworkqueue`.filename,
  `ecw_factencountermedaxionworkqueue`.filedate,
  `ecw_factencountermedaxionworkqueue`.coid,
  `ecw_factencountermedaxionworkqueue`.dwlastupdatedatetime,
  `ecw_factencountermedaxionworkqueue`.insertedby,
  `ecw_factencountermedaxionworkqueue`.inserteddtm,
  `ecw_factencountermedaxionworkqueue`.modifiedby,
  `ecw_factencountermedaxionworkqueue`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_factencountermedaxionworkqueue`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factencountermedaxionworkqueue`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;