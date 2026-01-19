CREATE OR REPLACE VIEW edwpsc_views.`epic_factencounteranesthesianote`
AS SELECT
  `epic_factencounteranesthesianote`.anesthesianotekey,
  `epic_factencounteranesthesianote`.anesthesiakey,
  `epic_factencounteranesthesianote`.regionkey,
  `epic_factencounteranesthesianote`.coid,
  `epic_factencounteranesthesianote`.notetypecode,
  `epic_factencounteranesthesianote`.notetypeabbreviation,
  `epic_factencounteranesthesianote`.notetypename,
  `epic_factencounteranesthesianote`.notereason,
  `epic_factencounteranesthesianote`.noteproviderid,
  `epic_factencounteranesthesianote`.noteproviderkey,
  `epic_factencounteranesthesianote`.noteprovidername,
  `epic_factencounteranesthesianote`.noteuserid,
  `epic_factencounteranesthesianote`.noteuserkey,
  `epic_factencounteranesthesianote`.noteusername,
  `epic_factencounteranesthesianote`.patientid,
  `epic_factencounteranesthesianote`.patientkey,
  `epic_factencounteranesthesianote`.notecreatedatetime,
  `epic_factencounteranesthesianote`.noteupdatedatetime,
  `epic_factencounteranesthesianote`.notesignedflag,
  `epic_factencounteranesthesianote`.notesigneddatetime,
  `epic_factencounteranesthesianote`.encounterid,
  `epic_factencounteranesthesianote`.encounterkey,
  `epic_factencounteranesthesianote`.noteservicedatekey,
  `epic_factencounteranesthesianote`.notewritersavedflag,
  `epic_factencounteranesthesianote`.archivednoteflag,
  `epic_factencounteranesthesianote`.notedeleteflag,
  `epic_factencounteranesthesianote`.notedeletedatetime,
  `epic_factencounteranesthesianote`.notedeleteuserid,
  `epic_factencounteranesthesianote`.notedeleteuserkey,
  `epic_factencounteranesthesianote`.notedeleteusername,
  `epic_factencounteranesthesianote`.sourceaprimarykeyvalue,
  `epic_factencounteranesthesianote`.sourcesystemcode,
  `epic_factencounteranesthesianote`.dwlastupdatedatetime,
  `epic_factencounteranesthesianote`.insertedby,
  `epic_factencounteranesthesianote`.inserteddtm,
  `epic_factencounteranesthesianote`.modifiedby,
  `epic_factencounteranesthesianote`.modifieddtm
  FROM
    edwpsc_base_views.`epic_factencounteranesthesianote`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factencounteranesthesianote`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;