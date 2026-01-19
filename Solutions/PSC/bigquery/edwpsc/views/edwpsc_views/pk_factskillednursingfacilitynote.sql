CREATE OR REPLACE VIEW edwpsc_views.`pk_factskillednursingfacilitynote`
AS SELECT
  `pk_factskillednursingfacilitynote`.pkskillednursingfacilitynotekey,
  `pk_factskillednursingfacilitynote`.coid,
  `pk_factskillednursingfacilitynote`.pkregionname,
  `pk_factskillednursingfacilitynote`.notetypename,
  `pk_factskillednursingfacilitynote`.source,
  `pk_factskillednursingfacilitynote`.visitlocationfacility,
  `pk_factskillednursingfacilitynote`.pkfinancialnumber,
  `pk_factskillednursingfacilitynote`.pkencounterkey,
  `pk_factskillednursingfacilitynote`.pkencounterid,
  `pk_factskillednursingfacilitynote`.patientid,
  `pk_factskillednursingfacilitynote`.patientlastname,
  `pk_factskillednursingfacilitynote`.patientfirstname,
  `pk_factskillednursingfacilitynote`.patientmiddlename,
  `pk_factskillednursingfacilitynote`.patientmrn,
  `pk_factskillednursingfacilitynote`.chargetransactionid,
  `pk_factskillednursingfacilitynote`.notecreatedby,
  `pk_factskillednursingfacilitynote`.notemodifiedby,
  `pk_factskillednursingfacilitynote`.notecreateddate,
  `pk_factskillednursingfacilitynote`.notedate,
  `pk_factskillednursingfacilitynote`.signedstatus,
  `pk_factskillednursingfacilitynote`.signeddate,
  `pk_factskillednursingfacilitynote`.notelasteditdate,
  `pk_factskillednursingfacilitynote`.deleteflag,
  `pk_factskillednursingfacilitynote`.synccreatedtime,
  `pk_factskillednursingfacilitynote`.syncmodifiedtime,
  `pk_factskillednursingfacilitynote`.sourceaprimarykeyvalue,
  `pk_factskillednursingfacilitynote`.sourcesystemcode,
  `pk_factskillednursingfacilitynote`.insertedby,
  `pk_factskillednursingfacilitynote`.inserteddtm,
  `pk_factskillednursingfacilitynote`.modifiedby,
  `pk_factskillednursingfacilitynote`.modifieddtm,
  `pk_factskillednursingfacilitynote`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`pk_factskillednursingfacilitynote`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pk_factskillednursingfacilitynote`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;