CREATE OR REPLACE VIEW edwpsc_views.`ecw_factencountertocharge`
AS SELECT
  `ecw_factencountertocharge`.encountertochargekey,
  `ecw_factencountertocharge`.encounterregionid,
  `ecw_factencountertocharge`.encounterregionname,
  `ecw_factencountertocharge`.encountersourcesystem,
  `ecw_factencountertocharge`.sendingapplication,
  `ecw_factencountertocharge`.visitnumber,
  `ecw_factencountertocharge`.facilitymnemonic,
  `ecw_factencountertocharge`.placeofservicekey,
  `ecw_factencountertocharge`.practiceid,
  `ecw_factencountertocharge`.location,
  `ecw_factencountertocharge`.hospitalcoid,
  `ecw_factencountertocharge`.coid,
  `ecw_factencountertocharge`.sitecode,
  `ecw_factencountertocharge`.admitdatekey,
  `ecw_factencountertocharge`.admitdtm,
  `ecw_factencountertocharge`.dischargedatekey,
  `ecw_factencountertocharge`.dischargedtm,
  `ecw_factencountertocharge`.censusdatekey,
  `ecw_factencountertocharge`.patientname,
  `ecw_factencountertocharge`.patientmrn,
  `ecw_factencountertocharge`.patientage,
  `ecw_factencountertocharge`.sourceprimarykeyvalue,
  `ecw_factencountertocharge`.sourcerecordlastupdated,
  `ecw_factencountertocharge`.dwlastupdatedatetime,
  `ecw_factencountertocharge`.insertedby,
  `ecw_factencountertocharge`.inserteddtm,
  `ecw_factencountertocharge`.modifiedby,
  `ecw_factencountertocharge`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_factencountertocharge`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factencountertocharge`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;