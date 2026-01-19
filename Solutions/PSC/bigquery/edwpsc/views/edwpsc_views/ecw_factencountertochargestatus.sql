CREATE OR REPLACE VIEW edwpsc_views.`ecw_factencountertochargestatus`
AS SELECT
  `ecw_factencountertochargestatus`.encountertochargestatuskey,
  `ecw_factencountertochargestatus`.encountertochargekey,
  `ecw_factencountertochargestatus`.sourcesystem,
  `ecw_factencountertochargestatus`.systemstatus,
  `ecw_factencountertochargestatus`.systemstartdtm,
  `ecw_factencountertochargestatus`.systemstatusstartdtm,
  `ecw_factencountertochargestatus`.systemcoid,
  `ecw_factencountertochargestatus`.owner,
  `ecw_factencountertochargestatus`.sourceprimarykeyvalue,
  `ecw_factencountertochargestatus`.sourcerecordlastupdated,
  `ecw_factencountertochargestatus`.dwlastupdatedatetime,
  `ecw_factencountertochargestatus`.insertedby,
  `ecw_factencountertochargestatus`.inserteddtm,
  `ecw_factencountertochargestatus`.modifiedby,
  `ecw_factencountertochargestatus`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_factencountertochargestatus`
;