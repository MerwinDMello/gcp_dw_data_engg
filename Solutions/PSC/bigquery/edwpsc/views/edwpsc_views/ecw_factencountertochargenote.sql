CREATE OR REPLACE VIEW edwpsc_views.`ecw_factencountertochargenote`
AS SELECT
  `ecw_factencountertochargenote`.noteencountertochargekey,
  `ecw_factencountertochargenote`.encountertochargekey,
  `ecw_factencountertochargenote`.notename,
  `ecw_factencountertochargenote`.notestatus,
  `ecw_factencountertochargenote`.notepriority,
  `ecw_factencountertochargenote`.noteclosetomidnight,
  `ecw_factencountertochargenote`.notecreatedtm,
  `ecw_factencountertochargenote`.noteupdatedtm,
  `ecw_factencountertochargenote`.noteprovidersignednpi,
  `ecw_factencountertochargenote`.notecosignprovidernpi,
  `ecw_factencountertochargenote`.noteservicedatekey,
  `ecw_factencountertochargenote`.sourceprimarykeyvalue,
  `ecw_factencountertochargenote`.dwlastupdatedatetime,
  `ecw_factencountertochargenote`.insertedby,
  `ecw_factencountertochargenote`.inserteddtm,
  `ecw_factencountertochargenote`.modifiedby,
  `ecw_factencountertochargenote`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_factencountertochargenote`
;