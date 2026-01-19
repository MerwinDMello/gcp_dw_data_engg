CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factencountermedaxioncodingcptcodes`
AS SELECT
  `ecw_factencountermedaxioncodingcptcodes`.medaxioncodingcptcodeskey,
  `ecw_factencountermedaxioncodingcptcodes`.medaxioncodingstatuskey,
  `ecw_factencountermedaxioncodingcptcodes`.cptcode,
  `ecw_factencountermedaxioncodingcptcodes`.modifiercodes,
  `ecw_factencountermedaxioncodingcptcodes`.diagnosiscodes,
  `ecw_factencountermedaxioncodingcptcodes`.cptprovidername,
  `ecw_factencountermedaxioncodingcptcodes`.cptcount,
  `ecw_factencountermedaxioncodingcptcodes`.deleteflag,
  `ecw_factencountermedaxioncodingcptcodes`.dwlastupdatedatetime,
  `ecw_factencountermedaxioncodingcptcodes`.sourcesystemcode,
  `ecw_factencountermedaxioncodingcptcodes`.insertedby,
  `ecw_factencountermedaxioncodingcptcodes`.inserteddtm,
  `ecw_factencountermedaxioncodingcptcodes`.modifiedby,
  `ecw_factencountermedaxioncodingcptcodes`.modifieddtm
  FROM
    edwpsc.`ecw_factencountermedaxioncodingcptcodes`
;