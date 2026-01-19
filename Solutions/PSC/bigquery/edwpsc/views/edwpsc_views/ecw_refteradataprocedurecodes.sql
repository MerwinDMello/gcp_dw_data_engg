CREATE OR REPLACE VIEW edwpsc_views.`ecw_refteradataprocedurecodes`
AS SELECT
  `ecw_refteradataprocedurecodes`.teradataprocedurecodekey,
  `ecw_refteradataprocedurecodes`.procedurecode,
  `ecw_refteradataprocedurecodes`.proceduretypecode,
  `ecw_refteradataprocedurecodes`.procedurecodedescription,
  `ecw_refteradataprocedurecodes`.procedureclasssid,
  `ecw_refteradataprocedurecodes`.procedureclassdescription,
  `ecw_refteradataprocedurecodes`.proceduremodalitysid,
  `ecw_refteradataprocedurecodes`.proceduremodalitydescription,
  `ecw_refteradataprocedurecodes`.procedurespecialtysid,
  `ecw_refteradataprocedurecodes`.procedurespecialtydescription,
  `ecw_refteradataprocedurecodes`.effectivefromdatekey,
  `ecw_refteradataprocedurecodes`.effectivetodatekey,
  `ecw_refteradataprocedurecodes`.sexeditindicator,
  `ecw_refteradataprocedurecodes`.sourcerecordlastupdated,
  `ecw_refteradataprocedurecodes`.dwlastupdatedatetime,
  `ecw_refteradataprocedurecodes`.sourcesystemcode,
  `ecw_refteradataprocedurecodes`.insertedby,
  `ecw_refteradataprocedurecodes`.inserteddtm,
  `ecw_refteradataprocedurecodes`.modifiedby,
  `ecw_refteradataprocedurecodes`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_refteradataprocedurecodes`
;