CREATE OR REPLACE VIEW edwpsc_views.`ecw_refcptcode`
AS SELECT
  `ecw_refcptcode`.cptcodekey,
  `ecw_refcptcode`.cptcode,
  `ecw_refcptcode`.cptname,
  `ecw_refcptcode`.sourceaprimarykeyvalue,
  `ecw_refcptcode`.sourcearecordlastupdated,
  `ecw_refcptcode`.sourcebprimarykeyvalue,
  `ecw_refcptcode`.sourcebrecordlastupdated,
  `ecw_refcptcode`.dwlastupdatedatetime,
  `ecw_refcptcode`.sourcesystemcode,
  `ecw_refcptcode`.insertedby,
  `ecw_refcptcode`.inserteddtm,
  `ecw_refcptcode`.modifiedby,
  `ecw_refcptcode`.modifieddtm,
  `ecw_refcptcode`.deleteflag,
  `ecw_refcptcode`.category1code,
  `ecw_refcptcode`.category2code,
  `ecw_refcptcode`.cpttier1description,
  `ecw_refcptcode`.cpttier2description,
  `ecw_refcptcode`.cpttier3description,
  `ecw_refcptcode`.cpttier4description,
  `ecw_refcptcode`.cpttier5description
  FROM
    edwpsc_base_views.`ecw_refcptcode`
;