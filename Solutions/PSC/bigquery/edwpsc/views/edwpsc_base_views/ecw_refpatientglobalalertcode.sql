CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refpatientglobalalertcode`
AS SELECT
  `ecw_refpatientglobalalertcode`.patientglobalalertcodekey,
  `ecw_refpatientglobalalertcode`.patientglobalalertcodedesc,
  `ecw_refpatientglobalalertcode`.sourceprimarykeyvalue,
  `ecw_refpatientglobalalertcode`.sourcerecordlastupdated,
  `ecw_refpatientglobalalertcode`.dwlastupdatedatetime,
  `ecw_refpatientglobalalertcode`.sourcesystemcode,
  `ecw_refpatientglobalalertcode`.insertedby,
  `ecw_refpatientglobalalertcode`.inserteddtm,
  `ecw_refpatientglobalalertcode`.modifiedby,
  `ecw_refpatientglobalalertcode`.modifieddtm,
  `ecw_refpatientglobalalertcode`.deleteflag
  FROM
    edwpsc.`ecw_refpatientglobalalertcode`
;