CREATE OR REPLACE VIEW edwpsc_views.`ecw_refpatientglobalalert`
AS SELECT
  `ecw_refpatientglobalalert`.patientglobalalertkey,
  `ecw_refpatientglobalalert`.patientkey,
  `ecw_refpatientglobalalert`.patientglobalalertcodekey,
  `ecw_refpatientglobalalert`.patientglobalalertnote,
  `ecw_refpatientglobalalert`.patientglobalalertpriority,
  `ecw_refpatientglobalalert`.sourceprimarykeyvalue,
  `ecw_refpatientglobalalert`.sourcerecordlastupdated,
  `ecw_refpatientglobalalert`.dwlastupdatedatetime,
  `ecw_refpatientglobalalert`.sourcesystemcode,
  `ecw_refpatientglobalalert`.insertedby,
  `ecw_refpatientglobalalert`.inserteddtm,
  `ecw_refpatientglobalalert`.modifiedby,
  `ecw_refpatientglobalalert`.modifieddtm,
  `ecw_refpatientglobalalert`.deleteflag,
  `ecw_refpatientglobalalert`.regionkey
  FROM
    edwpsc_base_views.`ecw_refpatientglobalalert`
;