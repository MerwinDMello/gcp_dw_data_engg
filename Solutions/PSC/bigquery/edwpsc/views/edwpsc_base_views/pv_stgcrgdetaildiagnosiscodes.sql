CREATE OR REPLACE VIEW edwpsc_base_views.`pv_stgcrgdetaildiagnosiscodes`
AS SELECT
  `pv_stgcrgdetaildiagnosiscodes`.crgheader_diagnosiscodepk,
  `pv_stgcrgdetaildiagnosiscodes`.crgheader_diagnosiscodepk_txt,
  `pv_stgcrgdetaildiagnosiscodes`.crgdetailpk,
  `pv_stgcrgdetaildiagnosiscodes`.crgdetailpk_txt,
  `pv_stgcrgdetaildiagnosiscodes`.createdon,
  `pv_stgcrgdetaildiagnosiscodes`.createdby,
  `pv_stgcrgdetaildiagnosiscodes`.priority,
  `pv_stgcrgdetaildiagnosiscodes`.regionkey,
  `pv_stgcrgdetaildiagnosiscodes`.ts,
  `pv_stgcrgdetaildiagnosiscodes`.inserteddtm,
  `pv_stgcrgdetaildiagnosiscodes`.modifieddtm,
  `pv_stgcrgdetaildiagnosiscodes`.dwlastupdatedatetime,
  `pv_stgcrgdetaildiagnosiscodes`.sourcephysicaldeleteflag,
  `pv_stgcrgdetaildiagnosiscodes`.sourcesystemcode,
  `pv_stgcrgdetaildiagnosiscodes`.insertedby,
  `pv_stgcrgdetaildiagnosiscodes`.modifiedby
  FROM
    edwpsc.`pv_stgcrgdetaildiagnosiscodes`
;