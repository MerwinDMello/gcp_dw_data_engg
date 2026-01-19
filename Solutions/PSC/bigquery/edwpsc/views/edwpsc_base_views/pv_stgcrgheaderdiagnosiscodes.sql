CREATE OR REPLACE VIEW edwpsc_base_views.`pv_stgcrgheaderdiagnosiscodes`
AS SELECT
  `pv_stgcrgheaderdiagnosiscodes`.crgheaderdiagnosiscodepk,
  `pv_stgcrgheaderdiagnosiscodes`.crgheaderdiagnosiscodepk_txt,
  `pv_stgcrgheaderdiagnosiscodes`.crgheaderpk,
  `pv_stgcrgheaderdiagnosiscodes`.crgheaderpk_txt,
  `pv_stgcrgheaderdiagnosiscodes`.icd9code,
  `pv_stgcrgheaderdiagnosiscodes`.icd10code,
  `pv_stgcrgheaderdiagnosiscodes`.snomed,
  `pv_stgcrgheaderdiagnosiscodes`.priority,
  `pv_stgcrgheaderdiagnosiscodes`.createdon,
  `pv_stgcrgheaderdiagnosiscodes`.createdby,
  `pv_stgcrgheaderdiagnosiscodes`.icd9override,
  `pv_stgcrgheaderdiagnosiscodes`.regionkey,
  `pv_stgcrgheaderdiagnosiscodes`.ts,
  `pv_stgcrgheaderdiagnosiscodes`.inserteddtm,
  `pv_stgcrgheaderdiagnosiscodes`.modifieddtm,
  `pv_stgcrgheaderdiagnosiscodes`.dwlastupdatedatetime,
  `pv_stgcrgheaderdiagnosiscodes`.sourcephysicaldeleteflag,
  `pv_stgcrgheaderdiagnosiscodes`.sourcesystemcode,
  `pv_stgcrgheaderdiagnosiscodes`.insertedby,
  `pv_stgcrgheaderdiagnosiscodes`.modifiedby
  FROM
    edwpsc.`pv_stgcrgheaderdiagnosiscodes`
;