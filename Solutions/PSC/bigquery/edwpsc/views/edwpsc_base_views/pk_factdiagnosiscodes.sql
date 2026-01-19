CREATE OR REPLACE VIEW edwpsc_base_views.`pk_factdiagnosiscodes`
AS SELECT
  `pk_factdiagnosiscodes`.pkdiagnosiscodekey,
  `pk_factdiagnosiscodes`.icd10code,
  `pk_factdiagnosiscodes`.icd10codedescription,
  `pk_factdiagnosiscodes`.icd10order,
  `pk_factdiagnosiscodes`.deleteflag,
  `pk_factdiagnosiscodes`.pkregionname,
  `pk_factdiagnosiscodes`.sourcesystemcode,
  `pk_factdiagnosiscodes`.sourceaprimarykeyvalue,
  `pk_factdiagnosiscodes`.sourcebprimarykeyvalue,
  `pk_factdiagnosiscodes`.insertedby,
  `pk_factdiagnosiscodes`.inserteddtm,
  `pk_factdiagnosiscodes`.modifiedby,
  `pk_factdiagnosiscodes`.modifieddtm,
  `pk_factdiagnosiscodes`.dwlastupdatedatetime
  FROM
    edwpsc.`pk_factdiagnosiscodes`
;