CREATE OR REPLACE VIEW edwpsc_views.`ecw_factenvisiontermination`
AS SELECT
  `ecw_factenvisiontermination`.envisionterminationkey,
  `ecw_factenvisiontermination`.npi,
  `ecw_factenvisiontermination`.lastname,
  `ecw_factenvisiontermination`.firstname,
  `ecw_factenvisiontermination`.middlename,
  `ecw_factenvisiontermination`.suffix,
  `ecw_factenvisiontermination`.degree,
  `ecw_factenvisiontermination`.sitecode,
  `ecw_factenvisiontermination`.termeffectivedate,
  `ecw_factenvisiontermination`.status,
  `ecw_factenvisiontermination`.businessdayssinceloaded,
  `ecw_factenvisiontermination`.loaddate,
  `ecw_factenvisiontermination`.worked,
  `ecw_factenvisiontermination`.workeddate,
  `ecw_factenvisiontermination`.artivaworklist,
  `ecw_factenvisiontermination`.contentworklist,
  `ecw_factenvisiontermination`.sourcesystemcode,
  `ecw_factenvisiontermination`.dwlastupdatedatetime,
  `ecw_factenvisiontermination`.insertedby,
  `ecw_factenvisiontermination`.inserteddtm,
  `ecw_factenvisiontermination`.modifiedby,
  `ecw_factenvisiontermination`.modifieddtm
  FROM
    edwpsc_base_views.`ecw_factenvisiontermination`
;