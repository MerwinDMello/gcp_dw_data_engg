CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factenvisiondemographicchange`
AS SELECT
  `ecw_factenvisiondemographicchange`.envisiondemographicchangekey,
  `ecw_factenvisiondemographicchange`.npi,
  `ecw_factenvisiondemographicchange`.lastname,
  `ecw_factenvisiondemographicchange`.firstname,
  `ecw_factenvisiondemographicchange`.middlename,
  `ecw_factenvisiondemographicchange`.suffix,
  `ecw_factenvisiondemographicchange`.degree,
  `ecw_factenvisiondemographicchange`.sitecode,
  `ecw_factenvisiondemographicchange`.typeofchange,
  `ecw_factenvisiondemographicchange`.changedata,
  `ecw_factenvisiondemographicchange`.changeeffectivedate,
  `ecw_factenvisiondemographicchange`.businessdayssinceloaded,
  `ecw_factenvisiondemographicchange`.loaddate,
  `ecw_factenvisiondemographicchange`.worked,
  `ecw_factenvisiondemographicchange`.workeddate,
  `ecw_factenvisiondemographicchange`.artivaworklist,
  `ecw_factenvisiondemographicchange`.contentworklist,
  `ecw_factenvisiondemographicchange`.sourcesystemcode,
  `ecw_factenvisiondemographicchange`.dwlastupdatedatetime,
  `ecw_factenvisiondemographicchange`.insertedby,
  `ecw_factenvisiondemographicchange`.inserteddtm,
  `ecw_factenvisiondemographicchange`.modifiedby,
  `ecw_factenvisiondemographicchange`.modifieddtm
  FROM
    edwpsc.`ecw_factenvisiondemographicchange`
;