CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refsrtforecastestimate`
AS SELECT
  `ecw_refsrtforecastestimate`.forecastestimatekey,
  `ecw_refsrtforecastestimate`.years,
  `ecw_refsrtforecastestimate`.months,
  `ecw_refsrtforecastestimate`.pedate,
  `ecw_refsrtforecastestimate`.itemnumber,
  `ecw_refsrtforecastestimate`.owners,
  `ecw_refsrtforecastestimate`.controlnumber,
  `ecw_refsrtforecastestimate`.valescoind,
  `ecw_refsrtforecastestimate`.originalitemnumber,
  `ecw_refsrtforecastestimate`.coid,
  `ecw_refsrtforecastestimate`.vendor,
  `ecw_refsrtforecastestimate`.notes,
  `ecw_refsrtforecastestimate`.deptartment,
  `ecw_refsrtforecastestimate`.initialcmsrtestimates,
  `ecw_refsrtforecastestimate`.finalcmsrtestimates,
  `ecw_refsrtforecastestimate`.sourceaprimarykeyvalue,
  `ecw_refsrtforecastestimate`.deleteflag,
  `ecw_refsrtforecastestimate`.dwlastupdatedatetime,
  `ecw_refsrtforecastestimate`.sourcesystemcode,
  `ecw_refsrtforecastestimate`.insertedby,
  `ecw_refsrtforecastestimate`.inserteddtm,
  `ecw_refsrtforecastestimate`.modifiedby,
  `ecw_refsrtforecastestimate`.modifieddtm
  FROM
    edwpsc.`ecw_refsrtforecastestimate`
;