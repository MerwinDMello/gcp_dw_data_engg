CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_reffinancialpayorgroupsize`
AS SELECT
  `ecw_reffinancialpayorgroupsize`.ranknumber,
  `ecw_reffinancialpayorgroupsize`.financialpayorgroupname,
  `ecw_reffinancialpayorgroupsize`.financialpayorgroupsize,
  `ecw_reffinancialpayorgroupsize`.charges13month,
  `ecw_reffinancialpayorgroupsize`.insertedby,
  `ecw_reffinancialpayorgroupsize`.inserteddtm,
  `ecw_reffinancialpayorgroupsize`.modifiedby,
  `ecw_reffinancialpayorgroupsize`.modifieddtm,
  `ecw_reffinancialpayorgroupsize`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_reffinancialpayorgroupsize`
;