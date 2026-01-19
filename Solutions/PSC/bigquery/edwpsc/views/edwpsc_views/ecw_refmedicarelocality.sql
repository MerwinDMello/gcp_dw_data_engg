CREATE OR REPLACE VIEW edwpsc_views.`ecw_refmedicarelocality`
AS SELECT
  `ecw_refmedicarelocality`.medicarelocalitykey,
  `ecw_refmedicarelocality`.state,
  `ecw_refmedicarelocality`.zipcode,
  `ecw_refmedicarelocality`.carrier,
  `ecw_refmedicarelocality`.locality,
  `ecw_refmedicarelocality`.ruralind,
  `ecw_refmedicarelocality`.labcblocality,
  `ecw_refmedicarelocality`.ruralind2,
  `ecw_refmedicarelocality`.plus4flag,
  `ecw_refmedicarelocality`.partbdrugindicator,
  `ecw_refmedicarelocality`.yearqtr,
  `ecw_refmedicarelocality`.dwlastupdatedatetime,
  `ecw_refmedicarelocality`.sourcesystemcode,
  `ecw_refmedicarelocality`.insertedby,
  `ecw_refmedicarelocality`.inserteddtm,
  `ecw_refmedicarelocality`.modifiedby,
  `ecw_refmedicarelocality`.modifieddtm,
  `ecw_refmedicarelocality`.medicarelocality
  FROM
    edwpsc_base_views.`ecw_refmedicarelocality`
;