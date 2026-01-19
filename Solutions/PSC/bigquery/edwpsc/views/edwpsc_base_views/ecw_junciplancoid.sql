CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_junciplancoid`
AS SELECT
  `ecw_junciplancoid`.junciplancoidkey,
  `ecw_junciplancoid`.iplankey,
  `ecw_junciplancoid`.coid,
  `ecw_junciplancoid`.insertedby,
  `ecw_junciplancoid`.inserteddtm,
  `ecw_junciplancoid`.modifiedby,
  `ecw_junciplancoid`.modifieddtm,
  `ecw_junciplancoid`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_junciplancoid`
;