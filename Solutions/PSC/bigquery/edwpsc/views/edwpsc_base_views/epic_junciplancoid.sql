CREATE OR REPLACE VIEW edwpsc_base_views.`epic_junciplancoid`
AS SELECT
  `epic_junciplancoid`.junciplancoidkey,
  `epic_junciplancoid`.iplankey,
  `epic_junciplancoid`.coid,
  `epic_junciplancoid`.insertedby,
  `epic_junciplancoid`.inserteddtm,
  `epic_junciplancoid`.modifiedby,
  `epic_junciplancoid`.modifieddtm,
  `epic_junciplancoid`.dwlastupdatedatetime
  FROM
    edwpsc.`epic_junciplancoid`
;