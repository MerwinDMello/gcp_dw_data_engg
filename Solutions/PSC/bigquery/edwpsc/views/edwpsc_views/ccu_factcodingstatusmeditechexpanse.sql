CREATE OR REPLACE VIEW edwpsc_views.`ccu_factcodingstatusmeditechexpanse`
AS SELECT
  `ccu_factcodingstatusmeditechexpanse`.ccucodingstatuskey,
  `ccu_factcodingstatusmeditechexpanse`.regionkey,
  `ccu_factcodingstatusmeditechexpanse`.worklistuserkey,
  `ccu_factcodingstatusmeditechexpanse`.worklistdatetime,
  `ccu_factcodingstatusmeditechexpanse`.workliststatus,
  `ccu_factcodingstatusmeditechexpanse`.codingstatus,
  `ccu_factcodingstatusmeditechexpanse`.codingstatusdate,
  `ccu_factcodingstatusmeditechexpanse`.codingstatususerkey,
  `ccu_factcodingstatusmeditechexpanse`.sourceaprimarykeyvalue,
  `ccu_factcodingstatusmeditechexpanse`.sourcearecordlastupdated,
  `ccu_factcodingstatusmeditechexpanse`.sourcebprimarykeyvalue,
  `ccu_factcodingstatusmeditechexpanse`.sourcebrecordlastupdated,
  `ccu_factcodingstatusmeditechexpanse`.dwlastupdatedatetime,
  `ccu_factcodingstatusmeditechexpanse`.sourcesystemcode,
  `ccu_factcodingstatusmeditechexpanse`.insertedby,
  `ccu_factcodingstatusmeditechexpanse`.inserteddtm,
  `ccu_factcodingstatusmeditechexpanse`.modifiedby,
  `ccu_factcodingstatusmeditechexpanse`.modifieddtm
  FROM
    edwpsc_base_views.`ccu_factcodingstatusmeditechexpanse`
;