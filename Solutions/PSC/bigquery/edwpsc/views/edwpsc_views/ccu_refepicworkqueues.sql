CREATE OR REPLACE VIEW edwpsc_views.`ccu_refepicworkqueues`
AS SELECT
  `ccu_refepicworkqueues`.workqueuekey,
  `ccu_refepicworkqueues`.workqueueid,
  `ccu_refepicworkqueues`.ccuqueueflag,
  `ccu_refepicworkqueues`.practicequeueflag,
  `ccu_refepicworkqueues`.accountresolutionqueueflag,
  `ccu_refepicworkqueues`.vendorqueueflag,
  `ccu_refepicworkqueues`.ccuvendorqueueflag,
  `ccu_refepicworkqueues`.active,
  `ccu_refepicworkqueues`.dwlastupdatedatetime,
  `ccu_refepicworkqueues`.insertedby,
  `ccu_refepicworkqueues`.inserteddtm,
  `ccu_refepicworkqueues`.modifiedby,
  `ccu_refepicworkqueues`.modifieddtm,
  `ccu_refepicworkqueues`.regionkey
  FROM
    edwpsc_base_views.`ccu_refepicworkqueues`
;