SET hive.support.concurrency=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=5000; 

USE hdp_oc_team;

-- DROP TABLE hdp_oc_team.StgAfterPurchaseTrigger;
-- CREATE TABLE hdp_oc_team.StgAfterPurchaseTrigger ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE as
-- SELECT DISTINCT rod.shopper_id AS shopper_id, TO_DATE(rod.order_date) AS order_date
-- FROM godaddywebsitetraffic.visitpagerequestquerystring_snap vpr 
-- JOIN godaddywebsitetraffic.visitorder_snap vo ON vpr.visittracking_id = vo.visittracking_id 
-- JOIN bigreporting.rptorderdetail_snap rod ON vo.orderid = rod.order_id AND rod.pf_id in(566281, 566283, 566293)
-- WHERE (LOWER(visitpageparameter) = 'isccode' OR  LOWER(visitpageparameter) = 'isc')
-- AND LOWER(visitpagevalue) = 'gd51381b' 
-- AND rod.order_date >= date_sub(to_date(from_unixtime(unix_timestamp())), 30)
-- AND rod.order_date >= to_date('2015-01-14');


WITH shoppers AS (
SELECT DISTINCT rod.shopper_id AS shopper_id, TO_DATE(rod.order_date) AS order_date
FROM godaddywebsitetraffic.visitpagerequestquerystring_snap vpr 
JOIN godaddywebsitetraffic.visitorder_snap vo ON vpr.visittracking_id = vo.visittracking_id 
JOIN bigreporting.rptorderdetail_snap rod ON vo.orderid = rod.order_id AND rod.pf_id in(566281, 566283, 566293)
WHERE (LOWER(visitpageparameter) = 'isccode' OR  LOWER(visitpageparameter) = 'isc')
AND LOWER(visitpagevalue) = 'gd51381b' 
AND rod.order_date >= date_sub(to_date(from_unixtime(unix_timestamp())), 30)
AND rod.order_date >= to_date('2015-01-14')
AND rod.shopper_id NOT IN (
SELECT iv.shopper_id 
FROM hdp_oc_team.StgAfterPurchaseTrigger iv))
INSERT OVERWRITE TABLE hdp_oc_team.StgAfterPurchaseTrigger
SELECT s.shopper_id AS shopper_id, s.order_date AS order_date
FROM shoppers s;