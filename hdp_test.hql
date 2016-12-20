
SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;

Insert Overwrite Table hdp_oc_team.hdp_test
Select
domain_id,
domain_name,
parent_resource_id,
unique_id,
gdshop_product_typeid,
add_on_resource_id,
cast(order_date as string) as order_date,
cast(cancel_date as string) as cancel_date,
order_id,
row_id,
bundle_flag 
From bisandbox.domain_parent_resource
limit 50
;