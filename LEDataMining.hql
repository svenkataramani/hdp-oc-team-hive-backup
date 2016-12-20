SET hive.support.concurrency=FALSE;
SET hive.cli.print.current.db=TRUE;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.groupby.orderby.position.alias=TRUE;

use hdp_oc_team;

with
a as(
select shopper_id, count(distinct resource_id) AS products_owned, count(distinct pf_id) as owns_GD_prod_counts
from dm_ecommerce.fact_billing 
where cancel_ts IS NULL and billing_status_id in (1, 2)
group by shopper_id)

Insert Overwrite Table hdp_oc_team.ledatamining

select
i.shopper_id,
d.issuer_cn,
d.domainname,
f.first_name,
f.last_name,
f.phone1,
f.phone2,
f.emailhash,
c.activeportfoliosegmentid,
a.products_owned,
a.owns_GD_prod_counts
from bisandbox.le_domain_match AS d
join domains.domaininfo_snap AS i ON lower(trim(i.domainname))=lower(trim(d.domainname))
join domains.domaininfo_status_snap AS s ON i.Status=s.domaininfo_statusId
join fortknox.fortknox_shopper_snap AS f ON f.shopper_id = i.shopper_id   
join cust_customertracking.shopperheaderaudit_snap AS c ON c.shopper_id = i.shopper_id
join a on a.shopper_id = i.shopper_id
where recactive = TRUE and s.isactiveflag = true and lower(d.issuer_cn) like '%encrypt%';