use hdp_oc_team;

--create table hdp_oc_team.ShopAcctCntctEml ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE as
--select
--shopperid,
--cm.customermap_id,
--email_id,
--SUBSTRING(sentdate,0,10) AS sentdate,
--CONCAT(SUBSTRING(sentdate,0,19),'.000') AS sentdatetime,
--privatelabel_id,
--isccode,
--emailsource_id,
--emailsourcekey,
--template_id,
--message_id,
--batch_id,
--errorcode_id,
--emailformat,
--lang,
--ce.auditcreatedatetime AS auditcreatedatetime
--from cust_contact_txlog.contact_email ce,
--cust_contact.customermap_snap cm
--where ce.customermap_id = cm.customermap_id
--and cm.shopperid rlike '^[0-9]+$' = TRUE;

with
ec as(
select
shopperid,
cm.customermap_id,
email_id,
SUBSTRING(sentdate,0,10) AS sentdate,
CONCAT(SUBSTRING(sentdate,0,19),'.000') AS sentdatetime,
privatelabel_id,
isccode,
emailsource_id,
emailsourcekey,
template_id,
message_id,
batch_id,
errorcode_id,
emailformat,
lang,
ce.auditcreatedatetime AS auditcreatedatetime
from cust_contact.contact_email_snap ce,
cust_contact.customermap_snap cm
where ce.customermap_id = cm.customermap_id
and to_date(sentdate) >= date_sub(to_date(from_unixtime(unix_timestamp())), 365)
and cm.shopperid rlike '^[0-9]+$' = TRUE)

insert overwrite table hdp_oc_team.ShopAcctCntctEml
select distinct shopperid,
customermap_id,
email_id,
sentdate,
sentdatetime,
privatelabel_id,
isccode,
emailsource_id,
emailsourcekey,
template_id,
message_id,
batch_id,
errorcode_id,
emailformat,
lang,
auditcreatedatetime
from ec;