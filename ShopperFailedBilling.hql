use hdp_oc_team;

with
brh as
(
select *
from godaddybilling.gdshop_billingresourcehistory_snap
where to_date(date_entered) >= date_sub(to_date(from_unixtime(unix_timestamp())), 30)
),

cpl as
(
select *
from godaddycpl.gdshop_common_purchase_log_snap
where to_date(date_entered) >= date_sub(to_date(from_unixtime(unix_timestamp())), 30)
)

insert overwrite table hdp_oc_team.do_ShopperFailedBilling 
select distinct brh.gdshop_billingresourcehistoryid as gdsBillResrcHistId, brh.shopper_id, brh.date_entered, brh.order_id, brh.row_id, brh. resource_id, brh.gdshop_billing_attemptid, brh.adjusted_price, brh.gdshop_product_typeid, cpl.cpl_id, cpl.billing_attempt, cpl.response_code, cpl.reason_code, cpl.source, cpl.billing_country_code, cpl.total, cpl.attempts
from brh
left outer join
cpl on brh.order_id = cpl.order_id;