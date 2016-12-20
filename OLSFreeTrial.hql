use hdp_oc_team;

with
ols as(
select a.shopper_id, b.resource_id, 'Live' as ols_status
from dm_customer.ols_site a,
dm_ecommerce.fact_billing_external_resource b
where store_status_code = 'LIVE'
and site_status_code = 'ACTIVE'
and a.account_uid = b.external_resource_id
group by a.shopper_id, b.resource_id),

ft as(
select b.shopper_id, c.resource_id, b.date_entered as start_date
from bigreporting.dim_product_snap a,
GoDaddy.gdshop_receipt_virtualOrder_snap b,
dm_ecommerce.fact_billing c
where a.pf_id = b.pf_id
and pnl_version = 'Online Store'
and b.order_id = c.order_id
and b.row_id = c.row_id
group by b.shopper_id, c.resource_id, b.date_entered),

maxft as(
select shopper_id, max(start_date) as msd
from ft
group by shopper_id
)

insert overwrite table hdp_oc_team.cdl_ols_free_trl
select ft.shopper_id, ft.start_date as ols_ft_strt_dt, case when ols.ols_status is null then 0 else 1 end as ols_ft_live_flg
from
maxft
inner join
ft on maxft.shopper_id = ft.shopper_id and maxft.msd = ft.start_date
left outer join
ols on ft.shopper_id = ols.shopper_id and ft.resource_id = ols.resource_id
group by ft.shopper_id, ft.start_date, case when ols.ols_status is null then 0 else 1 end;