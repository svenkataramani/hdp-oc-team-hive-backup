#!/bin/sh

start_date=$(date --date="1 days ago" +"%Y-%m-%d")
echo $start_date
echo $end_date

# kinit -R -k -t /home/hdp_oc_team/kerberos/hdp_oc_team.keytab hdp_oc_team@DC1.CORP.GD

# hive -hiveconf startdate=${start_date} -hiveconf enddate=${end_date} -f  /home/hdp_oc_team/RedPoint/prod_events_rp.hql;


