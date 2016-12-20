use hdp_oc_team;

--create table hdp_oc_team.dialer_detail ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE as
--select *
--from cisco_call.t_dialer_detail_snap
--where to_date(dbdatetime) >= date_sub(to_date(from_unixtime(unix_timestamp())), 60);

with
dd as(
select distinct recoverykey, datetime, timezone, customertimezone, dbdatetime, campaignid, callresult, callresultdetail, callstatuszone1, callstatuszone2, queryruleid, dialinglistid, phone, phoneext, skillgroupskilltargetid, phoneindex, phoneid, zoneindex, agentperipheralnumber, peripheralid, peripheralcallkey, callduration, accountnumber, firstname, lastname, callbackphone, callbackdatetime, dialingmode, dialerid, portnumber, importruledatetime, internaluse1, internaluse2, internaluse3, internaluse4, internaluse5, internaluse6, internaluse7, internaluse8, internaluse9, internaluse10, internaluse11, futureuseint1, futureuseint2, futureuseint3, futureuseint4, futureuseint5, futureuseint6, futureuseint7, futureuseint8, futureusevarchar1, futureusevarchar2, futureusevarchar3, futureusevarchar4, routercallkey, callguid, routercallkeyday, wrapupdata, pickuptime, maxactiveglitchtime, numofactiveglitches, validspeechtime, maxpostspeechsilenceglitchtime, numofpostspeechsilenceglitches, silenceperiod, termtonedetectiontime, maxzcrstdev, noisethreshold, activethreshold, reservationcallduration, previewtime, campaignreportingdatetime, protocolid
from cisco_call.t_dialer_detail_snap
where to_date(dbdatetime) >= date_sub(to_date(from_unixtime(unix_timestamp())), 60))

insert overwrite table hdp_oc_team.dialer_detail
select recoverykey, datetime, timezone, customertimezone, dbdatetime, campaignid, callresult, callresultdetail, callstatuszone1, callstatuszone2, queryruleid, dialinglistid, phone, phoneext, skillgroupskilltargetid, phoneindex, phoneid, zoneindex, agentperipheralnumber, peripheralid, peripheralcallkey, callduration, accountnumber, firstname, lastname, callbackphone, callbackdatetime, dialingmode, dialerid, portnumber, importruledatetime, internaluse1, internaluse2, internaluse3, internaluse4, internaluse5, internaluse6, internaluse7, internaluse8, internaluse9, internaluse10, internaluse11, futureuseint1, futureuseint2, futureuseint3, futureuseint4, futureuseint5, futureuseint6, futureuseint7, futureuseint8, futureusevarchar1, futureusevarchar2, futureusevarchar3, futureusevarchar4, routercallkey, callguid, routercallkeyday, wrapupdata, pickuptime, maxactiveglitchtime, numofactiveglitches, validspeechtime, maxpostspeechsilenceglitchtime, numofpostspeechsilenceglitches, silenceperiod, termtonedetectiontime, maxzcrstdev, noisethreshold, activethreshold, reservationcallduration, previewtime, campaignreportingdatetime, protocolid
from dd;