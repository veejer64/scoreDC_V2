CREATE VIEW V_ALL_NWFLOW_DEVICES_EXTENDED
AS
select u.U_NAME, a.A_NAME, t.TA_NAME, ci.CI_NAME, cif.CIF_NAME, cif.CIF_HASH, cir.CIR_NAME, cir.CIR_HASH, dc.DC_NAME 
from url u 
inner join APPLICATION a on u.A_ID=a.A_ID 
inner join TECH_AREA t on a.ta_id=t.TA_ID
inner join APP_DEVICE_MAP adm on u.U_ID=adm.U_ID
inner join CONFIGURATION_ITEM ci on ci.ci_id=adm.CI_ID
inner join CONFIGURATION_ITEM_FUNCTION cif on cif.CIF_ID=ci.CIF_ID
inner join CONFIGURATION_ITEM_ROLE cir on cir.CIR_ID=ci.CIR_ID
inner join DATA_CENTER dc on dc.DC_ID=ci.DC_ID