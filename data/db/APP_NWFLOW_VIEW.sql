CREATE VIEW V_APP_NWFLOW
AS
select u.U_NAME, a.A_NAME, adf.U_LEVEL, cifrom.CI_NAME, ciffrom.CIF_NAME, dcfrom.DC_NAME, cirfrom.CIR_NAME, cito.CI_NAME, cifto.CIF_NAME, cirto.CIR_NAME, dcto.DC_NAME
from url u
inner join APPLICATION a on u.A_ID=a.A_ID
inner join APP_DEVICE_FLOW_MAP adf on u.U_ID=adf.U_ID
inner join CONFIGURATION_ITEM cifrom on cifrom.CI_ID=adf.CI_ID_FROM
inner join CONFIGURATION_ITEM cito on cito.CI_ID=adf.CI_ID_TO
inner join CONFIGURATION_ITEM_FUNCTION ciffrom on ciffrom.CIF_ID=cifrom.CIF_ID
inner join CONFIGURATION_ITEM_FUNCTION cifto on cifto.CIF_ID=cito.CIF_ID
inner join CONFIGURATION_ITEM_ROLE cirfrom on cirfrom.CIR_ID=cifrom.CIR_ID
inner join CONFIGURATION_ITEM_ROLE cirto on cirto.CIR_ID=cito.CIR_ID
inner join DATA_CENTER dcfrom on dcfrom.DC_ID=cifrom.DC_ID
inner join DATA_CENTER dcto on dcto.DC_ID=cito.DC_ID
