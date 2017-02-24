/*
USE EDW
GO 
==============================================================================
Author		: Mohammad Waheed
Create date	: 10/20/2016
Description	: To get the counts of rows which did not get inserted into the CDC tables from ASAP
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	

===================================================================================================================================================================
Usage Example:
DECLARE	@return_value int,
		@count1 int

EXEC	@return_value = [dbo].[usp_GetAsapVsCdcCounts]
		@count1 = @count1 OUTPUT

SELECT	@count1 as N'@count1'
===================================================================================================================================================================
*/
CREATE PROCEDURE [dbo].[usp_GetAsapVsCdcCounts]
(
@count1 int OUTPUT)

AS     

SET NOCOUNT ON;

Begin
--Create table EDW.dbo.AsapCountsCDC( TableName varchar(255),Id bigInt, IdName varchar(255),UpdatedDatetime Datetime)
Truncate table EDW.dbo.AsapCountsCDC

Create table #Asap( Id bigInt, UpdatedDatetime Datetime)
Create clustered index [idx_Id_UpdatedDatetime] ON #Asap (	ID ASC,	UpdatedDatetime ASC)

Create table #Cdc( Id bigInt,UpdatedDatetime Datetime)
Create clustered index [idx_Id_UpdatedDatetime] ON #Cdc (	ID ASC,	UpdatedDatetime ASC)

Create table #SEH( TableName varchar(255),Id bigInt, IdName varchar(255),UpdatedDatetime Datetime)

--BUYER
Insert into EDW.dbo.AsapCountsCDC
Select 'BI_ASAP_REP.dbo.Buyer', buyer_id,'buyer_id',Update_DateTime 
From BI_ASAP_REP.dbo.Buyer  With (nolock)
Where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE())
Except
Select 'BI_ASAP_REP.dbo.Buyer', buyer_id,'buyer_id',Update_DateTime 
from ( Select buyer_id,Update_DateTime 
		   from BI_ASAP_REP.cdc.dbo_Buyer_CT  With (nolock)
		   where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE()) 
		   group by buyer_id,Update_DateTime
		 )x 

--AUCTION_ITEM
insert into #Asap
Select  a.Salvage_id,a.Update_DateTime 
from BI_ASAP_REP.dbo.Auction_Item a With (nolock)
where a.Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE())

insert into #Cdc
select  Salvage_id,Update_DateTime 
from ( Select distinct Salvage_id,Update_DateTime 
		   from BI_ASAP_REP.cdc.dbo_Auction_Item_CT  With (nolock)
           where Update_DateTime >dateadd(dd,-1,GETDATE())  and Update_DateTime <=dateadd(hh,-1,GETDATE()) 
		   group by Salvage_id,Update_DateTime
		 )x 

insert into EDW.dbo.AsapCountsCDC
Select 'BI_ASAP_REP.dbo.Auction_Item', a.Id,'Salvage_id',a.UpdatedDatetime 
from #Asap a
where a.UpdatedDatetime >dateadd(dd,-1,GETDATE())  and a.UpdatedDatetime <=dateadd(hh,-1,GETDATE())
and Not Exists (select * 
                          from #Cdc x 
						  where x.Id=a.Id and x.UpdatedDatetime=a.UpdatedDatetime  
						 )

--SALVAGE
insert into EDW.dbo.AsapCountsCDC
Select 'BI_ASAP_REP.dbo.Salvage', Salvage_id,'Salvage_id',Update_DateTime 
from BI_ASAP_REP.dbo.Salvage  With (nolock)
where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE())
Except
select 'BI_ASAP_REP.dbo.Salvage', Salvage_id,'Salvage_id',Update_DateTime 
from ( Select Salvage_id,Update_DateTime 
		   from BI_ASAP_REP.cdc.dbo_Salvage_CT   With (nolock)
		   where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE()) 
		   group by Salvage_id,Update_DateTime
		 )x 

--SALVAGE_EVENT_HISTORY

--insert into EDW.dbo.AsapCountsCDC
--Select 'BI_ASAP_REP.dbo.Salvage_Event_History', Salvage_id,'Salvage_id',Update_DateTime 
--from BI_ASAP_REP.dbo.Salvage_Event_History  With (nolock)
--where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE())
--Except
--select 'BI_ASAP_REP.dbo.Salvage_Event_History', Salvage_id,'Salvage_id',Update_DateTime 
--from ( Select Salvage_id,Update_DateTime 
--		   from BI_ASAP_REP.cdc.dbo_Salvage_Event_History_CT  With (nolock)
--           where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE()) 
--		   group by Salvage_id,Update_DateTime
--		)x 
------
INSERT INTO #SEH
Select 'BI_ASAP_REP.dbo.Salvage_Event_History' AS TableName, Salvage_id,'Salvage_id' AS IdName,Update_DateTime 
from BI_ASAP_REP.dbo.Salvage_Event_History  With (nolock)
where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE()) --and  Salvage_id = 19423198

insert into EDW.dbo.AsapCountsCDC
Select 'BI_ASAP_REP.dbo.Salvage_Event_History', SEH.id,'Salvage_id',SEH.UpdatedDateTime 
from #SEH SEH With (nolock)
LEFT OUTER JOIN ( Select Salvage_id,Update_DateTime 
		   from BI_ASAP_REP.cdc.dbo_Salvage_Event_History_CT  With (nolock)
           where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE()) --and  Salvage_id = 19423198
		   group by Salvage_id,Update_DateTime
		)x 
ON SEH.ID = x.Salvage_ID AND SEH.UpdatedDateTime = x.Update_DateTime
WHERE X.Salvage_ID is null

--SALVAGE_INFO
insert into EDW.dbo.AsapCountsCDC
Select 'BI_ASAP_REP.dbo.Salvage_Info', Salvage_id,'Salvage_id',Assignment_DateTime 
from BI_ASAP_REP.dbo.Salvage_Info  With (nolock)
where Assignment_DateTime >dateadd(dd,-1,GETDATE()) and Assignment_DateTime <=dateadd(hh,-1,GETDATE())
Except
select 'BI_ASAP_REP.dbo.Salvage_Info', Salvage_id,'Salvage_id',Assignment_DateTime 
from ( Select Salvage_id,Assignment_DateTime 
           from BI_ASAP_REP.cdc.dbo_Salvage_Info_CT  With (nolock)
		   where Assignment_DateTime >dateadd(dd,-1,GETDATE()) and Assignment_DateTime <=dateadd(hh,-1,GETDATE()) 
		   group by Salvage_id,Assignment_DateTime
		)x 

--SALVAGE_PROVIDER_CHARGE
insert into EDW.dbo.AsapCountsCDC
Select 'BI_ASAP_REP.dbo.Salvage_Provider_Charge', Salvage_id,'Salvage_id',Update_DateTime 
from BI_ASAP_REP.dbo.Salvage_Provider_Charge  With (nolock)
where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE())
Except
select 'BI_ASAP_REP.dbo.Salvage_Provider_Charge', Salvage_id,'Salvage_id',Update_DateTime 
from(Select Salvage_id,Update_DateTime 
		 from BI_ASAP_REP.cdc.dbo_Salvage_Provider_Charge_CT   With (nolock)
         where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE()) 
		 group by Salvage_id,Update_DateTime)x 

--SALVAGE_TOW
insert into EDW.dbo.AsapCountsCDC
Select 'BI_ASAP_REP.dbo.Salvage_Tow', Salvage_id,'Salvage_id',Update_DateTime 
from BI_ASAP_REP.dbo.Salvage_Tow  With (nolock)
where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE())
Except
select 'BI_ASAP_REP.dbo.Salvage_Tow', Salvage_id,'Salvage_id',Update_DateTime 
from ( Select Salvage_id,Update_DateTime 
		   from BI_ASAP_REP.cdc.dbo_Salvage_Tow_CT   With (nolock)
           where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE()) 
           group by Salvage_id,Update_DateTime
		 )x 

--SALVAGE_PROVIDER_CHARGE
insert into EDW.dbo.AsapCountsCDC
Select 'BI_ASAP_REP.dbo.Salvage_Provider_Charge', Salvage_Provider_Charge_ID,'Salvage_Provider_Charge_ID',Update_DateTime 
from BI_ASAP_REP.dbo.Salvage_Provider_Charge  With (nolock)
where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE())
Except
select 'BI_ASAP_REP.dbo.Salvage_Provider_Charge', Salvage_Provider_Charge_ID,'Salvage_Provider_Charge_ID',Update_DateTime 
from ( Select Salvage_Provider_Charge_ID,Update_DateTime 
           from BI_ASAP_REP.cdc.dbo_Salvage_Provider_Charge_CT   With (nolock)
           where Update_DateTime >dateadd(dd,-1,GETDATE()) and Update_DateTime <=dateadd(hh,-1,GETDATE()) 
           group by Salvage_Provider_Charge_ID,Update_DateTime
         )x 

Select @count1=isnull(MAX(counts),0)  from (
select count(*) counts from EDW.dbo.AsapCountsCDC
Group by TableName )x

--Select * from EDW.dbo.AsapCountsCDC


IF OBJECT_ID('tempdb..#Asap') IS NOT NULL DROP TABLE #Asap
IF OBJECT_ID('tempdb..#Cdc') IS NOT NULL DROP TABLE #Cdc
IF OBJECT_ID('tempdb..#dbo_Salvage_Event_History_CT') IS NOT NULL DROP TABLE #dbo_Salvage_Event_History_CT
IF OBJECT_ID('tempdb..#Salvage_Event_History') IS NOT NULL DROP TABLE #Salvage_Event_History

End
