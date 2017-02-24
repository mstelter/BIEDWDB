

/* 
USE EDW
GO 
==============================================================================
Author		: Mohammad Waheed
Create date	: 10/27/2016 
Description	: Populate the EDW DimBranch table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer			

==============================================================================
Usage Example:
DECLARE	@return_value int,
		@CountPercentage int

EXEC	@return_value = [dbo].[usp_GetEtlTablesStatistics]
		@CountPercentage = @CountPercentage OUTPUT

SELECT	@CountPercentage as N'@CountPercentage'
==============================================================================
*/
Create PROCEDURE [dbo].[usp_GetEtlTablesStatistics]
(@CountPercentage  int output )

AS     
BEGIN

--if OBJECT_ID('Tempdb.dbo.#EdwDailyLoadStatistics') is not null
--Drop table #EdwDailyLoadStatistics

--Create table Edw.dbo.EdwDailyLoadStatistics  (Name varchar(50),Value varchar(50),WeekDayName varchar(20),pw_Date date, pw_Counts int, cw_Date date, cw_Counts int, CountPercentage int)
Truncate table Edw.dbo.EdwDailyLoadStatistics 


;With cw_ISStockSold as(
Select  datename(weekday,SoldDateTime) WeekDayName, DS.ISStockSold,Convert(date,SoldDateTime) cw_SoldDateTime, Count(*) cw_Counts 
from Edw.[dbo].[DimStock] ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where Convert(date,SoldDateTime)  = Convert(date,DateAdd(dd,-1,GETDATE()))
Group by DS.ISStockSold,Convert(date,SoldDateTime),datename(weekday,SoldDateTime)

)

, pw_ISStockSold as(
Select  datename(weekday,SoldDateTime) WeekDayName, DS.ISStockSold,Convert(date,SoldDateTime) pw_SoldDateTime, Count(*) pw_Counts 
from Edw.[dbo].[DimStock] ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where Convert(date,SoldDateTime)  = Convert(date,DateAdd(dd,-8,GETDATE())) 
Group by DS.ISStockSold,Convert(date,SoldDateTime),datename(weekday,SoldDateTime)
)

insert into Edw.dbo.EdwDailyLoadStatistics 
(           Name,                            Value,           WeekDayName, pw_Date,                pw_Counts, cw_Date,                cw_Counts, CountPercentage)
Select 'Sold Count by Sold Ind' ,a.ISStockSold,a.WeekDayName, pw_SoldDateTime, pw_Counts, cw_SoldDateTime, cw_Counts, pw_Counts/Case when cw_Counts=0 then 1 else cw_Counts end
From cw_ISStockSold a
		inner join pw_ISStockSold b on a.WeekDayName=b.WeekDayName and a.ISStockSold=b.ISStockSold


;With cw_IsIBFSold as(
Select  datename(weekday,SoldDateTime) WeekDayName, DS.IsIBFSold,Convert(date,SoldDateTime) cw_SoldDateTime, Count(*) cw_Counts 
from Edw.[dbo].[DimStock] ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where Convert(date,SoldDateTime)  = Convert(date,DateAdd(dd,-1,GETDATE()))
Group by DS.IsIBFSold,Convert(date,SoldDateTime),datename(weekday,SoldDateTime)
)

, pw_IsIBFSold as(
Select  datename(weekday,SoldDateTime) WeekDayName, DS.IsIBFSold,Convert(date,SoldDateTime) pw_SoldDateTime, Count(*) pw_Counts 
from Edw.[dbo].[DimStock] ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where Convert(date,SoldDateTime)  = Convert(date,DateAdd(dd,-8,GETDATE())) 
Group by DS.IsIBFSold,Convert(date,SoldDateTime),datename(weekday,SoldDateTime)
)

insert into Edw.dbo.EdwDailyLoadStatistics 
(           Name,                            Value,           WeekDayName, pw_Date,                pw_Counts, cw_Date,                cw_Counts, CountPercentage)
Select 'Count of I Buy Fast Sold' ,a.IsIBFSold,a.WeekDayName, pw_SoldDateTime, pw_Counts, cw_SoldDateTime, cw_Counts, pw_Counts/Case when cw_Counts=0 then 1 else cw_Counts end
From cw_IsIBFSold a
		inner join pw_IsIBFSold b on a.WeekDayName=b.WeekDayName and a.IsIBFSold=b.IsIBFSold



;With cw_IsIBIDWon as(
Select  datename(weekday,SoldDateTime) WeekDayName, DS.IsIBIDWon,Convert(date,SoldDateTime) cw_SoldDateTime, Count(*) cw_Counts 
from Edw.[dbo].[DimStock] ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where Convert(date,SoldDateTime)  = Convert(date,DateAdd(dd,-1,GETDATE()))
Group by DS.IsIBIDWon,Convert(date,SoldDateTime),datename(weekday,SoldDateTime)
)

, pw_IsIBIDWon as(
Select  datename(weekday,SoldDateTime) WeekDayName, DS.IsIBIDWon,Convert(date,SoldDateTime) pw_SoldDateTime, Count(*) pw_Counts 
from Edw.[dbo].[DimStock] ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where Convert(date,SoldDateTime)  = Convert(date,DateAdd(dd,-8,GETDATE())) 
Group by DS.IsIBIDWon,Convert(date,SoldDateTime),datename(weekday,SoldDateTime)
)

insert into Edw.dbo.EdwDailyLoadStatistics 
(           Name,                            Value,           WeekDayName, pw_Date,                pw_Counts, cw_Date,                cw_Counts, CountPercentage)
Select 'Count of I Bid Sold' ,a.IsIBIDWon,a.WeekDayName, pw_SoldDateTime, pw_Counts, cw_SoldDateTime, cw_Counts, pw_Counts/Case when cw_Counts=0 then 1 else cw_Counts end
From cw_IsIBIDWon a
		inner join pw_IsIBIDWon b on a.WeekDayName=b.WeekDayName and a.IsIBIDWon=b.IsIBIDWon


/*
--;With cw_ProcessStatusCode as(
--Select  datename(weekday,SoldDateTime) WeekDayName, DS.ProcessStatusDescription,Convert(date,SoldDateTime) cw_SoldDateTime, Count(*) cw_Counts 
--from Edw.[dbo].[DimStock] ds with (nolock)
--Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID-- where Convert(date,SoldDateTime) !='2999-12-31' Order by Convert(date,SoldDateTime) desc
--Where Convert(date,SoldDateTime)  = Convert(date,DateAdd(dd,-1,GETDATE()))
--Group by DS.ProcessStatusDescription,Convert(date,SoldDateTime),datename(weekday,SoldDateTime)
--)

--, pw_ProcessStatusCode as(
--Select  datename(weekday,SoldDateTime) WeekDayName, DS.ProcessStatusDescription,Convert(date,SoldDateTime) pw_SoldDateTime, Count(*) pw_Counts 
--from Edw.[dbo].[DimStock] ds with (nolock)
--Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID-- where Convert(date,SoldDateTime) !='2999-12-31' Order by Convert(date,SoldDateTime) desc
--Where Convert(date,SoldDateTime)  = Convert(date,DateAdd(dd,-8,GETDATE())) 
--Group by DS.ProcessStatusDescription,Convert(date,SoldDateTime),datename(weekday,SoldDateTime)
--)

----insert into Edw.dbo.EdwDailyLoadStatistics 
----(           Name,                            Value,           WeekDayName, pw_Date,                pw_Counts, cw_Date,                cw_Counts, CountPercentage)
--Select 'Count of ProcessStatusDescription' ,a.ProcessStatusDescription,a.WeekDayName, pw_SoldDateTime, pw_Counts, cw_SoldDateTime, cw_Counts, pw_Counts/cw_Counts
--From cw_ProcessStatusCode a
--		inner join pw_ProcessStatusCode b on a.WeekDayName=b.WeekDayName and a.ProcessStatusDescription=b.ProcessStatusDescription
*/


;With cw_SoldDateTime as(
Select  datename(weekday,SoldDateTime) WeekDayName, Convert(date,SoldDateTime) cw_SoldDateTime, Count(*) cw_Counts 
from Edw.[dbo].[DimStock] ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where Convert(date,SoldDateTime)  = Convert(date,DateAdd(dd,-1,GETDATE()))
Group by Convert(date,SoldDateTime),datename(weekday,SoldDateTime)
)

, pw_SoldDateTime as(
Select  datename(weekday,SoldDateTime) WeekDayName, Convert(date,SoldDateTime) pw_SoldDateTime, Count(*) pw_Counts 
from Edw.[dbo].[DimStock] ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where Convert(date,SoldDateTime)  = Convert(date,DateAdd(dd,-8,GETDATE())) 
Group by Convert(date,SoldDateTime),datename(weekday,SoldDateTime)
)

insert into Edw.dbo.EdwDailyLoadStatistics 
(           Name,                            Value,           WeekDayName, pw_Date,                pw_Counts, cw_Date,                cw_Counts, CountPercentage)
Select 'Count by SoldDateTime' ,' ' Value,a.WeekDayName, pw_SoldDateTime, pw_Counts, cw_SoldDateTime, cw_Counts, pw_Counts/Case when cw_Counts=0 then 1 else cw_Counts end
From cw_SoldDateTime a
		inner join pw_SoldDateTime b on a.WeekDayName=b.WeekDayName 



;With cw_AssignmentDateTime as(
Select  datename(weekday,AssignmentDateTime) WeekDayName, Convert(date,AssignmentDateTime) cw_AssignmentDateTime, Count(*) cw_Counts 
from Edw.[dbo].[DimStock] ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where Convert(date,AssignmentDateTime)  = Convert(date,DateAdd(dd,-1,GETDATE()))
Group by Convert(date,AssignmentDateTime),datename(weekday,AssignmentDateTime)
)
, pw_AssignmentDateTime as(
Select  datename(weekday,AssignmentDateTime) WeekDayName, Convert(date,AssignmentDateTime) pw_AssignmentDateTime, Count(*) pw_Counts 
from Edw.[dbo].[DimStock] ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where Convert(date,AssignmentDateTime)  = Convert(date,DateAdd(dd,-8,GETDATE())) 
Group by Convert(date,AssignmentDateTime),datename(weekday,AssignmentDateTime)
)

insert into Edw.dbo.EdwDailyLoadStatistics 
(           Name,                            Value,           WeekDayName, pw_Date,                pw_Counts, cw_Date,                cw_Counts, CountPercentage)
Select 'Count by AssignmentDateTime' ,' ' Value,a.WeekDayName, pw_AssignmentDateTime, pw_Counts, cw_AssignmentDateTime, cw_Counts, pw_Counts/Case when cw_Counts=0 then 1 else cw_Counts end
From cw_AssignmentDateTime a
		inner join pw_AssignmentDateTime b on a.WeekDayName=b.WeekDayName 



;With cw_NetReturn as(
Select  datename(weekday,convert(date,Convert(varchar(8),ds.SoldDayID))) WeekDayName, convert(date,Convert(varchar(8),ds.SoldDayID)) cw_SoldDateTime, Sum(isnull(NetReturn,0)) cw_Counts 
from Edw.[dbo].v_FactStock ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where convert(date,Convert(varchar(8),ds.SoldDayID))  = Convert(date,DateAdd(dd,-1,GETDATE()))
Group by convert(date,Convert(varchar(8),ds.SoldDayID)) ,datename(weekday,convert(date,Convert(varchar(8),ds.SoldDayID))) 
)

, pw_NetReturn as(
Select  datename(weekday,convert(date,Convert(varchar(8),ds.SoldDayID))) WeekDayName, convert(date,Convert(varchar(8),ds.SoldDayID)) pw_SoldDateTime, Sum(isnull(NetReturn,0)) pw_Counts 
from Edw.[dbo].v_FactStock ds with (nolock)
Inner join Edw.[dbo].[FactStockCycleTime] fsct With(nolock) on ds.StockID=fsct.StockID
Where convert(date,Convert(varchar(8),ds.SoldDayID))  = Convert(date,DateAdd(dd,-8,GETDATE()))
Group by convert(date,Convert(varchar(8),ds.SoldDayID)) ,datename(weekday,convert(date,Convert(varchar(8),ds.SoldDayID))) 
)

insert into Edw.dbo.EdwDailyLoadStatistics 
(           Name,                            Value,           WeekDayName, pw_Date,                pw_Counts, cw_Date,                cw_Counts, CountPercentage)
Select 'Sum of NetReturn by Sold Date' ,' ' Value,a.WeekDayName, a.cw_SoldDateTime, pw_Counts, cw_SoldDateTime, cw_Counts, pw_Counts/Case when cw_Counts=0 then 1 else cw_Counts end
From cw_NetReturn a
		inner join pw_NetReturn b on a.WeekDayName=b.WeekDayName 

--Select MAX(isnull(CountPercentage,0)) from #EdwDailyLoadStatistics
--select * from #EdwDailyLoadStatistics

Select @CountPercentage =MAX(isnull(CountPercentage,0)) from Edw.dbo.EdwDailyLoadStatistics 
END

