
Create PROCEDURE [dbo].[usp_OHAAggregatedDetails] 
(
@BranchName varchar(255),
@QtrYear Varchar(6),
@FileType Varchar(50)=Null
)
/*************************************************************
USE EDW
GO 
==============================================================================
Author		: Mohammad Waheed
Create date	: 09/15/2016
Description	: Aggregated Data from [EDW].[DBO].OHAInformation
==============================================================================

Revision History
Date			Author			 Revision Description

Usage:
EXEC  [dbo].[usp_OHAAggregatedDetails]  'Auction Center - IL','2016Q4'

*************************************************************/
As
begin

--declare @BranchName varchar(255)='Corporate Head Quarters - Weschester' ,@QtrYear Varchar(6)='2016Q3',@FileType Varchar(50)='IC'
Select Sum(Answer) as YesCounts
	  ,Sum(Case when Answer =0 then 1 else 0 end) as NoCounts--Making No Value to 1 to sum
	  ,Sum(Answer)+Sum(Case when Answer =0 then 1 else 0 end) As TotalCounts --Sum of YesCount+NoCounts
	  ,Sum((Answer*Score)) As YesPoints
	  ,0 As NoPoints
	  ,Sum((Answer*Score)) As PossibleYesPoints 
	  ,Sum((Case when Answer =0 then 1 else 0 end )*Score) as PossibleNoPoints  --Missing score for NoPoints since they are 0
	  ,(Sum(Answer)*100)/(Sum(Answer)+Sum(Case when Answer =0 then 1 else 0 end)) as YesRate  --YesCounts divided by TotalCounts (Percentage)
	  ,(Sum(Case when Answer =0 then 1 else 0 end)*100)/(Sum(Answer)+Sum(Case when Answer =0 then 1 else 0 end)) NoRate--NoCounts divided by TotalCounts (Percentage)
	  ,((Sum((Answer*Score))+0)*100)/(Sum((Answer*Score))+Sum((Case when Answer =0 then 1 else 0 end)*Score)) TotalRate -- TotalPoints divided by TotalPossiblePoints
from OHAInformation
where Score is not null and answer!=2
and 
BranchName=@BranchName 
and 	
QtrYear=@QtrYear 
and 
FileType=isnull(@FileType,FileType)


 End