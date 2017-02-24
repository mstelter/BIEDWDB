

/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 4/10/2012
Description	: Populate the EDW DimAuction table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
10/01/2012  Somesh V            Added Auction Lane
10/02/2012  Somesh V            Removed Auction Lane
11/26/2012  Somesh V            Added logic for SpecaltyAuctionType and SpecaltyAuctionTypeMessage
2/6/2012	Matt S				Delete duplicate Auctions
==============================================================================
Usage Example:
Exec usp_ETLDimAuction 
--SELECT * FROM DimAuction
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimAuction]
AS     
BEGIN

DECLARE @ETLLoadID INT,
		@ExtractRowCount_sp INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@DeleteRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10),
		@MinAllowableDate		Datetime,
		@MaxAllowableDate		Datetime
SET NOCOUNT ON;
BEGIN TRY

----Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimAuction'
SELECT @MinAllowableDate = MIN(DayDate), @MaxAllowableDate = MAX(DayDate) from DimDay WITH (NOLOCK) WHERE DayID > 19000101 and DayID < 29991231

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table Auction
*********************************************************************************************/
CREATE TABLE #TMP_Auction
(	[Auction_ID] [int] NOT NULL, --[ASAP_Auction_Schedule_ID]
	[Branch_Number] [int] NULL,	--, [Branch_ID]
	[Auction_DayID] [int] NULL,	--, [Auction_DateTime] Datetime
	[Auction_DateTime] [datetime] NULL,	--, [Auction_DateTime] Datetime
	[Auction_Method_Code] [char](2) NULL,	--, [Auction_Method_Code] 
	[Auction_Method_Description] [varchar](40) NULL,	--, [Auction_Method_Code] 
	[Auction_Type_Code] [char](2) NULL,	--, [Auction_Type_Code]
	[Auction_Type_Description] [varchar](40) NULL,	--, [Auction_Type_Code]
	[Auctioneer_Name] [nvarchar](100) NULL,	--, [Auctioneer_ID]

)

CREATE TABLE #temp_AuctionRecordCount(ChangeType VARCHAR(30)) 

/*********************************************************************************************
Insert the values from the CDC Current Auction table.
*********************************************************************************************/
INSERT INTO #TMP_Auction
(	[Auction_ID]
	,[Branch_Number]
	,[Auction_DayID]
	,[Auction_DateTime]
	,[Auction_Method_Code]
	,[Auction_Method_Description]
	,[Auction_Type_Code]
	,[Auction_Type_Description]
	,[Auctioneer_Name]

	)
SELECT 
	 Auction_Schedule_ID
	,Branch_Number
    ,CASE WHEN Isnull(Auction_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(CONVERT(INT, CONVERT(CHAR(8), Auction_DateTime, 112)), 29991231) ELSE 29991231 END AS Auction_Day_ID
    ,CASE WHEN Isnull(Auction_DateTime, '2999-12-31') BETWEEN @MinAllowableDate AND @MaxAllowableDate THEN Isnull(Auction_DateTime, '2999-12-31') ELSE '2999-12-31' END AS Auction_DateTime
	,AuctionS.Auction_Method_Code
	,Auction_Method_Description
	,AuctionS.Auction_Type_Code
	,Auction_Type_Description
	,User_Name as Auctioneer_Name
	FROM (SELECT * FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Auction_Schedule](@StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5) AuctionS
	LEFT OUTER JOIN BI_ASAP_REP.dbo.Auction_Method AM WITH (NOLOCK)
		ON AuctionS.Auction_Method_Code = AM.Auction_Method_Code
	LEFT OUTER JOIN BI_ASAP_REP.dbo.Auction_Type AUT WITH (NOLOCK)
		ON AuctionS.Auction_Type_Code = AUT.Auction_Type_Code
	LEFT OUTER JOIN BI_ASAP_REP.dbo.User_Info UI WITH (NOLOCK)
		ON AuctionS.Auctioneer_ID = UI.User_ID

/*********************************************************************************************
MERGE the data from the temp table into the EDW DimAuction table
This does Update and Insert only.  No Deletes
*********************************************************************************************/
BEGIN TRANSACTION 
  
    MERGE [dbo].[DimAuction] AS UA
    USING #TMP_Auction AS A
          ON A.Auction_ID = UA.AuctionID
    WHEN MATCHED THEN 
    UPDATE SET 
		[AuctionID] = [Auction_ID]
		,[BranchNumber] = [Branch_Number]
		,[AuctionDayID] = [Auction_DayID]
		,[AuctionDateTime] = [Auction_DateTime]
		,[AuctionMethodCode] = [Auction_Method_Code]
		,[AuctionMethodDescription] = [Auction_Method_Description]
		,[AuctionTypeCode] = [Auction_Type_Code]
		,[AuctionTypeDescription] = [Auction_Type_Description]
		,[AuctioneerName] = [Auctioneer_Name]
		,ETLLoadUpdateID = @ETLLoadID	
   WHEN NOT MATCHED THEN
   INSERT([AuctionID]
	,[BranchNumber]
	,[AuctionDayID]
	,[AuctionDateTime]
	,[AuctionMethodCode]
	,[AuctionMethodDescription]
	,[AuctionTypeCode]
	,[AuctionTypeDescription]
	,[AuctioneerName]
	,ETLLoadID
	,ETLLoadUpdateID
	)	 
	VALUES ([Auction_ID]
	,[Branch_Number]
	,[Auction_DayID]
	,[Auction_DateTime]
	,[Auction_Method_Code]
	,[Auction_Method_Description]
	,[Auction_Type_Code]
	,[Auction_Type_Description]
	,[Auctioneer_Name]	
	,@ETLLoadID
	,@ETLLoadID
	)

     
OUTPUT $action INTO #temp_AuctionRecordCount;

  /*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #temp_AuctionRecordCount

--Specialty Auction Type 
 UPDATE DA
 SET 
 SpecailtyAuctionType = AAI.Message_Title
,SpecailtyAuctionTypeMessage = AAI.Message
 FROM 
 EDW.dbo.DimAuction DA WITH (NOLOCK)
      INNER JOIN [BI_Auction_Rep].[dbo].[Auction_Announcement_Info] AAI WITH (NOLOCK)
         ON DA.BranchNumber = AAI.BranchID  AND CONVERT(DATE,DA.AuctionDateTime) = CONVERT(DATE,AAI.AuctionDate)

--Delete future auctions deleted in ASAP
DELETE FROM DA
--SELECT * 
FROM edw.dbo.DimAuction DA WITH (NOLOCK)
LEFT OUTER JOIN BI_ASAP_REP.dbo.Auction_Schedule ASCH WITH (NOLOCK)
	ON DA.AuctionID = ASCH.Auction_Schedule_ID
LEFT OUTER JOIN (SELECT DISTINCT AuctionID FROM edw.dbo.DimAuctionItem DAI WITH (NOLOCK)
					WHERE 	DAI.AuctionDateTime > GETDATE()) DAI
						ON DA.AuctionID = DAI.AuctionID
WHERE 	ASCH.Auction_Schedule_ID IS NULL AND DA.AuctionDateTime between GETDATE() and '12/30/2999'
	AND DAI.AuctionID IS NULL
	  


/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
			DROP TABLE #temp_AuctionRecordCount
			DROP TABLE #TMP_Auction		
		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
			ROLLBACK TRAN
		
			IF OBJECT_ID('tempdb..#temp_AuctionRecordCount') IS NOT NULL DROP TABLE #temp_AuctionRecordCount
			IF OBJECT_ID('tempdb..#TMP_Auction') IS NOT NULL DROP TABLE #TMP_Auction
		EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	END CATCH
	END
GO


