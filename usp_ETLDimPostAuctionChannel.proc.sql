/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 06/12/2012
Description	: Populate the EDW DimPostAuctionChannel table from Xciara import
==============================================================================
REVISION History
ChangeDate	Developer			
7/2/2012	Matt Stelter	Changed the mail message to use XML
7/6/2012	Matt Stelter	Changed join to StockNumber
03/11/2015	Bruce Kutnick	Added TRUNCATE TABLE  [dbo].[PostAuctionChannelDailyFeed] just prior to the end of the TRY event.
						    This truncate was originally in DTSX package PostAuctionChannelStageInsert, but was moved here to correct for the situation
							where we would be missing PostAuction files if Step1 of EDWDailyLoad would have to be restarted after failure.
==============================================================================
Usage Example:
Exec usp_ETLDimPostAuctionChannel
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimPostAuctionChannel]
AS
  BEGIN
	  DECLARE @ETLLoadID       INT,
			  @InsertRowCount  INT,
			  @UpdateRowCount  INT,
			  @DeleteRowCount  INT,
			  @StartLSN        BINARY(10),
			  @EndLSN          BINARY(10),
			  @ExtractRowCount INT,
				@intRowCount INT,
				@intExceptionCount INT,
				@strQuery VARCHAR(MAX),
			 @strXciraLoadDate AS CHAR(5),
			 @strXciraLoadTime AS CHAR(5),
			 @strXciraSubject AS VARCHAR(200),
			 @strXciraMessage AS VARCHAR(400),
			 @strRecipients AS VARCHAR(200)

	  SET NOCOUNT ON;

	  BEGIN  TRY
		  --Run the Audit begin
		  EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimPostAuctionChannel'

CREATE TABLE #AuctionScheduleTmp
  (BranchNumber    INT NULL,
   AuctionDateTime DATETIME NULL,
   AuctionID       INT NULL)

CREATE TABLE #PostAuctionChannelStage
  ([XciraAuctionDateTime]      [DATETIME] NOT NULL,
   [XciraAuctionNumber]        [INT] NOT NULL,
   [XciraSaleNo]               [INT] NOT NULL,
   [AuctionDate]             [DATETIME] NOT NULL,
   [AuctionNumber]           [VARCHAR](20) NULL,
   [AwardedAmount]           [INT] NULL,
   [BranchName]              [VARCHAR](60) NULL,
   [BranchNumber]            [INT] NULL,
   [BuyerNo]                 [INT] NULL,
   [BuyerType]               [CHAR](1) NULL,
   [HighNonOnsiteBidder]     [INT] NULL,
   [HighNonOnsiteAmount]     [INT] NULL,
   [HighNonOnsiteType]       [CHAR](1) NULL,
   [ItemNo]                  [INT] NOT NULL,
   [IAAStockID]              [INT] NOT NULL,
   [LaneDesignator]          [CHAR](1) NULL,
   [NumAcceptedProxyBids]    [INT] NOT NULL,
   [NumAcceptedInternetBids] [INT] NOT NULL,
   [NumOfAcceptedFloorBids]  [INT] NOT NULL,
   [RunnerUpBid]             [INT] NULL,
   [RunnerUpBidderType]      [CHAR](1) NULL,
   [RunnerUpBuyer]           [VARCHAR](20) NULL,
   [Sourcename]              [VARCHAR](255) NOT NULL,
   [UpdateDateTime]          [DATETIME] NOT NULL,
   [BuyerNoEmployeeID]       [VARCHAR](20) NULL,
   [RunnerUpEmployeeID]      [VARCHAR](20) NULL,
   [HighNonOnSiteEmployeeID] [VARCHAR](20) NULL)

IF OBJECT_ID('tempdb..#tmp_RecordCount') IS NOT NULL DROP TABLE #tmp_RecordCount
CREATE TABLE #tmp_RecordCount(ChangeType VARCHAR(30)) 

IF OBJECT_ID('tempdb..#DimPostAuctionChannel_Final') IS NOT NULL DROP TABLE #DimPostAuctionChannel_Final
CREATE TABLE #DimPostAuctionChannel_Final(
	[XCiraAuctionDateTime] [datetime] NOT NULL,
	[XCiraAuctionNumber] [int] NOT NULL,
	[XCiraSaleNo] [int] NOT NULL,
	[AuctionDate] [datetime] NOT NULL,
	[AuctionNumber] [varchar](20) NULL,
	[AuctionScheduleID] [int] NOT NULL,
	[AwardedAmount] [int] NULL,
	[BranchNumber] [int] NOT NULL,
	[BuyerID] [int] NOT NULL,
	[BuyerType] [char](1) NULL,
	[HighNonOnsiteBidder] [int] NULL,
	[HighNonOnsiteAmount] [int] NULL,
	[HighNonOnsiteType] [char](1) NULL,
	[ItemNo] [int] NOT NULL,
	[StockID] [int] NOT NULL,
	[LaneDesignator] [char](1) NULL,
	[NumAcceptedProxyBids] [int] NOT NULL,
	[NumAcceptedInternetBids] [int] NOT NULL,
	[NumOfAcceptedFloorBids] [int] NOT NULL,
	[RunnerUpBid] [int] NULL,
	[RunnerUpBidderType] [char](1) NULL,
	[RunnerUpBuyer] [varchar](20) NULL,
	[Sourcename] [varchar](255) NOT NULL,
	[UpdateDateTime] [datetime] NOT NULL,
	[StockNumber] [int] NULL)
 
IF OBJECT_ID('tempdb..#TMP_Xcira_AUCTION_FILE_LOAD') IS NOT NULL DROP TABLE #TMP_Xcira_AUCTION_FILE_LOAD
CREATE TABLE #TMP_Xcira_AUCTION_FILE_LOAD(BranchNumber INT,
		   BranchName VARCHAR(50),
		   Received_Data VARCHAR(10),
		   Auction_Date DATETIME,
		   Load_Date DATETIME) 
		   
IF OBJECT_ID('tempdb..#TMPAuctionLoadedForDAY') IS NOT NULL DROP TABLE #TMPAuctionLoadedForDAY
CREATE TABLE #TMPAuctionLoadedForDAY(BranchNumber INT,
		   BranchName VARCHAR(50),
		   ReceivedData VARCHAR(10),
		   UpdateDateTime DATETIME,
		   auctiondatetime DATETIME,
		   AuctionId DATETIME)		   
		   
/*************END CREATE TEMP TABLES*****************/	
	   
INSERT INTO #PostAuctionChannelStage
			(BranchName,BranchNumber,LaneDesignator,AuctionNumber,XciraAuctionDatetime,XciraAuctionNumber,AuctionDate,XciraSaleNo,ItemNo,IAAStockID,BuyerNo,
			 AwardedAmount
			 ,BuyerType,RunnerUpBuyer,RunnerUpBid,RunnerUpBidderType,HighNonOnsiteBidder,HighNonOnsiteAmount,HighNonOnsiteType,NumAcceptedProxyBids,
			 NumAcceptedInternetBids,NumOfAcceptedFloorBids,UpdateDateTime,SourceName,BuyerNoEmployeeID,RunnerUpEmployeeID,HighNonOnSiteEmployeeID)
SELECT Substring(src.sourcename, 4, (Charindex('_', src.sourcename)-8)) AS branch_name,
	   Substring(src.sourcename, (Charindex('_', src.sourcename))-4, 3) AS branch_number,
	   Substring(src.sourcename, (Charindex('_', src.sourcename))-1, 1) AS lane_designator,
	   Substring(src.sourcename, (Charindex('_', src.sourcename)+1), -1*(Charindex('_', src.sourcename)+1-Charindex('_20', src.sourcename))) AS
	   auction_number,
	   Substring(src.sourcename, (Charindex('_20', src.sourcename))+1, 10) AS Xcira_auction_date,
	   Substring(src.sourcename, (Charindex('_20', src.sourcename))+12, (Charindex('.csv', src.sourcename))-((Charindex('_20', src.sourcename))+12))
	   AS
	   Xcira_auction_number,
	   src.AuctionDate,
	   src.AMSSaleNo,
	   src.ItemNo,
	   Cast(Substring(src.IAAStockID, (Charindex('-', src.IAAStockID))+1, 16) AS INT),
	   CASE
		 WHEN Charindex('_', src.BuyerNo)>0 THEN LEFT(src.BuyerNo, Charindex('_', src.BuyerNo)-1)
		 ELSE Isnull(Cast(src.BuyerNo AS INT), 1000000)
	   END,
	   src.AwardedAmount,
	   src.BuyerType,
	   CASE
		 WHEN Charindex('_', src.RunnerUpBuyer)>0 THEN LEFT(src.RunnerUpBuyer, Charindex('_', src.RunnerUpBuyer)-1)
		 ELSE src.RunnerUpBuyer
	   END,
	   src.RunnerUpBid,
	   src.RunnerUpBidderType,
	   CASE
		 WHEN Charindex('_', src.HighNonOnsiteBidder)>0 THEN LEFT(src.HighNonOnsiteBidder, Charindex('_', src.HighNonOnsiteBidder)-1)
		 ELSE src.HighNonOnsiteBidder
	   END,
	   src.HighNonOnsiteAmount,
	   src.HighNonOnsiteType,
	   src.NumAcceptedProxyBids,
	   src.NumAcceptedInternetBids,
	   src.NumOfAcceptedFloorBids,
	   BD.BusinessDateTime,
	   src.sourcename,
	   CASE
		 WHEN Charindex('_', src.BuyerNo)>0 THEN RIGHT(src.BuyerNo, Len(src.BuyerNo)-Charindex('_', src.BuyerNo))
	   END,
	   CASE
		 WHEN Charindex('_', src.RunnerUpBuyer)>0 THEN RIGHT(src.RunnerUpBuyer, Len(src.RunnerUpBuyer)-Charindex('_', src.RunnerUpBuyer))
	   END,
	   CASE
		 WHEN Charindex('_', src.HighNonOnsiteBidder)>0 THEN RIGHT(src.HighNonOnsiteBidder, Len(src.HighNonOnsiteBidder)-
																							Charindex('_', src.HighNonOnsiteBidder))
	   END
FROM   (SELECT *--,substring(src1.sourcename, (charindex('_', src1.sourcename))- 4, 3) 
		FROM   dbo.PostAuctionChannelDailyFeed src1 WITH (NOLOCK)
		--We need to load only those file where auction_number starts with PR     
		WHERE  LEFT(Substring(src1.sourcename, (Charindex('_', src1.sourcename)+1), -1*(Charindex('_', src1.sourcename)+1-Charindex('_20',src1.sourcename))), 2)IN('PR','QA')
		   AND
		   --If the auction_number starts with PR it may still has a invalid branch_number on the file.
		   Isnumeric(Substring(src1.sourcename, (Charindex('_', src1.sourcename))-4, 3))=1) src
	   CROSS JOIN dbo.BusinessDay BD WITH (NOLOCK)
WHERE  (Isnumeric(CASE WHEN Charindex('_', src.BuyerNo)>0 THEN LEFT(src.BuyerNo, Charindex('_', src.BuyerNo)-1)
					ELSE src.BuyerNo END)=1
		AND CASE
			  WHEN Charindex('_', src.BuyerNo)>0 THEN LEFT(src.BuyerNo, Charindex('_', src.BuyerNo)-1)
			  ELSE src.BuyerNo
			END NOT LIKE '%[-+.$]%'
		AND Cast(CASE WHEN Charindex('_', src.BuyerNo)>0 THEN LEFT(src.BuyerNo, Charindex('_', src.BuyerNo)-1)
				   ELSE src.BuyerNo
				 END AS BIGINT)<=2147483647-- Avoid the overflowed error
						)
		OR BuyerNo IS NULL

/*************************************************************************************
*******************Populate DimPostAuctionChannnel************************************
*************************************************************************************/
INSERT INTO #AuctionScheduleTmp
			(BranchNumber,AuctionDateTime,AuctionID)
SELECT BranchNumber,
	   AuctionDateTime,
	   Max(AuctionID) AS AuctionID
--into #AuctionScheduleTmp
FROM   dbo.DimAuction WITH (NOLOCK)
WHERE  AuctionDateTime>Getdate()-7
GROUP  BY BranchNumber,
		  AuctionDateTime

--SELECT * FROM #AuctionScheduleTmp
--SELECT * FROM #PostAuctionChannelStage


 INSERT INTO #DimPostAuctionChannel_Final
			(XCiraAuctionDateTime,XCiraAuctionNumber,XCiraSaleNo,AuctionDate,AuctionNumber,AuctionScheduleID,AwardedAmount,BranchNumber,BuyerID,
			 BuyerType,
			 HighNonOnsiteBidder,HighNonOnsiteAmount,HighNonOnsiteType,ItemNo,StockNumber,LaneDesignator,NumAcceptedProxyBids,NumAcceptedInternetBids,
			 NumofAcceptedFloorBids,RunnerUpBid,RunnerUpBidderType,RunnerUpBuyer,StockID,SourceName,UpdateDateTime)
SELECT A.XciraAuctionDateTime,
	   A.XciraAuctionNumber,
	   A.XciraSaleNo,
	   A.AuctionDate,
	   A.AuctionNumber,
	   Isnull(AU.AuctionID, 1000000),
	   A.AwardedAmount,
	   Isnull(BR.BranchNumber, 0),
	   Isnull(BUY.BuyerID, 1000000),
	   A.BuyerType,
	   A.HighNonOnsiteBidder,
	   A.HighNonOnsiteAmount,
	   A.HighNonOnsiteType,
	   A.ItemNo,
	   A.IAAStockID,
	   A.LaneDesignator,
	   A.NumAcceptedProxyBids,
	   A.NumAcceptedInternetBids,
	   A.NumofAcceptedFloorBids,
	   A.RunnerUpBid,
	   A.RunnerUpBidderType,
	   A.RunnerUpBuyer,
	   Isnull(ST.StockID, 1000000),
	   A.SourceName,
	   A.UpdateDateTime
FROM   #PostAuctionChannelStage A WITH (NOLOCK)
	   LEFT OUTER JOIN dbo.DimBranch BR WITH (NOLOCK)
		 ON A.BranchNumber=BR.branchnumber
	   LEFT OUTER JOIN #AuctionScheduleTmp AU WITH (NOLOCK)
		 ON A.AuctionDate=AU.AuctionDateTime
			AND AU.branchNumber=BR.branchNumber
	   LEFT OUTER JOIN dbo.DimBuyer BUY WITH (NOLOCK)
		 ON A.buyerNo=BUY.BuyerID
	   LEFT OUTER JOIN dbo.DimStock ST WITH (NOLOCK)
		 ON ST.StockNumber=A.IAAStockID
WHERE  A.IAAStockID IS NOT NULL

--SELECT * FROM #DimPostAuctionChannel_Final

BEGIN TRAN
 
     MERGE [dbo].[DimPostAuctionChannel] AS PAC
    USING #DimPostAuctionChannel_Final AS PACF WITH (NOLOCK)
          ON PACF.XciraAuctionDateTime=PAC.XciraAuctionDateTime
					  AND PACF.XciraSaleNo=PAC.XciraSaleNo
					  AND PACF.StockID=PAC.StockID
    WHEN MATCHED THEN 
        UPDATE SET 
			PAC.XCiraAuctionDateTime=PACF.XCiraAuctionDateTime,
			PAC.XCiraAuctionNumber=PACF.XCiraAuctionNumber,
			PAC.XCiraSaleNo=PACF.XCiraSaleNo,
			PAC.AuctionDate=PACF.AuctionDate,
			PAC.AuctionNumber=PACF.AuctionNumber,
			--PAC.AuctionScheduleID=PACF.AuctionScheduleID,
			PAC.AwardedAmount=PACF.AwardedAmount,
			PAC.BranchNumber=PACF.BranchNumber,
			PAC.BuyerID=PACF.BuyerID,
			PAC.BuyerType=PACF.BuyerType,
			PAC.HighNonOnsiteBidder=PACF.HighNonOnsiteBidder,
			PAC.HighNonOnsiteAmount=PACF.HighNonOnsiteAmount,
			PAC.HighNonOnsiteType=PACF.HighNonOnsiteType,
			PAC.ItemNo=PACF.ItemNo,
			PAC.StockID=PACF.StockID,
			PAC.LaneDesignator=PACF.LaneDesignator,
			PAC.NumAcceptedProxyBids=PACF.NumAcceptedProxyBids,
			PAC.NumAcceptedInternetBids=PACF.NumAcceptedInternetBids,
			PAC.NumOfAcceptedFloorBids=PACF.NumOfAcceptedFloorBids,
			PAC.RunnerUpBid=PACF.RunnerUpBid,
			PAC.RunnerUpBidderType=PACF.RunnerUpBidderType,
			PAC.RunnerUpBuyer=PACF.RunnerUpBuyer,
			PAC.Sourcename=PACF.Sourcename,
			PAC.UpdateDateTime=PACF.UpdateDateTime,
			PAC.StockNumber=PACF.StockNumber
	WHEN NOT MATCHED THEN
    INSERT(XCiraAuctionDateTime,
			XCiraAuctionNumber,
			XCiraSaleNo,
			AuctionDate,
			AuctionNumber,
			AuctionScheduleID,
			AwardedAmount,
			BranchNumber,
			BuyerID,
			BuyerType,
			HighNonOnsiteBidder,
			HighNonOnsiteAmount,
			HighNonOnsiteType,
			ItemNo,
			StockID,
			LaneDesignator,
			NumAcceptedProxyBids,
			NumAcceptedInternetBids,
			NumOfAcceptedFloorBids,
			RunnerUpBid,
			RunnerUpBidderType,
			RunnerUpBuyer,
			Sourcename,
			UpdateDateTime,
			StockNumber) 
	  VALUES (PACF.XCiraAuctionDateTime,
				PACF.XCiraAuctionNumber,
				PACF.XCiraSaleNo,
				PACF.AuctionDate,
				PACF.AuctionNumber,
				PACF.AuctionScheduleID,
				PACF.AwardedAmount,
				PACF.BranchNumber,
				PACF.BuyerID,
				PACF.BuyerType,
				PACF.HighNonOnsiteBidder,
				PACF.HighNonOnsiteAmount,
				PACF.HighNonOnsiteType,
				PACF.ItemNo,
				PACF.StockID,
				PACF.LaneDesignator,
				PACF.NumAcceptedProxyBids,
				PACF.NumAcceptedInternetBids,
				PACF.NumOfAcceptedFloorBids,
				PACF.RunnerUpBid,
				PACF.RunnerUpBidderType,
				PACF.RunnerUpBuyer,
				PACF.Sourcename,
				PACF.UpdateDateTime,
				PACF.StockNumber)
	OUTPUT $action INTO #tmp_RecordCount;

SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #tmp_RecordCount

COMMIT TRAN

/****************************************************************************************
***************************EMAIL NOTIFICATION SECTION************************************
****************************************************************************************/
INSERT INTO #TMPAuctionLoadedForDAY(BranchNumber,
		   BranchName,
		   ReceivedData,
		   UpdateDateTime,
		   auctiondatetime,
		   AuctionId)
SELECT ia.BranchNumber,
	   ia.BranchName,
	   CASE
		 WHEN Count(AI.AuctionItemID)>10 THEN
		   CASE
			 WHEN Count(ps.PostAuctionChannelID)>10 THEN 'Yes'
			 ELSE 'No'
		   END
		 ELSE 'NA'
	   END AS ReceivedData,
	   ps.UpdateDateTime,
	   as1.auctiondatetime,
	   As1.AuctionId
FROM   dbo.DimAuction as1 WITH (NOLOCK)
	   LEFT OUTER JOIN dbo.DimPostAuctionChannel ps WITH (NOLOCK)
		 ON as1.AuctionID=ps.AuctionScheduleID
	   INNER JOIN dbo.DimBranch ia WITH (NOLOCK)
		 ON ia.BranchNumber=as1.BranchNumber
	   LEFT OUTER JOIN dbo.DimAuctionItem AI WITH (NOLOCK)
		 ON AI.AuctionId=as1.AuctionID
WHERE  ps.UpdateDateTime=(CASE Datepart(dw, Getdate())
							 WHEN 7 THEN CONVERT(DATETIME, CONVERT(VARCHAR(20), Getdate()-1, 101), 101)
							 ELSE CONVERT(DATETIME, CONVERT(VARCHAR(20), Getdate(), 101), 101)
						   END)
GROUP  BY ia.BranchNumber,
		  BranchName,
		  ps.AuctionDate,
		  ps.UpdateDateTime,
		  as1.auctiondatetime,
		  As1.AuctionId
ORDER  BY ps.AuctionDate,
		  ia.BranchNumber,
		  BranchName,
		  ps.UpdateDateTime

SET @intRowCount = @@ROWCOUNT

IF @intRowCount=90 BEGIN
	  SELECT 'NOTHING LOADED FOR '+CONVERT(CHAR(5), Getdate(), 110)
  END
ELSE BEGIN

INSERT INTO #TMP_Xcira_AUCTION_FILE_LOAD(BranchNumber,
		   BranchName,
		   Received_Data,
		   Auction_Date,
		   Load_Date)
	SELECT ia.BranchNumber,
		   ia.BranchName,
		   CASE
			 WHEN Count(AI.AuctionItemID)>10 THEN
			   CASE
				 WHEN (TMPALFD.UpdateDateTime=CASE Datepart(dw, Getdate())
												 WHEN 7 THEN CONVERT(DATETIME, CONVERT(VARCHAR(20), Getdate()-1, 101), 101)
												 ELSE CONVERT(DATETIME, CONVERT(VARCHAR(20), Getdate(), 101), 101)
											   END) THEN 'Yes'
				 ELSE 'No'
			   END
			 ELSE 'NA'
		   END AS Received_Data,
		   TMPALFD.auctiondatetime AS Auction_Date,
		   TMPALFD.UpdateDateTime AS Load_Date
	FROM   dbo.DimAuction as1 WITH (NOLOCK)
		   LEFT OUTER JOIN #TMPAuctionLoadedForDAY TMPALFD WITH (NOLOCK)
			 ON as1.AuctionID=TMPALFD.AuctionID
		   INNER JOIN dbo.DimBranch ia WITH (NOLOCK)
			 ON ia.BranchNumber=as1.BranchNumber
		   LEFT OUTER JOIN dbo.DimAuctionItem AI WITH (NOLOCK)
			 ON AI.AuctionId=as1.AuctionID
	WHERE  as1.auctiondatetime=(CASE Datepart(dw, Getdate())
								   WHEN 7 THEN CONVERT(DATETIME, CONVERT(VARCHAR(20), Getdate()-1, 101), 101)
								   ELSE CONVERT(DATETIME, CONVERT(VARCHAR(20), Getdate(), 101), 101)
								 END)
	GROUP  BY ia.BranchNumber,
			  ia.BranchName,
			  TMPALFD.auctiondatetime,
			  TMPALFD.UpdateDateTime
	ORDER  BY ia.BranchNumber,
			  ia.BranchName

	CREATE TABLE #TMPXciraAUCTIONFILELOAD (Display_Order_Id INT,Branch_Number INT,Branch_Name VARCHAR(100),Received_Data VARCHAR(100),Auction_Date VARCHAR(100),Load_Date VARCHAR(100))
	INSERT INTO #TMPXciraAUCTIONFILELOAD (Display_Order_Id,Branch_Number,Branch_Name,Received_Data,Auction_Date,Load_Date)
	SELECT 1,
		   BranchNumber,
		   Ltrim(Rtrim(BranchName)) AS Branch_Name,
		   Received_Data,
		   CONVERT(CHAR(10), Auction_Date, 110) AS Auction_Date,
		   CONVERT(CHAR(10), Load_Date, 110) AS Load_Date
	FROM   #TMP_Xcira_AUCTION_FILE_LOAD WITH (NOLOCK)
	WHERE  Received_Data<>'NA'
	UNION ALL
	SELECT 2,NULL,'---','---','---','---'
	UNION ALL
	SELECT 3,BranchNumber,Ltrim(Rtrim(BranchName)) AS Branch_Name,ReceivedData,
		   CONVERT(CHAR(10), auctiondatetime, 110) AS Auction_Date,
		   CONVERT(CHAR(10), UpdateDateTime, 110) AS Load_Date
	FROM   #TMPAuctionLoadedForDAY WITH (NOLOCK)
	WHERE  ReceivedData<>'NA'
	   AND auctiondatetime<>(CASE Datepart(dw, Getdate())
								WHEN 7 THEN CONVERT(DATETIME, CONVERT(VARCHAR(20), Getdate()-1, 101), 101)
								ELSE CONVERT(DATETIME, CONVERT(VARCHAR(20), Getdate(), 101), 101)
							  END)
	ORDER  BY 1,Auction_Date,Branch_Name

	SELECT @intExceptionCount=Count(*)
	FROM   #TMPXciraAUCTIONFILELOAD WITH (NOLOCK)
	WHERE  Display_Order_Id=3

	SELECT @strXciraLoadDate=CONVERT(CHAR(5), Getdate(), 101),
		   @strXciraLoadTime=CONVERT(CHAR(5), Getdate(), 108)

	SELECT @strXciraSubject=@@SERVERNAME + ' - Xcira Post Auction Channel Data for '+@strXciraLoadDate+' at '+@strXciraLoadTime,
		   @strXciraMessage='Xcira Post Auction Channel data for '+@strXciraLoadDate+' for the following auctions has been imported into EDW.'

--'mstelter@iaai.com;'--mvenugopalan@iaai.com;OPSMonitor@iaai.com;rmondor@xcira.com;operations@xcira.com;clovely@xcira.com;tylerb@xcira.com;'
	SELECT @strRecipients=[ConfiguredValue]--'mstelter@iaai.com;'--mvenugopalan@iaai.com;OPSMonitor@iaai.com;rmondor@xcira.com;operations@xcira.com;clovely@xcira.com;tylerb@xcira.com;'
	 FROM [msdb].[dbo].[SSISConfigurations]
	 WHERE [ConfigurationFilter]='PostAuctionChannelFTP' AND [PackagePath]='\Package.Variables[User::varToMail].Properties[Value]'

 --EXEC [msdb].[dbo].[sp_send_dbmail] @recipients = @strRecipients,@query = @strQuery,@subject = @strXciraSubject,@body = @strXciraMessage,@profile_name = 'PEDWDB'

IF @intExceptionCount=0 
BEGIN
	SET @strQuery =
		N'<H3>Xcira Post Auction Channel data for ' + @strXciraLoadDate + ' for the following auctions has been imported into EDW.</H3>' + 
		N'<table border="1">' + 
		N'<tbody align="center" style="font-family:times new roman; font-size:12">' +
		N'<tr><th>Branch Number</th><th>Branch Name</th><th>Received Data</th><th>Auction Date</th><th>Load Date</th></tr>' + 
		CAST((SELECT td = CAST(ISNULL(Branch_Number,'') as varchar(3)), ''
				,td = LTRIM(RTRIM(ISNULL(Branch_Name,''))), ''
				,td = CAST(ISNULL(Received_Data,'') as varchar(5)), ''
				,td = ISNULL(Auction_Date,''), ''
				,td = ISNULL(Load_Date,''), '' 
		FROM #TMPXciraAUCTIONFILELOAD WITH (NOLOCK) WHERE Display_Order_Id = 1 ORDER BY Display_Order_Id, Branch_Name
		FOR XML PATH('tr'),TYPE) AS NVARCHAR(MAX)) + N'</tbody>' + N'</table>' 
END
ELSE 
BEGIN
		SET @strQuery =
	N'<H3>Xcira Post Auction Channel data for ' + @strXciraLoadDate + ' for the following auctions has been imported into EDW.</H3>' + 
	N'<table border="1">' + 
	N'<tbody align="center" style="font-family:times new roman; font-size:12">' +
	N'<tr><th>Branch Number</th><th>Branch Name</th><th>Received Data</th><th>Auction Date</th><th>Load Date</th></tr>' + 
	CAST((SELECT td = CAST(ISNULL(Branch_Number,'') as varchar(3)), ''
			,td = LTRIM(RTRIM(ISNULL(Branch_Name,''))), ''
			,td = CAST(ISNULL(Received_Data,'') as varchar(5)), ''
			,td = ISNULL(Auction_Date,''), ''
			,td = ISNULL(Load_Date,''), '' 
	FROM #TMPXciraAUCTIONFILELOAD WITH (NOLOCK) ORDER BY Display_Order_Id, Branch_Name
	FOR XML PATH('tr'),TYPE) AS NVARCHAR(MAX)) + N'</tbody>' + N'</table>' 
END

EXEC msdb.dbo.sp_send_dbmail @recipients=@strRecipients,@subject=@strXciraSubject,@body=@strQuery,@profile_name='PEDWDB',@body_format='HTML'

END 

		  EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ExtractRowCount = @ExtractRowCount,@InsertRowCount = @InsertRowCount,
		  @UpdateRowCount = @UpdateRowCount

			IF Object_id('tempdb..#TMP_Xcira_AUCTION_FILE_LOAD') IS NOT NULL DROP TABLE #TMP_Xcira_AUCTION_FILE_LOAD
			IF Object_id('tempdb..#TMPAuctionLoadedForDAY') IS NOT NULL DROP TABLE #TMPAuctionLoadedForDAY
			IF Object_id('tempdb..#TMPXciraAUCTIONFILELOAD') IS NOT NULL DROP TABLE #TMPXciraAUCTIONFILELOAD 
			IF Object_id('tempdb..#AuctionScheduleTmp') IS NOT NULL DROP TABLE #AuctionScheduleTmp
			IF Object_id('tempdb..#PostAuctionChannelStage') IS NOT NULL DROP TABLE #PostAuctionChannelStage
			IF Object_id('tempdb..#DimPostAuctionChannel_Final') IS NOT NULL DROP TABLE #DimPostAuctionChannel_Final

			TRUNCATE TABLE  [dbo].[PostAuctionChannelDailyFeed]

		  RETURN 0
	  END TRY

	  BEGIN CATCH
		  SET NOCOUNT OFF

		  ROLLBACK TRAN

		  EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ExtractRowCount = @ExtractRowCount,@InsertRowCount = @InsertRowCount,
		  @UpdateRowCount = @UpdateRowCount,@ErrorCode = @@ERROR

		  DECLARE @ErrorMessage  NVARCHAR(4000),
				  @ErrorSeverity INT,
				  @ErrorState    INT;

		  SELECT @ErrorMessage=Error_message(),
				 @ErrorSeverity=Error_severity(),
				 @ErrorState=Error_state();

		  RAISERROR (@ErrorMessage,
					 @ErrorSeverity,
					 @ErrorState);
					 
			IF Object_id('tempdb..#TMP_Xcira_AUCTION_FILE_LOAD') IS NOT NULL DROP TABLE #TMP_Xcira_AUCTION_FILE_LOAD
			IF Object_id('tempdb..#TMPAuctionLoadedForDAY') IS NOT NULL DROP TABLE #TMPAuctionLoadedForDAY
			IF Object_id('tempdb..#TMPXciraAUCTIONFILELOAD') IS NOT NULL DROP TABLE #TMPXciraAUCTIONFILELOAD 
			IF Object_id('tempdb..#AuctionScheduleTmp') IS NOT NULL DROP TABLE #AuctionScheduleTmp
			IF Object_id('tempdb..#PostAuctionChannelStage') IS NOT NULL DROP TABLE #PostAuctionChannelStage
			IF Object_id('tempdb..#DimPostAuctionChannel_Final') IS NOT NULL DROP TABLE #DimPostAuctionChannel_Final
		  RETURN -1
	  END CATCH
  END
