
/*
USE EDW
GO 
==============================================================================
Author		: Somesh Velupalli 
Create date	: 09/20/2012
Description	: Populate the EDW DimRapidBidDetail table 
==============================================================================
REVISION History
ChangeDate	     Developer	    	Release/Problem Number
9/25/2012		Matt Stelter		Added Auction Lane
9/26/2012       Somesh V            Removed AuctionID,AuctionLane,StockID,ProviderID,BranchNumber 
                                    and added AuctionItemID
1/4/2012		Matt Stelter		Fixed duplicates

==============================================================================
Usage Example:
Exec usp_ETLDimRapidBidDetail
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimRapidBidDetail]
AS
  DECLARE @ExtractRowCount_sp INT,
          @ETLLoadID_sp       INT,
          @InsertRowCount_sp  INT,
          @UpdateRowCount_sp  INT,
          @CurrentDatetime    DATETIME

  SET NOCOUNT ON;

  BEGIN TRY
      --Run the Audit Begin
      EXEC @ETLLoadID_sp = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimRapiBidDetail'

      SET @CurrentDatetime = (SELECT ER.MaxDatetime
                              FROM   dbo.ETLRun ER WITH (NOLOCK)
                              WHERE  ETLJobName='DailyETLJob')

 /**************************************************CREATE TEMP TABLES***********************************************************/
      CREATE TABLE #TMP_Bidder_List
        (TMP_Bidder_List_ID  INT IDENTITY(1, 1) NOT FOR REPLICATION NOT NULL,
         Buyer_Employee_ID   INT,
         Buyer_ID            INT,
         Bidder_Display_Name VARCHAR(100))

      CREATE CLUSTERED INDEX IDX_Bidder_List
        ON #TMP_Bidder_List(Bidder_Display_Name)
        
      CREATE TABLE #TMP_Auction_Schedule_Source
        ([Auction_Item_ID]                   [INT],
         [Auction_Item_Sequence_Number] [VARCHAR] (80),
         [Auction_DateTime]             [DATETIME],
         [Auction_Day_ID]               [CHAR] (8),
         BranchNumber INT)

      CREATE CLUSTERED INDEX IDX_Auction_Schedule_Source
        ON #TMP_Auction_Schedule_Source(Auction_Day_ID,Auction_Item_Sequence_Number)

      CREATE TABLE #TMP_Rapid_Bid_Detail_Source
        (Rapid_Bid_Datetime        [DATETIME] NULL,
         Rapid_Bid_Day_ID          [INT] NULL,
         Rapid_Bid_Amount          [MONEY] NULL,
         Rapid_Bid_Displaylocation [NVARCHAR] (50) NULL,
         Rapid_Bid_Displayname     [NVARCHAR] (50) NULL,
         Auction_Item_ID                INT NULL,
         Buyer_ID                  [INT] NULL,
         Buyer_Employee_ID         [INT] NULL)
         
      CREATE CLUSTERED INDEX IDX_Rapid_Bid_Detail_Source
        ON #TMP_Rapid_Bid_Detail_Source(Auction_Item_ID, Rapid_Bid_Datetime, Rapid_Bid_Displayname, Rapid_Bid_Amount)

      CREATE TABLE #temp_RecordCount
        (ChangeType VARCHAR(10))
        
 /**************************************************************************************************************************/
                      
      INSERT INTO #TMP_Bidder_List
                  (Buyer_Employee_ID,Buyer_ID,Bidder_Display_Name)
      SELECT Bidder.BuyerEmployeeID,
             Bidder.BuyerID,
             Bidder.BidderDisplayName
      FROM   dbo.DimBuyerEmployee AS Bidder WITH (NOLOCK)

      INSERT INTO #TMP_Auction_Schedule_Source
                  (Auction_Item_ID,Auction_Item_Sequence_Number,Auction_DateTime,Auction_Day_ID,BranchNumber)
      SELECT  DAI.AuctionItemID,
              Cast(DAI.AuctionItemSequenceNumber AS VARCHAR(80)) AS Auction_Item_Sequence_Number,
              DAI.AuctionDatetime,
              CONVERT(CHAR(8), DAI.AuctionDateTime, 112) AS Auction_Day_ID,BranchNumber                      
      FROM   dbo.DimAuctionItem DAI WITH (NOLOCK)
             INNER JOIN dbo.DimAuction AI WITH (NOLOCK)
               ON DAI.AuctionID=AI.AuctionID
     --      WHERE  ASCH.Auction_DateTime>= Dateadd(D, -1, @Last_Run_Start)
      WHERE  DAI.AuctionDateTime BETWEEN Dateadd(D, -1, @CurrentDatetime) AND Dateadd(D, 1, @CurrentDatetime)

      INSERT INTO #TMP_Rapid_Bid_Detail_Source
                  (Rapid_Bid_Datetime,Rapid_Bid_Day_ID,Rapid_Bid_Amount,Rapid_Bid_Displaylocation,Rapid_Bid_Displayname,Auction_Item_ID,
                   Buyer_ID,Buyer_Employee_ID)
      SELECT Dateadd(hh, -1, RBL.TimeStamp) AS Rapid_Bid_Datetime,
             Cast(CONVERT(CHAR(8), Dateadd(hh, -1, RBL.TimeStamp), 112) AS INT) AS Rapid_Bid_Day_ID,
             Isnull(RBL.RapidAmount, 0) AS Rapid_Bid_Amount,
             RBL.Location AS Rapid_Bid_Displaylocation,
             RBL.Bidder AS Rapid_Bid_Displayname,
             TASS.Auction_Item_ID AS Auction_Item_ID,
             Bidder.Buyer_ID AS Buyer_ID,
             Bidder.Buyer_Employee_ID AS Bidder_DM_ID			
      FROM   (SELECT Max([RapidBidLogID]) AS MAXRapidBidLog_ID,
                     [Item],
                     [RapidAmount],
                     [Bidder],
                     Cast(CONVERT(VARCHAR(20), [TimeStamp], 120) AS DATETIME) AS Timestamp,
                     [Location],
                     [BranchNumber],
                     [AuctionDatetime],
                     AuctionLane
              FROM   dbo.RapidBidLog AS RBL WITH (NOLOCK)
              GROUP  BY [Item],
                        [RapidAmount],
                        [Bidder],
                        Cast(CONVERT(VARCHAR(20), [TimeStamp], 120) AS DATETIME),
                        [Location],
                        [BranchNumber],
                        [AuctionDatetime],
                        AuctionLane) RBL
             INNER JOIN #TMP_Auction_Schedule_Source TASS WITH (NOLOCK)
               ON CONVERT(CHAR(8), RBL.AuctionDatetime, 112)=TASS.Auction_Day_ID
                  AND RBL.Item=TASS.Auction_Item_Sequence_Number
                  AND RBL.BranchNumber = TASS.BranchNumber
             LEFT OUTER JOIN #TMP_Bidder_List AS Bidder WITH (NOLOCK)
               ON Bidder.Bidder_Display_Name=RBL.Bidder
      WHERE  Isnumeric(Isnull(RBL.Item, 0))=1

      --         AND Dateadd(hh, -1, RBL.TimeStamp)>=Dateadd(D, -3, @Last_Run_Start)

      BEGIN TRAN

      INSERT INTO dbo.DimRapidBidDetail
                  (RapidBidDatetime,RapidBidDayID,RapidBidAmount,RapidBidDisplaylocation,RapidBidDisplayname,AuctionItemID,BuyerID,
                   BuyerEmployeeID,ETLLoadID,ETLLoadUpdateID)
      SELECT RBDS.Rapid_Bid_Datetime,
             RBDS.Rapid_Bid_Day_ID,
             RBDS.Rapid_Bid_Amount,
             RBDS.Rapid_Bid_Displaylocation,
             RBDS.Rapid_Bid_Displayname,
             RBDS.Auction_Item_ID,
             RBDS.Buyer_ID,
             RBDS.Buyer_Employee_ID,
             @ETLLoadID_sp,
             @ETLLoadID_sp
      FROM   #TMP_Rapid_Bid_Detail_Source RBDS WITH (NOLOCK)
             LEFT OUTER JOIN dbo.DimRapidBidDetail AS RBD WITH (NOLOCK)
               ON RBDS.Auction_Item_ID=RBD.AuctionItemID
                  AND RBDS.Rapid_Bid_Datetime=RBD.RapidBidDatetime
                  AND RBDS.Rapid_Bid_Displayname=RBD.RapidBidDisplayname
                  AND RBDS.Rapid_Bid_Amount=RBD.RapidBidAmount
      WHERE  RBD.RapidBidDetailID IS NULL

      SELECT @InsertRowCount_sp=Count(*)
      FROM   #temp_RecordCount
      WHERE  ChangeType='INSERT'

      COMMIT TRAN

      EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
      @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp

      IF Object_id('tempdb..#TMP_Auction_Schedule_Source') IS NOT NULL DROP TABLE #TMP_Auction_Schedule_Source
      IF Object_id('tempdb..#TMP_Bidder_List') IS NOT NULL DROP TABLE #TMP_Bidder_List
      IF Object_id('tempdb..#TMP_Rapid_Bid_Detail_Source') IS NOT NULL DROP TABLE #TMP_Rapid_Bid_Detail_Source
      IF Object_id('tempdb..#temp_RecordCount') IS NOT NULL DROP TABLE #temp_RecordCount

      RETURN 0
  END TRY

  BEGIN CATCH
      SET NOCOUNT OFF

      IF @@TRANCOUNT>0
        ROLLBACK TRAN

      EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
      @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp,@ErrorCode = @@ERROR

      DECLARE @ErrorMessage  NVARCHAR(4000),
              @ErrorSeverity INT,
              @ErrorState    INT;

      SELECT @ErrorMessage=Error_message(),
             @ErrorSeverity=Error_severity(),
             @ErrorState=Error_state();

      RAISERROR (@ErrorMessage,
                 @ErrorSeverity,
                 @ErrorState);

      IF Object_id('tempdb..#TMP_Auction_Schedule_Source') IS NOT NULL DROP TABLE #TMP_Auction_Schedule_Source
      IF Object_id('tempdb..#TMP_Bidder_List') IS NOT NULL DROP TABLE #TMP_Bidder_List
      IF Object_id('tempdb..#TMP_Rapid_Bid_Detail_Source') IS NOT NULL DROP TABLE #TMP_Rapid_Bid_Detail_Source
      IF Object_id('tempdb..#temp_RecordCount') IS NOT NULL  DROP TABLE #temp_RecordCount

      RETURN -1
  END CATCH
