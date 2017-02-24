/*
USE EDW
GO 
==============================================================================
Author		: Somesh Velupalli 
Create date	: 09/18/2011
Description	: Populate the EDW DimSoldDetail table from Auction Log tables.
==============================================================================
REVISION History
ChangeDate	     Developer	    	Release/Problem Number
9/26/2012        Somesh V           Removed AuctionID,AuctionLane,StockID,ProviderID,BranchNumber 
                                    and added AuctionItemID
1/24/2013		Matt Stelter		Corrected join to AuctionItem table  
4/17/2014		Matt Stelter		Added RingRep                                    
==============================================================================
Usage Example:
Exec usp_ETLDimSoldDetail
SELECT TOP 100 * from dbo.DimSoldDetail order by 1 desc
SELECT TOP 100 * from dbo.Soldlog order by 1 desc
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimSoldDetail]
AS
  DECLARE @ExtractRowCount_sp INT,
          @ETLLoadID_sp       INT,
          @InsertRowCount_sp  INT,
          @UpdateRowCount_sp  INT,
          @CurrentDatetime    DATETIME

  SET NOCOUNT ON;

  BEGIN TRY
      --Run the Audit Begin
      EXEC @ETLLoadID_sp = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimSoldDetail'

      SET @CurrentDatetime = (SELECT ER.MaxDatetime
                              FROM   dbo.ETLRun ER WITH (NOLOCK)
                              WHERE  ETLJobName='DailyETLJob')

/*******************CREATE TEMP TABLES******************************/
      IF Object_id('tempdb..#TMP_Bidder_List') IS NOT NULL DROP TABLE #TMP_Bidder_List
      CREATE TABLE #TMP_Bidder_List
        (TMP_Bidder_List_ID  INT IDENTITY(1, 1) NOT FOR REPLICATION NOT NULL,
         Buyer_Employee_ID   INT,
         Buyer_ID            INT,
         Bidder_Display_Name VARCHAR(100))


      --IF Object_id('tempdb..#TMP_Max_Sold_Event') IS NOT NULL DROP TABLE #TMP_Max_Sold_Event
      --CREATE TABLE #TMP_Max_Sold_Event
      --  (Item                   INT,
      --   Auction_Item_ID INT,
      --   Max_Sold_Datetime      DATETIME)

      --IF Object_id('tempdb..#TMP_Max_Sold_Event') IS NOT NULL DROP TABLE #TMP_Max_Sold_Event
      --CREATE TABLE #TMP_Max_Sold_Event
      --  (SoldLogID                   INT,
      --   Max_Sold_Datetime      DATETIME) 
         
      IF Object_id('tempdb..#TMP_Auction_Source') IS NOT NULL DROP TABLE #TMP_Auction_Source
      CREATE TABLE #TMP_Auction_Source
        ([Auction_Item_ID]                   [INT],
         [Auction_DateTime]             [DATETIME],
         [Auction_Day_ID]               [CHAR] (8),
         [Auction_Item_Sequence_Number] [VARCHAR] (80),
         [BranchNumber] [INT],
         AuctionLane VARCHAR(1))
         

      IF Object_id('tempdb..#TMP_Sold_Detail_Source') IS NOT NULL DROP TABLE #TMP_Sold_Detail_Source
      CREATE TABLE #TMP_Sold_Detail_Source
        ([Sold_Log_Item]       [INT],
         [Sale_Amount]         [MONEY],
         [Auction_Item_ID]          [INT],
         [Sale_Event_Datetime] [DATETIME],
         [Sale_Event_Day_ID]   [CHAR] (8),
         [Sale_Status_Code]    [NCHAR] (1),
         [Bidder_Type]         [NCHAR] (1),
         [Bidder]              [NVARCHAR] (50),
         [Buyer_ID]            [INT],
         [Sold_Log_Row_ID]     [INT],
		 [RingRep]				[NVARCHAR](50))
         
      IF Object_id('tempdb..#TMP_Sold_Detail') IS NOT NULL  DROP TABLE #TMP_Sold_Detail
      CREATE TABLE #TMP_Sold_Detail
        ([Sold_Log_Item]       [INT],
         [Sale_Amount]         [MONEY],
         [Auction_Item_ID]          [INT],
         [Sale_Event_Datetime] [DATETIME],
         [Sale_Event_Day_ID]   [CHAR] (8),
         [Sale_Status_Code]    [NCHAR],
         [Buyer_Employee_ID]   [INT],
         [Bidder_Type]         [NCHAR] (1),
         [Buyer_ID]            [INT],
         [Sold_Log_Row_ID]     [INT],
		 [RingRep]				[NVARCHAR](50))   
         
 
      CREATE TABLE #temp_RecordCount
        (ChangeType VARCHAR(10))
                      
/*********************************************************************/

      INSERT INTO #TMP_Bidder_List
                  (Buyer_Employee_ID,Buyer_ID,Bidder_Display_Name)
      SELECT BE.BuyerEmployeeID,
             BE.BuyerID,
             BE.BidderDisplayName
      FROM   dbo.DimBuyerEmployee AS BE WITH (NOLOCK)
      WHERE  BE.CurrentStatusCode='ACT'

      --INSERT INTO #TMP_Max_Sold_Event
      --            (Item,Auction_Item_ID,Max_Sold_Datetime)
      --SELECT SL.Item,
      --       ASCH.AuctionItemID,
      --       Max(Dateadd(hh, -1, SL.TimeStamp)) AS Max_Sold_Datetime
      --FROM   dbo.SoldLog AS SL WITH (NOLOCK)
      --       INNER JOIN dbo.DimAuctionItem AS ASCH WITH (NOLOCK)
      --         ON CONVERT(CHAR(8), Dateadd(hh, -1, SL.TimeStamp), 112)=CONVERT(CHAR(8), ASCH.AuctionDateTime, 112)
      --       INNER JOIN dbo.DimAuctionItem AS AI WITH (NOLOCK)
      --         ON ASCH.AuctionItemID=AI.AuctionItemID
      --            AND SL.Item=Cast(AI.AuctionItemSequenceNumber AS VARCHAR(80))
      --WHERE DATEADD(hh,-1,SL.TimeStamp) >= DATEADD(D,-3,@CurrentDatetime)
      --GROUP  BY SL.Item,
      --          ASCH.AuctionItemID

	--INSERT INTO #TMP_Max_Sold_Event
	--			(SoldLogId,
	--			 Max_Sold_Datetime)
	--SELECT SoldLogId,
	--	   Max_Sold_Datetime
	--FROM   dbo.SoldLog SL WITH (NOLOCK)
	--	   INNER JOIN(SELECT Item,
	--						 BranchNumber,
	--						 AuctionLane,
	--						 Max(SL.TimeStamp) AS Max_Sold_Datetime
	--				  FROM   dbo.SoldLog SL WITH (NOLOCK)
	--				  WHERE  Dateadd(hh, -1, SL.TimeStamp)>=Dateadd(D, -3, @CurrentDatetime)
	--				  GROUP  BY Item,
	--							BranchNumber,
	--							AuctionLane) MD
	--		 ON SL.Item=MD.Item
	--			AND SL.BranchNumber=MD.BranchNumber
	--			AND SL.AuctionLane=MD.AuctionLane
	--			AND SL.TimeStamp=Max_Sold_Datetime 

	
      INSERT INTO #TMP_Auction_Source
                  (Auction_Item_ID,Auction_DateTime,Auction_Day_ID,Auction_Item_Sequence_Number,BranchNumber,AuctionLane)
      SELECT ASCH.AuctionItemID,
             ASCH.AuctionDateTime,
             CONVERT(CHAR(8), ASCH.AuctionDateTime, 112) AS Auction_Day_ID,
             CAST(ASCH.AuctionItemSequenceNumber AS VARCHAR(80)) AS Auction_Item_Sequence_Number,
             DA.BranchNumber,
             ASCH.AuctionLane
      FROM   dbo.DimAuctionItem AS ASCH WITH (NOLOCK)
      INNER JOIN EDW.dbo.DimAuction DA WITH (NOLOCK)
		ON ASCH.AuctionID = DA.AuctionID
      WHERE  ASCH.AuctionDateTime>=Dateadd(D, -3, @CurrentDatetime)


      INSERT INTO #TMP_Sold_Detail_Source
                  (Sold_Log_Item,Sale_Amount,Auction_Item_ID,Sale_Event_Datetime,Sale_Event_Day_ID,Sale_Status_Code,Bidder_Type,Bidder,
                   Buyer_ID,
                   Sold_Log_Row_ID,RingRep)
      SELECT DISTINCT SL.Item AS Sold_Log_Item,
                      SL.Amount AS Sale_Amount,
                      TAS.Auction_Item_ID,
                      Dateadd(hh, -1, SL.TimeStamp) AS Sale_Event_Datetime,
                      CONVERT(CHAR(8), Dateadd(hh, -1, SL.TimeStamp), 112) AS Sale_Event_Day_ID,
                      SL.Status AS Sale_Status_Code,
                      SL.BidderType AS Bidder_Type,
                      SL.Bidder,
                      STK.CurrentBuyerID,
                      SL.RowID AS Sold_Log_Row_ID
					  ,SL.RingRep
      FROM   dbo.SoldLog AS SL WITH (NOLOCK)
             INNER JOIN #TMP_Auction_Source AS TAS
               ON CONVERT(CHAR(8), Dateadd(hh, -1, SL.TimeStamp), 112)=TAS.Auction_Day_ID
                  AND SL.Item=TAS.Auction_Item_Sequence_Number
                  AND SL.BranchNumber = TAS.BranchNumber
                  AND SL.AuctionLane = TAS.AuctionLane
             INNER JOIN dbo.DimAuctionItem DAI WITH (NOLOCK)
                  ON TAS.Auction_Item_ID = DAI.AuctionItemID
             INNER JOIN dbo.DimStock AS STK WITH (NOLOCK)
               ON DAI.StockID=STK.StockID
             INNER JOIN dbo.FactStockCycleTime AS CSKDA WITH (NOLOCK)
               ON STK.StockID=CSKDA.StockID
             --INNER JOIN #TMP_Max_Sold_Event AS MSE
             --  ON MSE.Auction_Item_ID = DAI.AuctionItemID
             --     AND SL.Item=MSE.Item
             --     AND Dateadd(hh, -1, SL.TimeStamp)=MSE.Max_Sold_Datetime
    --         INNER JOIN #TMP_Max_Sold_Event MSE WITH (NOLOCK)
				--ON MSE.SoldLogID = SL.SoldLogId
      WHERE  Dateadd(hh, -1, SL.TimeStamp)>=Dateadd(D, -3, @CurrentDatetime)


      INSERT INTO #TMP_Sold_Detail
                  (Sold_Log_Item,Sale_Amount,Auction_Item_ID,Sale_Event_Datetime,Sale_Event_Day_ID,Sale_Status_Code,Buyer_Employee_ID,Bidder_Type,
                   Buyer_ID,Sold_Log_Row_ID,RingRep)
      SELECT TSDS.Sold_Log_Item,
             TSDS.Sale_Amount,
             TSDS.Auction_Item_ID,
             TSDS.Sale_Event_Datetime,
             TSDS.Sale_Event_Day_ID,
             TSDS.Sale_Status_Code,
             Bidder.Buyer_Employee_ID,
             TSDS.Bidder_Type,
             TSDS.Buyer_ID,
             TSDS.Sold_Log_Row_ID
			 ,TSDS.RingRep
      FROM   #TMP_Sold_Detail_Source AS TSDS
             LEFT OUTER JOIN #TMP_Bidder_List AS Bidder WITH (NOLOCK)
               ON Bidder.Bidder_Display_Name=TSDS.Bidder
             LEFT OUTER JOIN dbo.DimSoldDetail AS SD WITH (NOLOCK)
               ON TSDS.Auction_Item_ID=SD.AuctionItemID
      WHERE  SD.SoldDetailID IS NULL


      BEGIN TRANSACTION

      INSERT INTO dbo.DimSoldDetail
                  (SoldLogItem,SaleAmount,AuctionItemID,SaleEventDatetime,SaleEventDayID,SaleStatusCode,BuyerEmployeeID,BidderType,BuyerID,
                    SoldLogRowID,ETLLoadID,ETLLoadUpdateID,RingRep)
      SELECT TSD.Sold_Log_Item,
             TSD.Sale_Amount,
             TSD.Auction_Item_ID,
             TSD.Sale_Event_Datetime,
             TSD.Sale_Event_Day_ID,
             TSD.Sale_Status_Code,
             TSD.Buyer_Employee_ID,
             TSD.Bidder_Type,
             TSD.Buyer_ID,
             TSD.Sold_Log_Row_ID,
             @ETLLoadID_sp,
             @ETLLoadID_sp,
			 RingRep
      FROM   #TMP_Sold_Detail AS TSD

      SELECT @InsertRowCount_sp=Count(*)
      FROM   #temp_RecordCount
      WHERE  ChangeType='INSERT'

      COMMIT TRAN

	  	DELETE FROM DSD
		FROM   [dbo].[DimSoldDetail] DSD WITH (NOLOCK)
			   INNER JOIN (SELECT Max(SaleEventDateTime) AS MAXSaleEventDateTime,
								  DSD.AuctionItemID -- DSD.*
						   FROM   [dbo].[DimSoldDetail] DSD WITH (NOLOCK)
								  INNER JOIN (SELECT [AuctionItemID],
													 Count(*) thecount
											  FROM   [dbo].[DimSoldDetail] WITH (NOLOCK)
											  GROUP  BY [AuctionItemID]
											  HAVING Count(*)>1) Dups
									ON DSD.AuctionItemID=Dups.AuctionItemID
						   GROUP  BY DSD.AuctionItemID) Dups
				 ON DSD.AuctionItemID=Dups.AuctionItemID
					AND DSD.SaleEventDatetime<Dups.MAXSaleEventDateTime 

      EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
      @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp

      IF Object_id('tempdb..#TMP_Bidder_List') IS NOT NULL DROP TABLE #TMP_Bidder_List
      IF Object_id('tempdb..#TMP_Sold_Detail_Source') IS NOT NULL DROP TABLE #TMP_Sold_Detail_Source
      IF Object_id('tempdb..#TMP_Sold_Detail') IS NOT NULL DROP TABLE #TMP_Sold_Detail
      IF Object_id('tempdb..#TMP_Auction_Source') IS NOT NULL DROP TABLE #TMP_Auction_Source
      IF Object_id('tempdb..#temp_RecordCount') IS NOT NULL  DROP TABLE #temp_RecordCount

      RETURN 0
  END TRY

  BEGIN CATCH
      SET NOCOUNT OFF

      IF @@TRANCOUNT>0
        ROLLBACK TRAN

      EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID_sp,@ExtractRowCount = @ExtractRowCount_sp,@InsertRowCount =
      @InsertRowCount_sp,@UpdateRowCount = @UpdateRowCount_sp,@ErrorCode = @@ERROR

      IF Object_id('tempdb..#TMP_Bidder_List') IS NOT NULL DROP TABLE #TMP_Bidder_List
      IF Object_id('tempdb..#TMP_Sold_Detail_Source') IS NOT NULL DROP TABLE #TMP_Sold_Detail_Source
      IF Object_id('tempdb..#TMP_Sold_Detail') IS NOT NULL DROP TABLE #TMP_Sold_Detail
      IF Object_id('tempdb..#TMP_Auction_Source') IS NOT NULL DROP TABLE #TMP_Auction_Source
      IF Object_id('tempdb..#temp_RecordCount') IS NOT NULL DROP TABLE #temp_RecordCount

      DECLARE @ErrorMessage  NVARCHAR(4000),
              @ErrorSeverity INT,
              @ErrorState    INT;

      SELECT @ErrorMessage=Error_message(),
             @ErrorSeverity=Error_severity(),
             @ErrorState=Error_state();

      RAISERROR (@ErrorMessage,
                 @ErrorSeverity,
                 @ErrorState);



      RETURN -1
  END CATCH
