CREATE PROCEDURE [dbo].[usp_ETLLKStockID]
/*
USE EDW
GO 
==============================================================================
Author		: Venkata Vempali
Create date	: 02/27/2012
Description	: Loads StockIds.
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	
03/01/2012    VV                Updated the Where Clause condition : 
									Added Sold Date ,Cancel Date ,Settlement Date
04/20/2012	Juan X. Jacome		-- Update logic for @Ind = 0
03/29/2013    VV                Added logic for Dim Time Cleassing.
04/11/2013    VV                Updated the Dim Time Cleansing logic.
5/21/2013	Matt Stelter		Changed to read year from SSISConfigTable & to keep StockID = 1000000
6/2/2015	Juan X. Jacome		Added with(Nolock) to INSERT INTO #TMP_TransactionDayID section
==============================================================================
Usage Example:
--Exec usp_ETLLKStockID
==============================================================================
*/


AS
BEGIN
SET NOCOUNT ON;

BEGIN TRY

DECLARE @Ind int, @Years int ,@Dateid int 

--Date cleansing
DECLARE @StartDayid int ,@EndDayid int
SELECT @StartDayid = ConfiguredValue FROM msdb.dbo.SSISConfigurations WHERE ConfigurationFilter = 'DimDayStartDayID' --20050101
SELECT @EndDayid = ConfiguredValue FROM msdb.dbo.SSISConfigurations WHERE ConfigurationFilter = 'DimDayEndDayID' --20050101

--IF OBJECT_ID('tempdb..#TMP_LKStock_ID') IS NOT NULL DROP TABLE #TMP_LKStock_ID
--CREATE TABLE #TMP_LKStock_ID(StockID [int] NULL)


IF OBJECT_ID('tempdb..#TMP_AuctionDayID_StockID') IS NOT NULL DROP TABLE #TMP_AuctionDayID_StockID
CREATE TABLE #TMP_AuctionDayID_StockID(StockID [int] NULL)

IF OBJECT_ID('tempdb..#TMP_Stock_StockID') IS NOT NULL DROP TABLE #TMP_Stock_StockID
CREATE TABLE #TMP_Stock_StockID(StockID [int] NULL)

IF OBJECT_ID('tempdb..#TMP_ReleaseProblemStartDayID') IS NOT NULL DROP TABLE #TMP_ReleaseProblemStartDayID
CREATE TABLE #TMP_ReleaseProblemStartDayID(StockID [int] NULL)

IF OBJECT_ID('tempdb..#TMP_AssignedDayID') IS NOT NULL DROP TABLE #TMP_AssignedDayID
CREATE TABLE #TMP_AssignedDayID(StockID [int] NULL)

IF OBJECT_ID('tempdb..#TMP_TowCompletedDayID') IS NOT NULL DROP TABLE #TMP_TowCompletedDayID
CREATE TABLE #TMP_TowCompletedDayID(StockID [int] NULL)

IF OBJECT_ID('tempdb..#TMP_TransactionDayID') IS NOT NULL DROP TABLE #TMP_TransactionDayID
CREATE TABLE #TMP_TransactionDayID(StockID [int] NULL ,TransactionDayID [int] NULL)


IF OBJECT_ID('tempdb..#TMP_TransactionDayID_Delete') IS NOT NULL DROP TABLE #TMP_TransactionDayID_Delete
CREATE TABLE #TMP_TransactionDayID_Delete(StockID [int] NULL)


   TRUNCATE TABLE [dbo].[LKStockID]
    
   SELECT @Ind = ConfiguredValue FROM [msdb].[dbo].[SSISConfigurations] WITH(NOLOCK) 
   WHERE ConfigurationFilter = 'EDWStockPresentInASAP'
   
   SELECT @Years = ConfiguredValue FROM [msdb].[dbo].[SSISConfigurations] WITH(NOLOCK) 
   WHERE ConfigurationFilter = 'EDWStockRetentionPeriod'
		 
   SELECT @Dateid =  CONVERT(INT, CONVERT(CHAR(8),  DATEADD(YEAR, DATEDIFF(YEAR, 0,DATEADD(YEAR, -(@Years-1), GETDATE())), 0) , 112))

 
   IF  @Ind = 1 -- Stocks Present in ASAP & in EDW based on the retention period
	   BEGIN

		    INSERT INTO dbo.LKStockID (StockID)
			SELECT DS.StockID FROM dbo.DimStock DS WITH(NOLOCK) 
				INNER JOIN BI_ASAP_Rep.dbo.Salvage S WITH(NOLOCK)
					ON DS.ASAPSalvageID = S.Salvage_ID
				INNER JOIN dbo.FactStockCycleTime FSCT WITH(NOLOCK)
					ON FSCT.StockID = DS.StockID
				WHERE FSCT.AssignmentDayID >= @Dateid 
			--UNION 
			--SELECT 1000000 AS StockID	--Default StockID
		END
	ELSE IF @Ind = 0 --Stocks present in ASAP & From EDW based on the retention period
		BEGIN
		
		  INSERT INTO dbo.LKStockID (StockID)
		  SELECT StockID 
		  FROM dbo.FactStockCycleTime WITH(NOLOCK) 
		  WHERE SoldDayID >= @Dateid 
		  AND   CancelDayID >= @Dateid 
		  
		  --UNION
		  --SELECT 1000000 AS StockID	--Default StockID
		  
		END
				
		INSERT INTO #TMP_AuctionDayID_StockID(StockID)
		SELECT LK.StockID
		--,CAST(CONVERT(CHAR(8),DA.AuctionDateTime,112) AS INT) AS AuctionDayID 
		FROM LKStockID AS LK WITH (NOLOCK)
			INNER JOIN DimAuctionItem AS DAI WITH (NOLOCK)
				ON DAI.StockID=LK.StockID
			INNER JOIN dbo.DimAuction DA WITH (NOLOCK)
				ON DAI.AuctionID = DA.AuctionID
		WHERE ((CAST(CONVERT(CHAR(8),DA.AuctionDateTime,112) AS INT) < @StartDayid OR CAST(CONVERT(CHAR(8),DA.AuctionDateTime,112) AS INT) > @EndDayid)
					AND CAST(CONVERT(CHAR(8),DA.AuctionDateTime,112) AS INT) <> 19000101 AND CAST(CONVERT(CHAR(8),DA.AuctionDateTime,112) AS INT) <> 29991231)
					
		DELETE 
		FROM LKStockID 
		WHERE StockID IN (SELECT DISTINCT StockID FROM #TMP_AuctionDayID_StockID)
					
		INSERT INTO #TMP_Stock_StockID(StockID)
		SELECT LK.StockID
		--,FSCT.ProviderSoldDayID
		--,FSCT.AssignmentDayID
		--,FSCT.SoldDayID
		FROM LKStockID AS LK WITH (NOLOCK)
			INNER JOIN FactStockCycleTime FSCT WITH (NOLOCK)
				ON FSCT.StockID = LK.StockID 
		WHERE ((FSCT.ProviderSoldDayID < @StartDayid OR FSCT.ProviderSoldDayID > @EndDayid) AND FSCT.ProviderSoldDayID <> 19000101 AND FSCT.ProviderSoldDayID <> 29991231)
			OR ((FSCT.AssignmentDayID < @StartDayid OR FSCT.AssignmentDayID > @EndDayid) AND FSCT.AssignmentDayID <> 19000101 AND FSCT.AssignmentDayID <> 29991231)
			OR ((FSCT.SoldDayID < @StartDayid OR FSCT.SoldDayID > @EndDayid) AND FSCT.SoldDayID <> 19000101 AND FSCT.SoldDayID <> 29991231)
		
		
		DELETE 
		FROM LKStockID 
		WHERE StockID IN (SELECT DISTINCT StockID FROM #TMP_Stock_StockID)	


		INSERT INTO #TMP_ReleaseProblemStartDayID(StockID)
		SELECT LK.StockID
		--,FRP.ReleaseProblemStartDayID    
		FROM LKStockID AS LK WITH (NOLOCK)
			INNER JOIN FactReleaseProblem FRP WITH (NOLOCK)
				ON FRP.StockID = LK.StockID	
		WHERE ((FRP.ReleaseProblemStartDayID < @StartDayid OR FRP.ReleaseProblemStartDayID > @EndDayid) 
					AND FRP.ReleaseProblemStartDayID <> 19000101 AND FRP.ReleaseProblemStartDayID <> 29991231)


		DELETE 
		FROM LKStockID 
		WHERE StockID IN (SELECT DISTINCT StockID FROM #TMP_ReleaseProblemStartDayID)	



		INSERT INTO #TMP_AssignedDayID(StockID)
		SELECT LK.StockID
		--,FSA.AssignedDayID
		FROM LKStockID AS LK WITH (NOLOCK)
			INNER JOIN FactAssignment FSA WITH (NOLOCK)
				ON FSA.StockID = LK.StockID
		WHERE ((FSA.AssignedDayID < @StartDayid OR FSA.AssignedDayID > @EndDayid) 
					AND FSA.AssignedDayID <> 19000101 AND FSA.AssignedDayID <> 29991231) 
					
		DELETE 
		FROM LKStockID 
		WHERE StockID IN (SELECT DISTINCT StockID FROM #TMP_AssignedDayID)	
					


		INSERT INTO #TMP_TowCompletedDayID(StockID)
		SELECT DISTINCT LK.StockID
		--,ISNULL(FSTCT.TowCompletedDayID,29991231) AS TowCompletedDayID
		FROM LKStockID AS LK WITH (NOLOCK)
			INNER JOIN FactStockTowCycleTime FSTCT WITH (Nolock)
				ON FSTCT.StockID = LK.StockID
		WHERE ((FSTCT.TowCompletedDayID < @StartDayid OR FSTCT.TowCompletedDayID > @EndDayid) 
					AND FSTCT.TowCompletedDayID <> 19000101 AND ISNULL(FSTCT.TowCompletedDayID,29991231) <> 29991231) 



		DELETE 
		FROM LKStockID 
		WHERE StockID IN (SELECT DISTINCT StockID FROM #TMP_TowCompletedDayID)	
	
		INSERT INTO #TMP_TransactionDayID(StockID ,TransactionDayID)
		SELECT  StockID ,TransactionDayID
		FROM FactFinancialTransaction  WITH (NOLOCK)
		WHERE StockID IN (SELECT StockID FROM LKStockID WITH(NOLOCK))


		CREATE NONCLUSTERED INDEX [IX_TransactionDayID] ON #TMP_TransactionDayID
		(TransactionDayID ASC
		)

		INSERT INTO #TMP_TransactionDayID_Delete(StockID)
		SELECT DISTINCT FFT.StockID 
		FROM #TMP_TransactionDayID  FFT WITH (NOLOCK)
		WHERE ((FFT.TransactionDayID < @StartDayid OR FFT.TransactionDayID > @EndDayid) 
					AND FFT.TransactionDayID <> 19000101 AND FFT.TransactionDayID <> 29991231)



		DELETE 
		FROM LKStockID 
		WHERE StockID IN (SELECT DISTINCT StockID FROM #TMP_TransactionDayID_Delete)	

	DELETE FROM LKStockID WHERE StockID = 1000000
	INSERT INTO dbo.LKStockID (StockID) SELECT 1000000 AS StockID	--Default StockID

    RETURN 0
 
END TRY

BEGIN CATCH

    RETURN -1
    
END CATCH



END;