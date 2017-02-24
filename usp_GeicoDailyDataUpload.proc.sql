/* 
USE EDW
GO 
--==============================================================================
--Author		: Venkata Vempali
--Create date	: 09/06/2012
--Description	: Loading data to a table,to export data into a flat file for Sold & Unsold vehicles for Geico
--==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	
09/18/2012    Venkata Vempali     Update Case statment for Black Book ACV.
09/18/2012    Venkata Vempali     Update Garage Address.
09/18/2012    Venkata Vempali     Use PickupDateTime as StockDropoffDatetime.
09/19/2012    Venkata Vempali     Defaulted Charges to NULL for UnSold Stocks.
09/19/2012    Venkata Vempali     AdvanceTotalExpense + AdvanceTaxExpense = [Advance Total Expenses] (BO it was included bydefault)
09/24/2012    Venkata Vempali     Updated Original Bid Amount.
09/26/2012    Venkata Vempali     Updated [Advance Total Expenses] (AdvanceTotalCharge + AdvanceTaxCharge)
09/26/2012    Venkata Vempali     Updated logic for [Provider Total Charge Excluding Other], [Provider Total Charges], [Provider Total Charges & Advance Charge Expense],
10/02/2012    Venkata Vempali     Updated Buyer Country case stmt.
--==============================================================================
--Usage Example:
--Exec usp_GeicoDailyDataUpload
--==============================================================================
*/


CREATE PROCEDURE [dbo].[usp_GeicoDailyDataUpload]

AS
BEGIN

  SET NOCOUNT ON
 
  DECLARE @date           VARCHAR(20),
          @previousday    DATETIME,
          @presentday     DATETIME,
          @day            DATETIME,
          @night          DATETIME,
          @GETDATE        DATETIME,
          @ETLLoadID      INT,
		  @ExtractRowCount INT,
		  @InsertRowCount INT,
		  @UpdateRowCount INT 
          
 
BEGIN TRY
  
  --Run the Audit begin
  EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_GeicoDailyDataUpload'
          

  SET @GETDATE = GETDATE()
	
  SET @day = DATEADD(HH, 00, CONVERT(CHAR(10), @GETDATE, 102))--SETS THE CURRENT DAY's TIME TO 12:00 A.M.
  SET @night = DATEADD(HH, 20, @day)--SETS THE CURRENT DAY's TIME TO 08:00 P.M.
  IF DATENAME(dw, @GETDATE) = 'Tuesday'
    BEGIN
        SET @previousday = CASE
                             WHEN @GETDATE >= @day
                                  AND @GETDATE < @night THEN @night - 4
                             WHEN @GETDATE >= @night
                                  AND @GETDATE < DATEADD(HH, 24, @day) THEN @night - 3
                           END
        SET @presentday = CASE
                            WHEN @GETDATE >= @day
                                 AND @GETDATE < @night THEN @night - 1
                            WHEN @GETDATE >= @night
                                 AND @GETDATE < DATEADD(HH, 24, @day) THEN @night
                          END
    END
  ELSE
    BEGIN
        SET @previousday = CASE
                             WHEN @GETDATE >= @day
                                  AND @GETDATE < @night THEN @night - 2
                             WHEN @GETDATE >= @night
                                  AND @GETDATE < DATEADD(HH, 24, @day) THEN @night - 1
                           END
        SET @presentday = CASE
                            WHEN @GETDATE >= @day
                                 AND @GETDATE < @night THEN @night - 1
                            WHEN @GETDATE >= @night
                                 AND @GETDATE < DATEADD(HH, 24, @day) THEN @night
                          END
    END

  SET @date = REPLACE(CONVERT(CHAR(10), @presentday, 101), '/', '')



--/*********************************************************************************************
--	Create the temp table Geico_Daily_Data_Upload
--*********************************************************************************************/


	IF OBJECT_ID('tempdb..#TMP_Prebill') IS NOT NULL DROP TABLE #TMP_Prebill
	CREATE TABLE #TMP_Prebill(
		 StockID INT
		,PrebillSettlementDate DATETIME)

	INSERT INTO #TMP_Prebill(StockID ,PrebillSettlementDate)
	SELECT DS.StockID ,FSE.EventDateTime AS Prebill_Settlement_DateTime
	FROM dbo.FactStockEvent FSE WITH(NOLOCK)
		INNER JOIN dbo.DimStock DS WITH(NOLOCK)
			ON FSE.ASAPSalvageID = DS.ASAPSalvageID
		INNER JOIN dbo.DimProvider DP WITH(NOLOCK)
			ON DS.CurrentProviderID = DP.ProviderID
	WHERE FSE.EventTypeID = (SELECT DSET.EventTypeID FROM dbo.DimStockEventType DSET WHERE dset.EventCode = 'FPBE')
			AND DP.ProviderGroupName IN ( 'Berkshire Hathaway' )
			AND FSE.IsMostRecentEvent = 1
			AND FSE.EventDateTime BETWEEN @previousday AND @presentday



	IF OBJECT_ID('tempdb..#TMP_StocksForDay') IS NOT NULL  DROP TABLE #TMP_StocksForDay

	CREATE TABLE #TMP_StocksForDay(StockID INT)

	INSERT INTO #TMP_StocksForDay(StockID)
	SELECT FSCT.StockID 
	FROM dbo.FactStockCycleTime FSCT WITH(NOLOCK)
		 INNER JOIN dbo.DimStock DS WITH(NOLOCK)
			ON FSCT.StockID = DS.StockID 
		 INNER JOIN dbo.DimProvider DP WITH(NOLOCK)
			ON DS.CurrentProviderID = DP.ProviderID
		 LEFT OUTER JOIN #TMP_Prebill TPB 
			ON TPB.StockID = FSCT.StockID
	WHERE DP.ProviderGroupName = 'Berkshire Hathaway'
			AND COALESCE(TPB.PrebillSettlementDate,FSCT.SettlementDateTime)  BETWEEN @previousday AND @presentday	
			
			
	IF OBJECT_ID('tempdb..#TMP_NCPR_Date') IS NOT NULL  DROP TABLE #TMP_NCPR_Date
	CREATE TABLE #TMP_NCPR_Date(
		 StockID INT
		,NCPRDate DATETIME)

	INSERT INTO #TMP_NCPR_Date(StockID ,NCPRDate)
	SELECT DS.StockID , FSE.EventDateTime AS NCPRDate
	FROM dbo.DimStock DS WITH(NOLOCK)
		  INNER JOIN #TMP_StocksForDay TSFD WITH(NOLOCK)
			ON DS.StockID = TSFD.StockID
		  INNER JOIN dbo.FactStockEvent FSE WITH(NOLOCK)
			ON DS.ASAPSalvageID = FSE.ASAPSalvageID
	WHERE FSE.IsMostRecentEvent = 1
			AND FSE.EventTypeID = (SELECT DSET.EventTypeID FROM dbo.DimStockEventType DSET WHERE dset.EventCode = 'NCPR')	 
		



	IF OBJECT_ID('tempdb..#TMP_Provider_QR') IS NOT NULL  DROP TABLE #TMP_Provider_QR

	CREATE TABLE #TMP_Provider_QR(
		StockID INT,
		QuestionID INT,
		ProviderID INT,
		ResponseValue VARCHAR(40))


	INSERT INTO #TMP_Provider_QR(StockID ,ProviderID ,QuestionID ,ResponseValue)
	SELECT T.StockID ,FPQR.ProviderID ,FPQR.QuestionID ,FPQR.ResponseValue
	FROM dbo.FactProviderQuestionResponse FPQR  WITH (NOLOCK)
		INNER JOIN 
			(   SELECT  Max(FPQR.ProviderQuestionResponseID) AS ProviderQuestionResponseID ,FPQR.StockID 
				FROM dbo.FactProviderQuestionResponse FPQR  WITH (NOLOCK)
					LEFT OUTER JOIN dbo.DimProviderQuestion DPQC WITH (NOLOCK)
						ON DPQC.QuestionID = FPQR.QuestionID AND DPQC.ProviderID = FPQR.ProviderID
					LEFT OUTER JOIN dbo.DimProviderQuestion DPQP WITH (NOLOCK) 
						ON DPQP.QuestionID = FPQR.QuestionID AND DPQP.ProviderID = FPQR.ParentProviderID 
					INNER JOIN #TMP_StocksForDay DS WITH (NOLOCK) 
						ON (DS.StockID = FPQR.StockID)
				WHERE FPQR.QuestionID = 30
					  AND FPQR.ProviderID IN (SELECT ProviderID FROM dbo.DimProvider DP WITH(NOLOCK)
												  WHERE  DP.ProviderGroupName IN ( 'Berkshire Hathaway' ))
				GROUP BY FPQR.StockID) T
		  ON FPQR.ProviderQuestionResponseID = T.ProviderQuestionResponseID





	IF OBJECT_ID('tempdb..#TMP_Bid_Amount') IS NOT NULL DROP TABLE #TMP_Bid_Amount
	CREATE TABLE #TMP_Bid_Amount(
	   StockID INT,
	   Historical_High_Bid_Amount INT,
	   High_Bid_1st_Run INT,
	   High_Bid_2nd_Run INT,
	   High_Bid_3rd_Run INT)	

	INSERT INTO #TMP_Bid_Amount (StockID, Historical_High_Bid_Amount, 
							   High_Bid_1st_Run, High_Bid_2nd_Run, High_Bid_3rd_Run)
	SELECT DAI.StockID,
		 MAX(CASE WHEN DAI.IsProviderAuction = 1 
					THEN DAI.FinalBid
				END)AS [Historical_High_Bid_Amount]
		,MAX (CASE WHEN  DAI.AuctionSequenceNumber = 1 
					THEN CASE WHEN DAI.IsProviderAuction = 1
	    					   THEN DAI.FinalBid
							  WHEN DAI.IsProviderAuction = 0
								THEN 0 
							END	
				END) AS [High_Bid_1st_Run] 
		,MAX(CASE WHEN  DAI.AuctionSequenceNumber = 2
					THEN CASE WHEN DAI.IsProviderAuction = 1
	    					   THEN DAI.FinalBid	
							  WHEN DAI.IsProviderAuction = 0
							   THEN 0 
							END	
				END) AS [High_Bid_2nd_Run]
		,MAX(CASE WHEN  DAI.AuctionSequenceNumber = 3 
					THEN CASE WHEN DAI.IsProviderAuction = 1
	    					   THEN DAI.FinalBid	
							  WHEN DAI.IsProviderAuction = 0
							   THEN 0 
							END	
				END) AS [High_Bid_3rd_Run]
	FROM dbo.DimAuctionItem DAI WITH(NOLOCK)
		INNER JOIN #TMP_StocksForDay TS WITH(NOLOCK)
			ON DAI.StockID = TS.StockID 
	GROUP BY DAI.StockID


BEGIN TRANSACTION  

	
	   /****************************************************************************************************************
      				INSERTING SOLD & UNSOLD VEHICLES DATA INTO THE TABLE - BEGIN
      ****************************************************************************************************************/
      
      
      
      TRUNCATE TABLE dbo.GeicoDailyDataUpload
      
     
      INSERT INTO dbo.GeicoDailyDataUpload
                  ([Provider Office Name],
                   [Stock Number],
                   [Model Year],
                   [Make],
                   [Model Group],
                   [Model],
                   [Car Country of Origin],
                   [Full Body Style],
                   [Color of Car],
                   [VIN],
                   [Vehicle Type],
                   [Vehicle Category],
                   [Loss Type Description],
                   [Primary Damage Description],
                   [With Keys Ind],
                   [Assignment Type],
                   [Stock Status Ind],
                   [VIC Ind],
                   [Express Stock Pickup Ind],
                   [Mileage],
                   [Odometer Type],
                   [ACV],
                   [Black Book ACV],
                   [ECR],
                   [Branch Number],
                   [Branch Name],
                   [Branch Address Line 1v],
                   [Branch Address Line 2],
                   [Insured Last Name],
                   [Insured First Name],
                   [Branch City],
                   [Branch ZIP Code],
                   [Branch County],
                   [Branch State],
                   [Claim Number],
                   [IP Number],
                   [Owner Last Name],
                   [Owner First Name],
                   [Loss Date],
                   [Assignment Date],
                   [Release Date],
                   [Pickup Date],
                   [Dispatch Date],
                   [Stock Drop Off Date],
                   [Title In Date],
                   [Received All Doc Date],
                   [Cert In Date],
                   [Auction Date],
                   [Settlement Date],
                   [Buyer Payment Date],
                   [Buyer Pickup Date],
                   [Garage Name],
                   [Garage Address],
                   [Garage City],
                   [Garage State],
                   [Garage ZipCode],
                   [Garage County],
                   [Advance Tow Charge],
                   [Advance Storage Charge],
                   [Advacne Other Charge],
                   [Advance Tax Charges],
                   [Advance Total Expenses],
                   [Provider Total Charge Excluding Other],
                   [Provider Commission Charge],
                   [Provider Enhancement Charge],
                   [Provider Title Charge],
                   [Provider Storage Charge],
                   [Provider Tow Charge],
                   [Provider Other Charge],
                   [Provider Total Charges],
                   [Provider Total Charges & Advance Charge Expense],
                   [Deficit],
                   [Net Rerun],
                   [Title Type],
                   [Title Type Category],
                   [# Reruns],
                   [Call Car Ind],
                   [Minimum Bid Amount],
                   [Hold Indicator],
                   [Sold Date],
                   [Original Bid Amount],
                   [Gross Return],
                   [ASAP Buyer ID],
                   [Buyer Name],
                   [Buyer Address Line 1],
                   [Buyer Address Line 2],
                   [Buyer City],
                   [Buyer State],
                   [Buyer ZipCode],
                   [Buyer Country],
                   [Agreement Term Type],
                   [Tow Miles],
                   [Run & Drive],
                   [Average Salvage Value],
                   [IBuy Fast Indicator],
                   [Historical High Bid Amount],
                   [Minimum Bid Category],
                   [Highest Bid 1st Run],
                   [Highest Bid 2nd Run],
                   [Highest Bid 3rd Run],
                   [Cancel Salvage Indicator],
                   [Cancel Reason Description],
                   [Sold/Unsold],
                   [Date Reported to NICB],
				   [Sales_Document_Applied_Datetime],
				   [Sale_Document_Received_DateTime])
	SELECT LTRIM(RTRIM(DP.ProviderName)) AS [Provider Office Name],
			LTRIM(RTRIM(DS.FullStockNumber)) AS [Stock Number],
			NULLIF(DS.ModelYear,0) AS [Model Year],
			CASE WHEN LTRIM(RTRIM(DS.MakeName)) = '' THEN NULL 
					ELSE LTRIM(RTRIM(DS.MakeName)) END AS [Make],
			NULL AS [Model Group],
			CASE WHEN LTRIM(RTRIM(DS.ModelName)) = '' THEN NULL 
					ELSE LTRIM(RTRIM(DS.ModelName)) END AS [Model],
			CASE WHEN LTRIM(RTRIM(DS.CountryOfManufacture)) = '' THEN NULL
					ELSE LTRIM(RTRIM(DS.CountryOfManufacture)) END AS [Car Country of Origin],
			CASE WHEN LTRIM(RTRIM(DS.FullBodyStyle)) = '' THEN NULL	
					ELSE LTRIM(RTRIM(DS.FullBodyStyle)) END AS [Full Body Style],
			CASE WHEN LTRIM(RTRIM(DS.ColorDescription)) = '' THEN NULL	
					ELSE LTRIM(RTRIM(DS.ColorDescription)) END AS [Color of Car],
			CASE WHEN LTRIM(RTRIM(DS.VIN)) = '' THEN NULL
					ELSE LTRIM(RTRIM(DS.VIN)) END AS [VIN],
			CASE WHEN LTRIM(RTRIM(DS.StockTypeDescription)) = '' THEN NULL
					ELSE LTRIM(RTRIM(DS.StockTypeDescription)) END AS [Vehicle Type],
			CASE WHEN LTRIM(RTRIM(DS.StockSegment)) = '' THEN NULL	
					ELSE LTRIM(RTRIM(DS.StockSegment)) END AS [Vehicle Category],
			LTRIM(RTRIM(DS.LossTypeDescription)) AS [Loss Type Description],
			CASE WHEN LTRIM(RTRIM(DS.DamageDescriptionPrimary)) = '' THEN NULL	
					ELSE LTRIM(RTRIM(DS.DamageDescriptionPrimary)) END AS [Primary Damage Description],
			DS.KeysRetainedCodeatSale AS [With Keys Ind],
			CASE WHEN LTRIM(RTRIM(DS.AssignmentTypeDescription)) = '' THEN NULL	
					ELSE LTRIM(RTRIM(DS.AssignmentTypeDescription)) END AS [Assignment Type],
			CASE WHEN LTRIM(RTRIM(DS.StockStatus)) = '' THEN NULL 
					ELSE LTRIM(RTRIM(DS.StockStatus)) END AS [Stock Status Ind],
			DS.IsVIC AS [VIC Ind],
			CASE WHEN DS.IsExpressPickupTow = 1 THEN 1 ELSE 0 END AS [Express Stock Pickup Ind],
			NULLIF(DS.Mileage,0) AS Mileage, 
			LTRIM(RTRIM(DS.OdometerReadingTypeDescription )) AS [Odometer Type],

			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL
					ELSE (NULLIF((CASE WHEN DS.IsACVOutlier = 0  THEN DS.ActualCashValue
							ELSE NULL END),0)) 
					END AS [ACV],  
			--NULLIF((CASE WHEN DS.IsStockSold = 0 THEN DS.ASAPBlackbookACV
			--				ELSE DS.BlackbookACV END),0) AS [Black Book ACV],  
			NULLIF(DS.BlackbookACV ,0) AS [Black Book ACV], /* Change in EDW as ASAPBlackbookACV & BlackbookACV are same. In EDW we have only BlackbookACV object*/
			NULLIF((DS.ECR),0) AS [ECR],

			NULLIF(DS.AdministrativeBranchNumber,0) AS [Branch Number],  
			
			CASE WHEN DB.BranchNumber = 0 THEN NULL	
					ELSE LTRIM(RTRIM(DB.BranchName)) END AS [Branch Name],
			CASE WHEN DB.BranchNumber = 0 OR LTRIM(RTRIM(DB.AddressLine1)) = '' THEN NULL
					ELSE LTRIM(RTRIM(DB.AddressLine1)) END AS [Branch Address Line 1v],
			CASE WHEN DB.BranchNumber = 0 OR LTRIM(RTRIM(DB.AddressLine2)) = '' THEN NULL
					ELSE LTRIM(RTRIM(DB.AddressLine2)) END AS [Branch Address Line 2],
			
			
			CASE WHEN (DS.InsuredLastName = '' ) THEN NULL
					ELSE LTRIM(RTRIM(DS.InsuredLastName)) END AS [Insured Last Name],
			CASE WHEN (DS.InsuredFirstName = '' ) THEN NULL
					ELSE LTRIM(RTRIM(DS.InsuredFirstName)) END AS [Insured First Name],
			
			CASE WHEN DB.BranchNumber = 0 OR ISNULL(DB.CityName,'') = '' THEN 'NONE' ELSE DB.CityName END  AS [Branch City],
			CASE WHEN DB.BranchNumber = 0 THEN NULL	
					ELSE LEFT(DB.ZIPCode,5) END AS [Branch ZIP Code],
			CASE WHEN  DB.BranchNumber = 0 OR LTRIM(RTRIM(DB.CountyName)) = '' THEN NULL
					ELSE LTRIM(RTRIM(DB.CountyName)) END AS [Branch County],
			CASE WHEN  DB.BranchNumber = 0 OR LTRIM(RTRIM(DB.StateAbbreviation)) = '' THEN NULL
					ELSE LTRIM(RTRIM(DB.StateAbbreviation)) END AS [Branch State],
			CASE WHEN LEFT(LTRIM(RTRIM(DS.ClaimNumber)),16) = '' THEN NULL
					ELSE LEFT(LTRIM(RTRIM(DS.ClaimNumber)),16) END AS [Claim Number],
			PQR.ResponseValue AS [IP Number], 
			CASE WHEN (DS.OwnerLastName = '' ) THEN NULL
					ELSE LTRIM(RTRIM(DS.OwnerLastName))	END AS [Owner Last Name],
			CASE WHEN (DS.OwnerFirstName = '' ) THEN NULL 
					ELSE LTRIM(RTRIM(DS.OwnerFirstName)) END AS [Owner First Name],             
			CASE WHEN CONVERT(VARCHAR(10), FSCT.LossDateTime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.LossDateTime, 101) END AS [Loss Date],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.AssignmentDateTime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.AssignmentDateTime, 101) END AS [Assignment Date],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.ReleaseDateTime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.ReleaseDateTime, 101) END AS [Release Date],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.PickupDateTime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.PickupDateTime, 101) END AS [Pickup Date],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.DispatchDateTime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.DispatchDateTime, 101) END AS [Dispatch Date],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.PickupDatetime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.PickupDatetime, 101) END AS [Stock Drop Off Date],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.TitleInDateTime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.TitleInDateTime, 101) END AS [Title In Date],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.ReceivedAllDocumentsDateTime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.ReceivedAllDocumentsDateTime, 101) END AS [Received All Doc Date],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.CertINDateTime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.CertINDateTime, 101) END AS [Cert IN Date],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.AuctionDateTime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.AuctionDateTime, 101) END AS [Auction Date],
			CASE WHEN TPB.PrebillSettlementDate IS NOT NULL 
					THEN CASE WHEN CONVERT(VARCHAR(10), TPB.PrebillSettlementDate, 101) = '12/31/2999' THEN NULL
							  ELSE CONVERT(VARCHAR(10), TPB.PrebillSettlementDate, 101) END
				  ELSE CASE WHEN CONVERT(VARCHAR(10), FSCT.SettlementDateTime, 101) = '12/31/2999' THEN NULL 
							ELSE CONVERT(VARCHAR(10), FSCT.SettlementDateTime, 101) END 
				  END AS [Settlement Date],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.BuyerPaymentDateTime, 101) = '12/31/2999' THEN NULL 
				  ELSE CONVERT(VARCHAR(10), FSCT.BuyerPaymentDateTime, 101) END AS [Buyer Payment Date],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.BuyerPickupDateTime, 101) = '12/31/2999' THEN NULL
				  ELSE CONVERT(VARCHAR(10), FSCT.BuyerPickupDateTime, 101) END AS [Buyer Pickup Date],
			 
					 
			LTRIM(RTRIM(DSL.LocationName)) AS [Garage Name],
			CASE WHEN LTRIM(RTRIM(DSL.AddressLine1)) <> '' AND LTRIM(RTRIM(DSL.AddressLine2)) <> ''
						THEN ((DSL.AddressLine1) + ', ' + (DSL.AddressLine2))
				 WHEN LTRIM(RTRIM(DSL.AddressLine1)) <> '' 
						THEN LTRIM(RTRIM(DSL.AddressLine1)) 
				 ELSE NULL END  AS [Garage Address],
					
			LTRIM(RTRIM(DSL.CityName)) AS [Garage City],
			LTRIM(RTRIM(DSL.StateAbbreviation)) AS [Garage State],
			LEFT(LTRIM(RTRIM(DSL.ZipCode)),5) AS [Garage ZipCode],
			LTRIM(RTRIM(DSL.CountyName)) AS [Garage County],

            CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.AdvanceTowCharge) END AS [Advance Tow Charge],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.AdvanceStorageCharge) END AS [Advance Storage Charge],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.AdvanceOtherCharge) END AS [Advacne Other Charge],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.AdvanceTaxCharge) END AS [Advance Tax Charges],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 			 					
					ELSE (FSCE.AdvanceTotalCharge) + (FSCE.AdvanceTaxCharge) END AS [Advance Total Expenses],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.ProviderTotalCharge - FSCE.ProviderOtherCharge) END AS [Provider Total Charge Excluding Other],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.ProviderCommissionCharge) END AS [Provider Commission Charge],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.ProviderEnhancementCharge) END AS [Provider Enhancement Charge],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.ProviderTitleCharge) END AS [Provider Title Charge],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.ProviderStorageCharge) END AS [Provider Storage Charge],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.ProviderTowCharge) END AS [Provider Tow Charge],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.ProviderOtherCharge) END AS [Provider Other Charge],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (FSCE.ProviderTotalCharge) END AS [Provider Total Charges],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE ((FSCE.ProviderTotalCharge) + (FSCE.AdvanceTotalCharge + FSCE.AdvanceTaxCharge)) END AS [Provider Total Charges & Advance Charge Expense],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE
						(CASE WHEN ( (CASE WHEN ISNULL(DS.PAPrice, 0) = 0 THEN ISNULL(DS.SalePrice, 0) ELSE DS.PAPrice END) 
										  - (FSCE.ProviderCommissionCharge + FSCE.ProviderTowCharge + FSCE.ProviderStorageCharge + FSCE.ProviderTitleCharge + FSCE.ProviderEnhancementCharge + FSCE.ProviderOtherCharge) 
										  - (FSCE.AdvanceOtherCharge + FSCE.AdvanceStorageCharge + FSCE.AdvanceTowCharge) ) < 0 
								  THEN ( (CASE WHEN ISNULL(DS.PAPrice, 0) = 0 THEN ISNULL(DS.SalePrice, 0) ELSE DS.PAPrice END) 
										  - (FSCE.ProviderCommissionCharge + FSCE.ProviderTowCharge + FSCE.ProviderStorageCharge + FSCE.ProviderTitleCharge + FSCE.ProviderEnhancementCharge + FSCE.ProviderOtherCharge) 
										  - (FSCE.AdvanceOtherCharge + FSCE.AdvanceStorageCharge + FSCE.AdvanceTowCharge) )
								ELSE 0 END) 
					 END AS [Deficit],        
		    CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE
						(CASE WHEN ( (CASE WHEN ISNULL(DS.PAPrice, 0) = 0 THEN ISNULL(DS.SalePrice, 0) ELSE DS.PAPrice END) 
										  - (FSCE.ProviderCommissionCharge + FSCE.ProviderTowCharge + FSCE.ProviderStorageCharge + FSCE.ProviderTitleCharge + FSCE.ProviderEnhancementCharge + FSCE.ProviderOtherCharge) 
										  - (FSCE.AdvanceOtherCharge + FSCE.AdvanceStorageCharge + FSCE.AdvanceTowCharge) ) >= 0 
								  THEN ( (CASE WHEN ISNULL(DS.PAPrice, 0) = 0 THEN ISNULL(DS.SalePrice, 0) ELSE DS.PAPrice END) 
										  - (FSCE.ProviderCommissionCharge + FSCE.ProviderTowCharge + FSCE.ProviderStorageCharge + FSCE.ProviderTitleCharge + FSCE.ProviderEnhancementCharge + FSCE.ProviderOtherCharge) 
										  - (FSCE.AdvanceOtherCharge + FSCE.AdvanceStorageCharge + FSCE.AdvanceTowCharge) )
								ELSE 0 END) 
					END AS [Net Rerun], 
			CASE WHEN LTRIM(RTRIM(DS.SaleDocumentTypeDescription)) = '' THEN NULL	
					ELSE LTRIM(RTRIM(DS.SaleDocumentTypeDescription)) END AS [Title Type],
			LTRIM(RTRIM(DS.SaleDocumentTypeCategory)) AS [Title Type Category], 
			CASE WHEN DS.SaleRunCount >= 1 THEN DS.SaleRunCount - 1
					ELSE 0 END AS [# Reruns],
			DS.IsCallForSaleAuthorization AS [Call Car Ind],
			ISNULL(DS.MinimumBidAmount,0) AS [Minimum Bid Amount],
			CONVERT(INT,ABS(DS.IsHadHold)) AS [Hold Indicator], 
			CASE WHEN CONVERT(VARCHAR(10), FSCT.SoldDateTime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.SoldDateTime, 101) END AS [Sold Date],

			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE COALESCE(DS.OriginalBidAmount,DS.GrossReturn,0) END AS [Original Bid Amount],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE (CASE WHEN ISNULL(DS.PAPrice,0) = 0 THEN ISNULL(DS.SalePrice,0) 
									ELSE ISNULL(DS.PAPrice,0) END) 
					END AS [Gross Return],

			CASE WHEN FSCT.SoldDayID = 29991231 OR DS.CurrentBuyerID = 0 THEN NULL 
					ELSE DS.CurrentBuyerID END AS [ASAP Buyer ID],
			CASE WHEN FSCT.SoldDayID = 29991231 OR DBU.BuyerID = 0 OR ISNULL(LTRIM(RTRIM(DBU.BuyerName)),'') = '' THEN NULL
						ELSE LTRIM(RTRIM(DBU.BuyerName)) END AS [Buyer Name],
			CASE WHEN FSCT.SoldDayID = 29991231 OR DBU.BuyerID = 0 OR ISNULL(LTRIM(RTRIM(DBU.AddressLine1)),'') = '' THEN NULL
					ELSE LTRIM(RTRIM(DBU.AddressLine1)) END AS [Buyer Address Line 1],
			CASE WHEN FSCT.SoldDayID = 29991231 OR DBU.BuyerID = 0 OR ISNULL(LTRIM(RTRIM(DBU.AddressLine2)),'') = '' THEN NULL
					ELSE LTRIM(RTRIM(DBU.AddressLine2)) END AS [Buyer Address Line 2],
			CASE WHEN FSCT.SoldDayID = 29991231 OR DBU.BuyerID = 0 OR ISNULL(LTRIM(RTRIM(DBU.CityName)),'') = '' THEN NULL
					ELSE LTRIM(RTRIM(DBU.CityName)) END AS [Buyer City],
			CASE WHEN FSCT.SoldDayID = 29991231 OR DBU.BuyerID = 0 OR ISNULL(LTRIM(RTRIM(DBU.StateAbbreviation)),'') = '' THEN NULL
					ELSE LTRIM(RTRIM(DBU.StateAbbreviation)) END AS [Buyer State],
			CASE WHEN FSCT.SoldDayID = 29991231 OR DBU.BuyerID = 0 OR ISNULL(LTRIM(RTRIM(DBU.ZipCode)),'') = '' THEN NULL
					ELSE LEFT(LTRIM(RTRIM(DBU.ZipCode)),5) END AS [Buyer ZipCode],
			CASE WHEN FSCT.SoldDayID = 29991231 OR DBU.BuyerID = 0 OR ISNULL(LTRIM(RTRIM(DBU.CountryDescription)),'') = '' THEN NULL
					ELSE LTRIM(RTRIM(DBU.CountryDescription)) END AS [Buyer Country],
			CASE WHEN LTRIM(RTRIM(DS.AgreementTermTypeDescription)) = '' THEN NULL
					ELSE LTRIM(RTRIM(DS.AgreementTermTypeDescription)) END AS [Agreement Term Type],
			CASE WHEN FSCT.SoldDayID = 29991231 THEN NULL 
					ELSE DS.InboundTowMiles END AS [Tow Miles], 
			DS.IsRunAndDriveAuction AS [Run & Drive],
			NULL AS [Average Salvage Value],
			
			CASE WHEN FSCT.SoldDayID = 29991231 THEN 0
					ELSE DS.IsIBFSold END AS [IBuy Fast Indicator],  
			
			
			TBA.Historical_High_Bid_Amount AS [Historical High Bid Amount], 
			CASE WHEN DS.MinimumBidSource = '' THEN NULL
					ELSE DS.MinimumBidSource END AS [Minimum Bid Category],
			TBA.High_Bid_1st_Run AS [Highest Bid 1st Run],
			TBA.High_Bid_2nd_Run AS [Highest Bid 2nd Run],
			TBA.High_Bid_3rd_Run AS [Highest Bid 3rd Run],
			DS.IsCancel AS [Cancel Salvage Indicator],
			CASE WHEN LTRIM(RTRIM(DS.StockCancelReasonDescription)) = '' THEN NULL
					ELSE LTRIM(RTRIM(DS.StockCancelReasonDescription)) END AS [Cancel Reason Description],
			
			CASE WHEN FSCT.SoldDayID = 29991231 THEN 'U' ELSE 'S' END AS [Sold/Unsold],
			CASE WHEN ISNULL(NCPR.NCPRDate,'12/31/2999') = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), NCPR.NCPRDate, 101) END AS [Date Reported to NICB],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.SalesDocumentAppliedDatetime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.SalesDocumentAppliedDatetime, 101) END AS [Sales_Document_Applied_Datetime],
			CASE WHEN CONVERT(VARCHAR(10), FSCT.SaleDocumentReceivedDateTime, 101) = '12/31/2999' THEN NULL
					ELSE CONVERT(VARCHAR(10), FSCT.SaleDocumentReceivedDateTime, 101) END AS [Sale_Document_Received_DateTime]
			--SELECT *  
			FROM dbo.DimStock DS WITH(NOLOCK)
				INNER JOIN dbo.DimProvider AS DP WITH(NOLOCK)
					ON DS.CurrentProviderID = DP.ProviderID
				INNER JOIN dbo.DimBranch AS DB WITH(NOLOCK)
					ON DS.AdministrativeBranchNumber = DB.BranchNumber
				INNER JOIN dbo.FactStockCycleTime AS FSCT WITH(NOLOCK)
					ON DS.StockID = FSCT.StockID
				INNER JOIN dbo.DimBuyer AS DBU WITH(NOLOCK)
					ON DS.CurrentBuyerID = DBU.BuyerID
				INNER JOIN #TMP_StocksForDay S 
					ON S.StockID = DS.StockID
				LEFT OUTER JOIN dbo.FactStockChargeExpense AS FSCE WITH(NOLOCK)
					ON FSCE.StockID = DS.StockID
				LEFT OUTER JOIN #TMP_Prebill TPB 
					ON S.StockID = TPB.StockID	
				LEFT OUTER JOIN #TMP_Bid_Amount TBA 
					ON S.StockID = TBA.StockID
				LEFT OUTER JOIN #TMP_NCPR_Date NCPR
					ON S.StockID = NCPR.StockID	
				LEFT OUTER JOIN dbo.DimStorageLocation DSL WITH(NOLOCK)
					ON DS.PickupLocationID = DSL.StorageLocationID
				LEFT OUTER JOIN #TMP_Provider_QR PQR
					ON DS.StockID = PQR.StockID		
				
COMMIT TRAN

EXEC [dbo].[usp_ETLLoadAudit]
	@StartEnd = 'E',
	@ETLLoadID = @ETLLoadID,
	@ExtractRowCount = @ExtractRowCount,
	@InsertRowCount = @InsertRowCount,
    @UpdateRowCount = @UpdateRowCount

	RETURN 0
END TRY

BEGIN CATCH

	SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
	
	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount, 
		@UpdateRowCount = @UpdateRowCount,
		@ErrorCode = @@ERROR
		
		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	

END CATCH

END


