/* 
Use EDW
--==============================================================================
--Author		:	John Stires
--Create date	:	07/31/2013
--Description	:	Loads Geico Inventory data, of Unsold vehicles, to a table
--						 for exporting data into a flat file for Geico.
--==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
08/02/2013	John Stires			Update logic for  Process Status Description	
08/07/2013	John Stires			Converted the hard coding for the Geico Region 
								to using the CodesTable.
08/08/2013	John Stires			Adjusdted the length of the Claim Number so as 
								to not truncate
08/09/2013	John Stires			Removed all the checks for the Settlement Date
08/13/2013	John Stires			Change the source for DaysInPhysicalInventoy to
								DaysInPhysicalInventoyProvider.
08/16/2013	John Stires			Changed the source of the Residence Indicator
								From the DimStorageLocation to DimStock
--==============================================================================
--Usage Example:
--Exec usp_GeicoDailyInventoryUpload
--
--==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_GeicoDailyInventoryUpload]

AS
BEGIN

  SET NOCOUNT ON
 
  DECLARE
		@ETLLoadID			INT,
		@ExtractRowCount	INT,
		@InsertRowCount		INT,
		@UpdateRowCount		INT 
 
BEGIN TRY
  
  --Run the Audit begin
  EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_GeicoDailyInventoryUpload'

--/********************************************************************************************
--	Create the temp tables for building the GeicoDailyInventoryUpload
--********************************************************************************************/

	IF OBJECT_ID('tempdb..#TMP_StocksForDay') IS NOT NULL  DROP TABLE #TMP_StocksForDay
	CREATE TABLE #TMP_StocksForDay	(
			  StockID INT		)


	IF OBJECT_ID('tempdb..#TMP_Provider_QR') IS NOT NULL  DROP TABLE #TMP_Provider_QR
	CREATE TABLE #TMP_Provider_QR	(
			  StockID		INT
			, ResponseValue	VARCHAR(40)	)


--*********************************************************************************************
--		Table to gather the stock IDs specifically for Geico
--*********************************************************************************************/
	INSERT INTO #TMP_StocksForDay(StockID)
--###
	SELECT
			  FSCT.StockID 
	FROM dbo.FactStockCycleTime		AS FSCT		WITH(NOLOCK)
		INNER JOIN dbo.DimStock		AS DS		WITH(NOLOCK)
			ON FSCT.StockID = DS.StockID
--###
		INNER JOIN dbo.LKStockID	AS LKS		WITH(NOLOCK)
			ON DS.StockID	= LKS.StockID
--###
		INNER JOIN dbo.DimProvider	AS DP		WITH(NOLOCK)
			ON DS.CurrentProviderID = DP.ProviderID
		--LEFT OUTER JOIN (	SELECT  DSET.EventTypeID,
		--							ASAPSalvageID
		--					FROM dbo.DimStockEventType	AS DSET	WITH(NOLOCK)
		--					JOIN dbo.FactStockEvent		AS FSE	WITH(NOLOCK)
		--							ON DSET.EventTypeID = FSE.EventTypeID
		--					WHERE dset.EventCode = 'FPBE') AS PreBill
		--	ON PreBill.ASAPSalvageID = DS.ASAPSalvageID
	WHERE DP.ProviderGroupName = 'Berkshire Hathaway'
			AND FSCT.SettlementDayID = 29991231
		--	AND PreBill.ASAPSalvageID IS NULL


--*********************************************************************************************
--		Table to gather the provider info specifically for Geico
--*********************************************************************************************/
	INSERT INTO #TMP_Provider_QR	(
			  StockID
			, ResponseValue		)
--###
	SELECT
			  T.StockID
			, FPQR.ResponseValue
	FROM dbo.FactProviderQuestionResponse FPQR  WITH (NOLOCK)
		INNER JOIN 
			(   SELECT	
						  Max(FPQR.ProviderQuestionResponseID) AS ProviderQuestionResponseID
						, FPQR.StockID 
				FROM dbo.FactProviderQuestionResponse		AS FPQR		WITH (NOLOCK)
					LEFT OUTER JOIN dbo.DimProviderQuestion AS DPQC	WITH (NOLOCK)
						ON		DPQC.QuestionID = FPQR.QuestionID
							AND	DPQC.ProviderID = FPQR.ProviderID
					LEFT OUTER JOIN dbo.DimProviderQuestion	AS DPQP		WITH (NOLOCK) 
						ON		DPQP.QuestionID = FPQR.QuestionID
							AND	DPQP.ProviderID = FPQR.ParentProviderID 
					INNER JOIN #TMP_StocksForDay			AS DS		WITH (NOLOCK) 
						ON		DS.StockID = FPQR.StockID
				WHERE		FPQR.QuestionID = 30
					  AND	FPQR.ProviderID IN (	SELECT ProviderID
													FROM dbo.DimProvider AS DP WITH(NOLOCK)
													WHERE  DP.ProviderGroupName IN ( 'Berkshire Hathaway' ))
				GROUP BY FPQR.StockID) AS T
		  ON FPQR.ProviderQuestionResponseID = T.ProviderQuestionResponseID

	
/******************************************************************************************************
      				INSERTING UNSOLD VEHICLE DATA INTO THE TABLE - BEGIN
*******************************************************************************************************/
BEGIN TRANSACTION
      
    TRUNCATE TABLE dbo.GeicoDailyInventoryUpload
      
    INSERT INTO dbo.GeicoDailyInventoryUpload
			(	  ProviderGroupName
				, ProviderName
				, GeicoRegion
				, BranchName
				, BranchState
				, StockNumber
				, AssignmentType
				, ClaimNumber
				, IPNumber
				, VIN
				, PolicyNumber
				, LossTypeDescription
				, PrimaryDamageDescription
				, ModelYear
				, Make
				, Model
				, SaleTitleType
				, SaleTitleTypeCategory
				, VICInd
				, WithKeysAtCheckInInd
				, KeyEnhancementInd
				, RunAndDriveInd
				, ProcessStatusDescription
				, AssignmentDateTime
				, ReleaseDateTime
				, LogInDateTime
				, CheckInDateTime
				, SMVDDateTime
				, CertInDateTime
				, DaysInPhysicalInventory
				, CancelDateTime
				, AuctionDate
				, AnticipatedSaleDate
				, RowID
				, SlotID
				, TitleInDateTime
				, ResidenceInd
				, GarageName
				, GarageAddress
				, GarageCity
				, GarageState
				, GarageZipCode
				, DeliveredToBranchInd
				, ActualCashValue
				, MinimumBidAmount
				, PickupDateTime
				, HadAdvanceChargeInd
				, AdvanceTowCharge
				, AdvanceStorageCharge
				, AdvanceOtherCharge
				, AdvanceTotalCharge
			)
--###
	SELECT
		  LTRIM(RTRIM(DP.ProviderGroupName))		AS [ProviderGroupName]	--Provider Group Name
		, LTRIM(RTRIM(DP.ProviderName))				AS [ProviderName]		--Provider Name
		, LTRIM(RTRIM(CD.AlphaID))					AS [Geico Region]		--Geico Region
		, CASE WHEN DB.BranchNumber = 0 THEN NULL						--Branch Name
				ELSE LTRIM(RTRIM(DB.BranchName)) END AS [BranchName]
		, CASE WHEN		DB.BranchNumber = 0								--Branch State
					OR	LTRIM(RTRIM(DB.StateAbbreviation)) = '' THEN NULL
				ELSE LTRIM(RTRIM(DB.StateAbbreviation)) 
												END AS [BranchState]
		, LTRIM(RTRIM(DS.FullStockNumber))			AS [StockNumber]		--Stock Number
		, CASE WHEN  LTRIM(RTRIM(DS.AssignmentTypeDescription)) = '' THEN NULL	
				ELSE LTRIM(RTRIM(DS.AssignmentTypeDescription))			--Assignment Type Description
												END AS [AssignmentTypeDescription]
		, CASE WHEN  LEFT(LTRIM(RTRIM(DS.ClaimNumber)),16) = '' THEN NULL --Claim Number
				ELSE LEFT(LTRIM(RTRIM(DS.ClaimNumber)),16) 
												END AS [ClaimNumber]		
		, CASE WHEN  LTRIM(RTRIM(PQR.ResponseValue)) = '' THEN NULL		--IP Number
				ELSE LTRIM(RTRIM(PQR.ResponseValue)) END AS [VIN]
		, CASE WHEN  LTRIM(RTRIM(DS.VIN)) = '' THEN NULL				--VIN
				ELSE LTRIM(RTRIM(DS.VIN))		END AS [VIN]
		, CASE WHEN  LTRIM(RTRIM(DS.PolicyNumber)) = '' THEN NULL		--Policy Number
				ELSE LTRIM(RTRIM(DS.PolicyNumber)) END AS [PolicyNumber]
		, CASE WHEN  LTRIM(RTRIM(DS.LossTypeDescription)) = '' THEN NULL--Loss Type Description
				ELSE LTRIM(RTRIM(DS.LossTypeDescription)) END AS [LossTypeDescription]
		, CASE WHEN  LTRIM(RTRIM(DS.DamageDescriptionPrimary)) = '' THEN NULL	
				ELSE LTRIM(RTRIM(DS.DamageDescriptionPrimary))			--Primary Damage Description
												END AS [PrimaryDamageDescription]
		, NULLIF(DS.ModelYear,0)					AS [ModelYear]			--Model Year
		,  CASE WHEN LTRIM(RTRIM(DS.MakeName)) = '' THEN NULL 
				ELSE LTRIM(RTRIM(DS.MakeName))	END AS [Make]			--Make
		,  CASE WHEN LTRIM(RTRIM(DS.ModelName)) = '' THEN NULL 
				ELSE LTRIM(RTRIM(DS.ModelName))	END AS [Model]			--Model
		, CASE WHEN  LTRIM(RTRIM(DS.SaleDocumentTypeDescription)) = '' THEN NULL	
				ELSE LTRIM(RTRIM(DS.SaleDocumentTypeDescription))
												END AS [SaleTitleType]		--Sale Title Type
		, CASE WHEN DS.SaleDocumentTypeDescription <> '' THEN DS.SaleDocumentTypeCategory 
				ELSE ''							END AS SaleDocumentTypeCategory --Sale Title Type Category
		, CASE WHEN DS.IsVIC = 1 THEN 1 
				ELSE 0							END AS [VIC Ind]			--VIC Ind
		, CASE WHEN ISNULL(DS.KeysRetainedCodeatCheckIn, 0) = 0 THEN 0
				ELSE DS.KeysRetainedCodeatCheckIn END AS [WithKeysAtCheckInInd]--With Keys at Check In Ind
		, CASE WHEN ISNULL(DS.IsKeyEnhancment, 0) = 0 THEN 0
				ELSE DS.IsKeyEnhancment			END	AS [KeyEnhancementInd]	--Key Enhancement Ind
		, CASE WHEN ISNULL(DS.IsRunAndDriveAuction, 0) = 0 THEN 0
				ELSE DS.IsRunAndDriveAuction	END AS [RunAndDriveInd]	--Run And Drive Ind
--###
		, CASE	WHEN LTRIM(RTRIM(DS.[ProcessStatusDescription])) = '' THEN NULL
				ELSE LTRIM(RTRIM(DS.[ProcessStatusDescription])) 
							END AS [ProcessStatusDescription]			--Process Status Description
		, CASE WHEN  ISNULL (FSCT.AssignmentDateTime, 0)	= 0
				  OR CONVERT(VARCHAR(10), FSCT.AssignmentDateTime, 101) = '12/31/2999'	THEN NULL
				ELSE CONVERT(VARCHAR(10), FSCT.AssignmentDateTime, 101)	+ ' ' +
					 CONVERT(VARCHAR(8),  FSCT.AssignmentDateTime, 108)	END AS [AssignmentDateTime]
		, CASE WHEN  ISNULL(FSCT.ReleaseDateTime, 0)		= 0
				  OR CONVERT(VARCHAR(10), FSCT.ReleaseDateTime, 101) = '12/31/2999'		THEN NULL
				ELSE CONVERT(VARCHAR(10), FSCT.ReleaseDateTime, 101)	+ ' ' +
					 CONVERT(VARCHAR(8),  FSCT.ReleaseDateTime, 108)	END AS [ReleaseDateTme]
		, CASE WHEN	 ISNULL (FSCT.LogInDateTime, 0)			= 0
				  OR CONVERT(VARCHAR(10), FSCT.LogInDateTime, 101) = '12/31/2999'		THEN NULL
				ELSE CONVERT(VARCHAR(10), FSCT.LogInDateTime, 101)	+ ' ' +
					 CONVERT(VARCHAR(8), FSCT.LogInDateTime, 108)	END AS [LogInDateTime]
		, CASE WHEN  ISNULL(FSCT.CheckInDateTime, 0)		= 0
				  OR CONVERT(VARCHAR(10), FSCT.CheckInDateTime, 101) = '12/31/2999'		THEN NULL
				ELSE CONVERT(VARCHAR(10), FSCT.CheckInDateTime, 101)+ ' ' +
					 CONVERT(VARCHAR(10), FSCT.CheckInDateTime, 108) END AS [CheckInDateTime]
		, CASE WHEN  ISNULL (FSCT.SMVDDatetime, 0)			= 0
				  OR CONVERT(VARCHAR(10), FSCT.SMVDDatetime, 101) = '12/31/2999'		THEN NULL
				ELSE CONVERT(VARCHAR(10), FSCT.SMVDDatetime, 101)+ ' ' +
					 CONVERT(VARCHAR(10), FSCT.SMVDDateTime, 108)		END AS [SMVDDateTime]
		, CASE WHEN  ISNULL (FSCT.CertInDateTime, 0)		= 0
				  OR CONVERT(VARCHAR(10), FSCT.CertInDateTime, 101) = '12/31/2999'	THEN NULL
				ELSE CONVERT(VARCHAR(10), FSCT.CertInDateTime, 101)+ ' ' +
					 CONVERT(VARCHAR(10), FSCT.CertInDateTime, 108)		END AS [CertInDateTime]
--###
		, ISNULL(FSCT.DaysInPhysicalInventoryProvider, 0)  AS DaysInPhysicalInventory --DaysInPhysicalInventory
		, CASE WHEN  ISNULL (FSCT.CancelDateTime, 0)			= 0
				  OR CONVERT(VARCHAR(10), FSCT.CancelDateTime, 101) = '12/31/2999'	THEN NULL
				ELSE CONVERT(VARCHAR(10), FSCT.CancelDateTime, 101)+ ' ' +
					 CONVERT(VARCHAR(10), FSCT.CancelDateTime, 108)
												END AS [CancelDateTime]		--CancelDateTime
--		, CASE WHEN  CONVERT(VARCHAR(10), FSCT.AuctionDateTime, 101) = '12/31/2999' THEN NULL
		, CASE WHEN  ISNULL (FSCT.AuctionDateTime, 0)		= 0
				  OR CONVERT(VARCHAR(10), FSCT.AuctionDateTime, 101) = '12/31/2999'	THEN NULL
				ELSE CONVERT(VARCHAR(10), FSCT.AuctionDateTime, 101)
												END	AS [AuctionDate]		--AuctionDate
		, CASE WHEN  ISNULL (FSCT.AnticipatedSaleDate, 0)	= 0
				  OR CONVERT(VARCHAR(10), FSCT.AnticipatedSaleDate, 101) = '12/31/2999'	THEN NULL
				ELSE CONVERT(VARCHAR(10), FSCT.AnticipatedSaleDate, 101)--AnticipatedSaleDate 
												END AS [AnticipatedSaleDate]
		, CASE WHEN  LTRIM(RTRIM(DS.[AisleNumber])) = ''	THEN NULL
				ELSE LTRIM(RTRIM(DS.[AisleNumber]))	END	AS [RowID]	--RowID  - Stock Location
		, CASE WHEN  ISNULL(DS.StallNumber,0) = 0 THEN NULL
				ELSE DS.[StallNumber]			END	AS [SlotID]				--SlotID - Stock Location
		, CASE WHEN  ISNULL (FSCT.TitleInDateTime, 0)		= 0
				  OR CONVERT(VARCHAR(10), FSCT.TitleInDateTime, 101) = '12/31/2999'	THEN NULL
				ELSE CONVERT(VARCHAR(10), FSCT.TitleInDateTime, 101)+ ' ' +
					 CONVERT(VARCHAR(10), FSCT.TitleInDateTime, 108) 
												END AS [TitleInDateTime] 	--TitleInDateTime
		, CASE WHEN DS.IsResidence = 1 THEN 1
				ELSE 0							END AS ResidenceInd		--ResidenceInd
		, CASE WHEN	 LTRIM(RTRIM(DSL.LocationName)) = '' THEN NULL
				ELSE LTRIM(RTRIM(DSL.LocationName))	END	AS [GarageName]	--GarageName
		, CASE	WHEN	LTRIM(RTRIM(DSL.AddressLine1)) <> ''			--GarageAddress
					AND LTRIM(RTRIM(DSL.AddressLine2)) <> ''			
					THEN ((DSL.AddressLine1) + ', ' + (DSL.AddressLine2))
				WHEN LTRIM(RTRIM(DSL.AddressLine1)) <> '' 
					THEN LTRIM(RTRIM(DSL.AddressLine1)) 
				ELSE NULL						END	AS [GarageAddress]
		, CASE WHEN	 LTRIM(RTRIM(DSL.CityName)) = '' THEN NULL
				ELSE LTRIM(RTRIM(DSL.CityName))	END	AS [GarageCity]		--GarageCity
		, CASE WHEN LTRIM(RTRIM(DSL.StateAbbreviation)) = '' THEN NULL
				ELSE LTRIM(RTRIM(DSL.StateAbbreviation)) END AS [GarageState] --GarageState
		, CASE WHEN LEFT(LTRIM(RTRIM(DSL.ZipCode)),5) = '' THEN NULL
				ELSE LEFT(LTRIM(RTRIM(DSL.ZipCode)),5)	END	AS [GarageZipCode]--GarageZipCode
		, CASE WHEN  DS.[IsDeliveredToBranch] = 1 THEN 1
				ELSE 0							END	AS [DeliveredToBranchInd]--DeliveredToBranchInd
		, CASE WHEN ISNULL(FSCT.SoldDayID, 0) = 0 THEN NULL
				ELSE (NULLIF((CASE WHEN DS.IsACVOutlier = 0  THEN DS.ActualCashValue
						ELSE NULL END),0))		END	AS [ActualCashValue]	--ActualCashValue
		, CASE WHEN ISNULL(DS.MinimumBidAmount,0) = 0 THEN NULL
				ELSE DS.MinimumBidAmount		END AS [MinimumBidAmount]	--MinimumBidAmount
		, CASE WHEN  ISNULL (FSCT.PickupDateTime, 0)	= 0
				  OR CONVERT(VARCHAR(10), FSCT.PickupDateTime, 101) = '12/31/2999'	THEN NULL
			ELSE CONVERT(VARCHAR(10), FSCT.PickupDateTime, 101) + ' ' +
					 CONVERT(VARCHAR(10), FSCT.PickupDateTime, 108)
												END AS [PickupDateTime]		--PickupDateTime
		, CASE WHEN DS.[isHadAdvanceCharge] = 1 THEN 1
				ELSE 0							END	AS [HadAdvanceChargeInd]--HadAdvanceChargeInd
		, CASE WHEN ISNULL(FSCT.SoldDayID, 0) = 0 THEN NULL				--AdvanceTowCharge
				ELSE (FSCE.AdvanceTowCharge)	END AS [AdvanceTowCharge]
		, CASE WHEN ISNULL(FSCT.SoldDayID, 0) = 0 THEN NULL				--AdvanceStorageCharge
				ELSE (FSCE.AdvanceStorageCharge)END AS [AdvanceStorageCharge]
		, CASE WHEN ISNULL(FSCT.SoldDayID, 0) = 0 THEN NULL				--AdvanceOtherCharge
				ELSE (FSCE.AdvanceOtherCharge)	END AS [AdvaceOtherCharge]
		, CASE WHEN ISNULL(FSCT.SoldDayID, 0) = 0 THEN NULL				--AdvanceTotalCharge
				ELSE (FSCE.AdvanceTotalCharge)	END AS [AdvaceTotalCharge]

	FROM dbo.DimStock AS DS							WITH(NOLOCK)
		INNER JOIN dbo.DimProvider AS DP			WITH(NOLOCK)
			ON DS.CurrentProviderID = DP.ProviderID
		INNER JOIN dbo.DimBranch AS DB				WITH(NOLOCK)
			ON DS.AdministrativeBranchNumber = DB.BranchNumber
		INNER JOIN dbo.FactStockCycleTime AS FSCT	WITH(NOLOCK)
			ON DS.StockID = FSCT.StockID	
		INNER JOIN dbo.DimBuyer AS DBU				WITH(NOLOCK)
			ON DS.CurrentBuyerID = DBU.BuyerID
		INNER JOIN CodesTable AS CD 				WITH(NOLOCK)
			ON		CD.CodeType	= 'GeicoRegion'
			  AND	CD.KeyType	= LTRIM(RTRIM(DB.StateAbbreviation))
		INNER JOIN #TMP_StocksForDay AS S 			WITH(NOLOCK)
			ON S.StockID = DS.StockID
		LEFT OUTER JOIN dbo.FactStockChargeExpense AS FSCE	WITH(NOLOCK)
			ON FSCE.StockID = DS.StockID
		LEFT OUTER JOIN dbo.DimStorageLocation AS DSL WITH(NOLOCK)
			ON DS.PickupLocationID = DSL.StorageLocationID
		LEFT OUTER JOIN #TMP_Provider_QR AS PQR		WITH(NOLOCK)
				ON DS.StockID = PQR.StockID
				
COMMIT TRANSACTION

EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd		= 'E',
		@ETLLoadID		= @ETLLoadID,
		@ExtractRowCount= @ExtractRowCount,
		@InsertRowCount	= @InsertRowCount,
	    @UpdateRowCount	= @UpdateRowCount

	RETURN 0
END TRY


BEGIN CATCH

	SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
	
	EXEC [dbo].[usp_ETLLoadAudit]
			@StartEnd		= 'E',
			@ETLLoadID		= @ETLLoadID,
			@ExtractRowCount= @ExtractRowCount,
			@InsertRowCount	= @InsertRowCount, 
			@UpdateRowCount	= @UpdateRowCount,
			@ErrorCode		= @@ERROR
		
		DECLARE	  
			  @ErrorMessage NVARCHAR(4000)
			, @ErrorSeverity INT
			, @ErrorState INT;
		SELECT	  
			  @ErrorMessage	 = ERROR_MESSAGE()
			, @ErrorSeverity = ERROR_SEVERITY()
			, @ErrorState	 = ERROR_STATE();
		RAISERROR (
			  @ErrorMessage
			, @ErrorSeverity
			, @ErrorState		);

		RETURN -1
END CATCH

END

GO
