/* 
USE EDW
GO 
==============================================================================
Author            : Juan X Jacome
Create date : 08/24/2012
Description : Populate the EDW DimBuyerLicense table every day
==============================================================================
REVISION History
ChangeDate	Developer			Release/Problem Number	

==============================================================================
Usage Example:
Exec usp_ETLDimBuyerLicense
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimBuyerLicense]

AS     
BEGIN

DECLARE @ETLLoadID INT,
		@ExtractRowCount INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@DeleteRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
		
SET NOCOUNT ON;
BEGIN TRY

----Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimBuyerLicense'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK)WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp tables
*********************************************************************************************/

IF OBJECT_ID('tempdb..#TMP_DimBuyerLicense') IS NOT NULL DROP TABLE #TMP_DimBuyerLicense
CREATE TABLE #TMP_DimBuyerLicense
(	[BuyerLicenseID] [int] NOT NULL,
	[BuyerID] [int] NOT NULL,
	[LicenseTypeCode] [char](6) NULL,
	[LicenseTypeDescription] [varchar](40) NULL,
	[EffectiveDayID] [int] NULL,
	[EffectiveDateTime] [datetime] NULL,
	[ExpirationDayID] [int] NULL,
	[ExpirationDateTime] [datetime] NULL,
	[LicenseNumber] [varchar](20) NULL,
	[LetterofCreditImageID] [int] NULL,
	[CountryAbbreviation] [char](2) NULL,
	[StateAbbreviation] [char](3) NULL,
	[ResaleForm] [varchar](50) NULL,
	[AssociatedVehicleTypes] [varchar](255) NULL,
	[All_Salvage_Types] [bit] NOT NULL

)

IF OBJECT_ID('tempdb..#TMP_DimBuyerLicense_Count') IS NOT NULL DROP TABLE #TMP_DimBuyerLicense_Count
CREATE TABLE #TMP_DimBuyerLicense_Count(ChangeType VARCHAR(10)) 

IF OBJECT_ID('tempdb..#TMP_BuyerLicenseTypeNotAll') IS NOT NULL DROP TABLE #TMP_BuyerLicenseTypeNotAll
CREATE TABLE #TMP_BuyerLicenseTypeNotAll
(	[BuyerLicenseID] [int] NOT NULL,
	[SalvageDescription] [varchar](40) NULL
)	

IF OBJECT_ID('tempdb..#TMP_BuyerLicenseTypeNotAll_AssociatedVehicleTypes') IS NOT NULL DROP TABLE #TMP_BuyerLicenseTypeNotAll_AssociatedVehicleTypes
CREATE TABLE #TMP_BuyerLicenseTypeNotAll_AssociatedVehicleTypes
(	[BuyerLicenseID] [int] NOT NULL,
	[AssociatedVehicleTypes] [varchar](255) NULL
)	
/*********************************************************************************************
Insert into #temp tables
*********************************************************************************************/
INSERT INTO #TMP_DimBuyerLicense --select * from #TMP_DimBuyerLicense
(	[BuyerLicenseID],
	[BuyerID],
	[LicenseTypeCode],
	[LicenseTypeDescription],
	[EffectiveDayID],
	[EffectiveDateTime],
	[ExpirationDayID],
	[ExpirationDateTime],
	[LicenseNumber],
	[LetterofCreditImageID],
	[CountryAbbreviation],
	[StateAbbreviation],
	[ResaleForm],
	[AssociatedVehicleTypes],
	[All_Salvage_Types]
)
select	BL.Buyer_License_ID,
		BL.Buyer_ID,
		ISNULL(BL.License_Type_Code,''),
		ISNULL(LT.License_Type_Description,''),
		ISNULL(CONVERT(INT, CONVERT(CHAR(8),(BL.Effective_Date), 112)), 29991231) AS EffectiveDayID,
		ISNULL(BL.Effective_Date,'12-31-2999'),
		ISNULL(CONVERT(INT, CONVERT(CHAR(8),(BL.Effective_Date), 112)), 29991231) AS ExpirationDayID,
		ISNULL(BL.Expiration_Date,'12-31-2999'),
		ISNULL(BL.License_Number,0),
		ISNULL(BL.Letter_of_Credit_Image_ID,0),
		ISNULL(BL.Country_Abbreviation,''),
		ISNULL(BL.State_Abbreviation,''),
		ISNULL(BL.ResaleForm,''),
		CASE WHEN BL.All_Salvage_Types = 1 THEN 'All' ELSE 'Not Sure' END AS AssociatedVehicleTypes,
		BL.All_Salvage_Types
  FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_buyer_license] (@StartLSN, @EndLSN, 'all with merge') AS BL
  INNER JOIN [BI_ASAP_Rep].dbo.License_Type LT
   ON BL.License_Type_Code = LT.License_Type_Code
  WHERE  [__$operation]=5
  
  
 
  INSERT INTO #TMP_BuyerLicenseTypeNotAll
(	BuyerLicenseID,
	SalvageDescription
)
  SELECT BLST.Buyer_License_ID, ST.Salvage_Description AS SalvageDescription
  FROM #TMP_DimBuyerLicense TDBL WITH (NOLOCK)
  INNER JOIN [BI_ASAP_Rep].dbo.Buyer_License_Salvage_Types BLST WITH (NOLOCK)
   ON TDBL.BuyerLicenseID = BLST.Buyer_License_ID
  INNER JOIN [BI_ASAP_Rep].dbo.salvage_type ST WITH (NOLOCK)
   ON BLST.Salvage_Type = ST.Salvage_Type
  WHERE TDBL.All_Salvage_Types = 0


INSERT INTO #TMP_BuyerLicenseTypeNotAll_AssociatedVehicleTypes
(	BuyerLicenseID,
	AssociatedVehicleTypes
)	
  SELECT t1.BuyerLicenseID,

       ( SELECT SalvageDescription + ', '

           FROM #TMP_BuyerLicenseTypeNotAll t2

          WHERE t2.BuyerLicenseID = t1.BuyerLicenseID

          ORDER BY SalvageDescription

            FOR XML PATH('') 
         ) AS SalvageDescriptionNew

      FROM #TMP_BuyerLicenseTypeNotAll t1
GROUP BY t1.BuyerLicenseID 


UPDATE TDBL
SET AssociatedVehicleTypes = SUBSTRING(TBLAVT.AssociatedVehicleTypes, 1,LEN(TBLAVT.AssociatedVehicleTypes)-1)
FROM #TMP_DimBuyerLicense TDBL WITH (NOLOCK)
INNER JOIN #TMP_BuyerLicenseTypeNotAll_AssociatedVehicleTypes TBLAVT WITH (NOLOCK)
 ON TDBL.BuyerLicenseID = TBLAVT.BuyerLicenseID

/*********************************************************************************************
* Merge
*********************************************************************************************/
  
  BEGIN TRANSACTION 
--Synchronize source data with target
	MERGE dbo.DimBuyerLicense AS DBL
	USING #TMP_DimBuyerLicense AS TDBL    
		ON DBL.BuyerLicenseID =  TDBL.BuyerLicenseID
		
WHEN MATCHED THEN
    --Row exists and data is different
	UPDATE SET 	
		BuyerID = TDBL.BuyerID,
		LicenseTypeCode = TDBL.LicenseTypeCode,
		LicenseTypeDescription = TDBL.LicenseTypeDescription,
		EffectiveDayID = TDBL.EffectiveDayID,
		EffectiveDateTime = TDBL.EffectiveDateTime,
		ExpirationDayID = TDBL.ExpirationDayID ,
		ExpirationDateTime = TDBL.ExpirationDateTime,
		LicenseNumber = TDBL.LicenseNumber,
		LetterofCreditImageID = TDBL.LetterofCreditImageID,
		CountryAbbreviation = TDBL.CountryAbbreviation,
		StateAbbreviation = TDBL.StateAbbreviation,
		ResaleForm = TDBL.ResaleForm,
		AssociatedVehicleTypes = TDBL.AssociatedVehicleTypes,
		ETLLoadUpdateID = @ETLLoadID
		
WHEN NOT MATCHED THEN
   INSERT(	BuyerLicenseID,
			BuyerID,
			LicenseTypeCode,
			LicenseTypeDescription,
			EffectiveDayID,
			EffectiveDateTime,
			ExpirationDayID,
			ExpirationDateTime,
			LicenseNumber,
			LetterofCreditImageID,
			CountryAbbreviation,
			StateAbbreviation,
			ResaleForm,
			AssociatedVehicleTypes,
			ETLLoadID,
			ETLLoadUpdateID
		)	 
	VALUES (BuyerLicenseID,
			BuyerID,
			LicenseTypeCode,
			LicenseTypeDescription,
			EffectiveDayID,
			EffectiveDateTime,
			ExpirationDayID,
			ExpirationDateTime,
			LicenseNumber,
			LetterofCreditImageID,
			CountryAbbreviation,
			StateAbbreviation,
			ResaleForm,
			AssociatedVehicleTypes,
			@ETLLoadID,
			@ETLLoadID
			)
	     
	OUTPUT $action INTO #TMP_DimBuyerLicense_Count;

	  
/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #TMP_DimBuyerLicense_Count WITH (NOLOCK)

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount


 
 
		COMMIT TRAN
		IF OBJECT_ID('tempdb..#TMP_DimBuyerLicense') IS NOT NULL DROP TABLE #TMP_DimBuyerLicense
		IF OBJECT_ID('tempdb..#TMP_DimBuyerLicense_Count') IS NOT NULL DROP TABLE #TMP_DimBuyerLicense_Count
		IF OBJECT_ID('tempdb..#TMP_BuyerLicenseTypeNotAll') IS NOT NULL DROP TABLE #TMP_BuyerLicenseTypeNotAll
		IF OBJECT_ID('tempdb..#TMP_BuyerLicenseTypeNotAll_AssociatedVehicleTypes') IS NOT NULL DROP TABLE #TMP_BuyerLicenseTypeNotAll_AssociatedVehicleTypes

		 
		
		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
		
		IF OBJECT_ID('tempdb..#TMP_DimBuyerLicense') IS NOT NULL DROP TABLE #TMP_DimBuyerLicense
		IF OBJECT_ID('tempdb..#TMP_DimBuyerLicense_Count') IS NOT NULL DROP TABLE #TMP_DimBuyerLicense_Count
		IF OBJECT_ID('tempdb..#TMP_BuyerLicenseTypeNotAll') IS NOT NULL DROP TABLE #TMP_BuyerLicenseTypeNotAll
		IF OBJECT_ID('tempdb..#TMP_BuyerLicenseTypeNotAll_AssociatedVehicleTypes') IS NOT NULL DROP TABLE #TMP_BuyerLicenseTypeNotAll_AssociatedVehicleTypes

		
		EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	END CATCH
	END


GO


