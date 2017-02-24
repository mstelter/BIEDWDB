/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 4/11/2013
Description	: Populate the DimVehicleFeatures table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	
==============================================================================
Usage Example:
Exec [usp_ETLDimStockFeature]
==============================================================================
*/
CREATE PROCEDURE [dbo].[usp_ETLDimStockFeature]
AS     
BEGIN

DECLARE @ETLLoadID        INT,
		@ExtractRowCount  INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
				
SET NOCOUNT ON;

	BEGIN TRY
            
--Run the Audit begin
EXEC @ETLLoadID = EDW.dbo.[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimStockFeature'

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM EDW.dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'
--'Starting [CSAT_Vehicle_Features] update to remove existing records which are going to be re-inserted'

IF OBJECT_ID('tempdb.dbo.#Tmp_Updated') IS NOT NULL DROP TABLE #Tmp_Updated
CREATE TABLE #Tmp_Updated (Salvage_ID INT NOT NULL)
 
INSERT INTO #Tmp_Updated (Salvage_ID)
SELECT DISTINCT SS.Salvage_ID 
FROM (SELECT * FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Feature] (
   @StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5) SS

	
CREATE TABLE [#CSAT_Vehicle_Features_Temp] (	
	[StockID] [int] NOT NULL,
	[SalvageFeatureSequenceNumber] [int] NOT NULL,
	[FeatureCode] [char](4) NULL,
	[FeatureValue] [varchar](200) NULL,
	[FeatureQuantity] [decimal](7, 2) NULL,
	[FeatureInteger] [int] NULL,
	[FeatureInd] [bit] NOT NULL,
	[SalvageFeatureGroupNumber] [int] NULL,
	[ASAPUpdateDateTime] [datetime] NULL,
	[FeatureDescription] [varchar](40) NULL)

INSERT INTO [#CSAT_Vehicle_Features_Temp] (
	[StockID],
	[SalvageFeatureSequenceNumber],
	[FeatureCode],
	[FeatureValue],
	[FeatureQuantity],
	[FeatureInteger],
	[FeatureInd],
	[SalvageFeatureGroupNumber],
	[ASAPUpdateDateTime],
	[FeatureDescription])
	 
SELECT
	DS.StockID,
	SF.Salvage_Feature_Sequence_Number,
	SF.[Feature_Code],
		SF.[Feature_Value],
		SF.[Feature_Quantity],
		SF.[Feature_Integer],
		SF.[Feature_Ind],
		SF.[Salvage_Feature_Group_Number],
		SF.[Update_DateTime],
		F.[Description]
FROM #Tmp_Updated tu WITH (NOLOCK)
INNER JOIN [BI_ASAP_Rep].dbo.[Salvage_Feature] SF WITH (NOLOCK)
	ON sf.Salvage_ID = tu.Salvage_ID
  INNER JOIN [BI_ASAP_Rep].dbo.Feature F WITH (NOLOCK)
	ON SF.Feature_Code = F.Feature_Code	
INNER JOIN [EDW].dbo.[DimStock] DS WITH (NOLOCK)
	ON sf.Salvage_ID = DS.ASAPSalvageID

IF OBJECT_ID('tempdb.dbo.#temp_RecordCount') IS NOT NULL DROP TABLE #temp_RecordCount
CREATE TABLE #temp_RecordCount(ChangeType VARCHAR(10)) 

BEGIN TRAN

 	MERGE dbo.[DimStockFeature] AS DSF
	USING #CSAT_Vehicle_Features_Temp AS VFT    
		ON DSF.[StockID] = VFT.[StockID]
	AND DSF.[SalvageFeatureSequenceNumber] = VFT.[SalvageFeatureSequenceNumber]
	WHEN NOT MATCHED  THEN 
	--Row exists in source but not in target
	INSERT ([StockID]
      ,[SalvageFeatureSequenceNumber]
      ,[FeatureCode]
      ,[FeatureValue]
      ,[FeatureQuantity]
      ,[FeatureInteger]
      ,[FeatureInd]
      ,[SalvageFeatureGroupNumber]
      ,[ASAPUpdateDateTime]
      ,[FeatureDescription]
      ,[ETLLoadID]
      ,[ETLLoadUpdateID])
	VALUES (
		[StockID]
		,[SalvageFeatureSequenceNumber]
		,[FeatureCode]
		,[FeatureValue]
		,[FeatureQuantity]
		,[FeatureInteger]
		,[FeatureInd]
		,[SalvageFeatureGroupNumber]
		,[ASAPUpdateDateTime]
		,[FeatureDescription]
		,@ETLLoadID
		,@ETLLoadID)
	WHEN MATCHED  THEN
	UPDATE SET 
		DSF.[StockID]= VFT.[StockID],
		DSF.[SalvageFeatureSequenceNumber]= VFT.[SalvageFeatureSequenceNumber],
		DSF.[FeatureCode]= VFT.[FeatureCode],
		DSF.[FeatureValue]= VFT.[FeatureValue],
		DSF.[FeatureQuantity]= VFT.[FeatureQuantity],
		DSF.[FeatureInteger]= VFT.[FeatureInteger],
		DSF.[FeatureInd]= VFT.[FeatureInd],
		DSF.[SalvageFeatureGroupNumber]= VFT.[SalvageFeatureGroupNumber],
		DSF.[ASAPUpdateDateTime]= VFT.[ASAPUpdateDateTime],
		DSF.[FeatureDescription]= VFT.[FeatureDescription],
		DSF.[ETLLoadID]= @ETLLoadID,
		DSF.[ETLLoadUpdateID]= @ETLLoadID
OUTPUT $action INTO #temp_RecordCount;
--select * from dimbranch
SELECT @InsertRowCount = COUNT(*)  FROM #temp_RecordCount WHERE ChangeType = 'INSERT'
SELECT @UpdateRowCount = COUNT(*)  FROM #temp_RecordCount WHERE ChangeType = 'UPDATE'

COMMIT TRAN

	EXEC [dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount,
		@UpdateRowCount = @UpdateRowCount
		
		IF OBJECT_ID('tempdb.dbo.#CSAT_Vehicle_Features_Temp') IS NOT NULL DROP TABLE #CSAT_Vehicle_Features_Temp
		IF OBJECT_ID('tempdb.dbo.#temp_RecordCount') IS NOT NULL DROP TABLE #temp_RecordCount
		IF OBJECT_ID('tempdb.dbo.#Tmp_Updated') IS NOT NULL DROP TABLE #Tmp_Updated

	RETURN 0
END TRY

BEGIN CATCH

	SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 
		ROLLBACK TRAN
	
	EXEC EDW.[dbo].[usp_ETLLoadAudit]
		@StartEnd = 'E',
		@ETLLoadID = @ETLLoadID,
		@ExtractRowCount = @ExtractRowCount,
		@InsertRowCount = @InsertRowCount, 
		@UpdateRowCount = @UpdateRowCount,
		@ErrorCode = @@ERROR
		
		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
		
		IF OBJECT_ID('tempdb.dbo.#CSAT_Vehicle_Features_Temp') IS NOT NULL DROP TABLE #CSAT_Vehicle_Features_Temp
		IF OBJECT_ID('tempdb.dbo.#temp_RecordCount') IS NOT NULL DROP TABLE #temp_RecordCount
		IF OBJECT_ID('tempdb.dbo.#Tmp_Updated') IS NOT NULL DROP TABLE #Tmp_Updated

		RETURN -1
	

END CATCH

END