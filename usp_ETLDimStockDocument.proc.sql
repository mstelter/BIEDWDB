
/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 1/7/2013
Description	: Populate the EDW DimStockDocument table from CDC	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
1/10/2013	Matt Stelter		Fixed Image Ind
==============================================================================
Usage Example:
Exec usp_ETLDimStockDocument
--SELECT * FROM DimStockDocument order by 3 desc
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimStockDocument]
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
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLDimStockDocument'
SELECT @MinAllowableDate = MIN(DayDate), @MaxAllowableDate = MAX(DayDate) from DimDay WITH (NOLOCK) WHERE DayID > 19000101 and DayID < 29991231

--Get the start and end LSNs
SELECT @StartLSN = MinLSN, @EndLSN = MaxLSN FROM dbo.ETLRun WITH (NOLOCK) WHERE ETLJobName = 'DailyETLJob'

/*********************************************************************************************
Create the temp table #TMP_DimStockDocument
*********************************************************************************************/
CREATE TABLE #TMP_DimStockDocument
(Salvage_Document_ID INT,
	StockID INT,
	Salvage_Document_Reference_ID INT,
	Create_DateTime DATETIME,
	IsHasDocumentImage TINYINT,
	Salvage_Document_Reference_Name VARCHAR(30),
	StockDocumentTypeCode VARCHAR(3),
	StockDocumentTypeDescription VARCHAR(30))

CREATE TABLE #temp_DimStockDocumentCount(ChangeType VARCHAR(30)) 

/*********************************************************************************************
Insert the values from the CDC Current Salvage Document table.
*********************************************************************************************/
SELECT @StartLSN = MIN(__$start_lsn), @EndLSN = MAX(__$start_lsn) FROM [BI_ASAP_Rep].[cdc].[dbo_Salvage_Document_CT]

INSERT INTO #TMP_DimStockDocument
(Salvage_Document_ID,
	StockID,
	Salvage_Document_Reference_ID,
	Create_DateTime,
	IsHasDocumentImage,
	Salvage_Document_Reference_Name,
	StockDocumentTypeCode,
	StockDocumentTypeDescription)
SELECT SD.Salvage_Document_ID,
       DS.StockID,
       SD.Salvage_Document_Reference_ID,
       SD.Create_DateTime,
       CASE WHEN ISNULL(SD.Image_ID,0) = 0 THEN 0 ELSE 1 END AS IsHasDocumentImage,
       SDR.Salvage_Document_Reference_Name,
       SDR.Salvage_Document_Type_Code,
       SDT.Salvage_Document_Type_Description
FROM (SELECT * FROM [BI_ASAP_Rep].[cdc].[fn_cdc_get_net_changes_dbo_Salvage_Document](@StartLSN,@EndLSN,'all with merge') WHERE [__$operation] = 5) SD
--FROM   [BI_ASAP_Rep].dbo.Salvage_Document AS SD WITH (NOLOCK)
       INNER JOIN [BI_ASAP_Rep].dbo.Salvage_Document_Reference AS SDR WITH (NOLOCK)
         ON SD.Salvage_Document_Reference_ID=SDR.Salvage_Document_Reference_ID
       INNER JOIN DBO.DimStock DS WITH (NOLOCK)
		ON DS.ASAPSalvageID = SD.Salvage_ID 	
       INNER JOIN [BI_ASAP_Rep].dbo.Salvage_Document_Type AS SDT WITH (NOLOCK)
         ON SDR.Salvage_Document_Type_Code=SDT.Salvage_Document_Type_Code
         
         --SELECT * FROM #TMP_DimStockDocument
/*********************************************************************************************
MERGE the data from the temp table into the EDW DimStockDocument table
This does Update and Insert only.  No Deletes
*********************************************************************************************/
BEGIN TRANSACTION 
  
    MERGE dbo.DimStockDocument AS DSD
    USING #TMP_DimStockDocument AS TDSD
          ON DSD.StockDocumentID = TDSD.Salvage_Document_ID
    WHEN MATCHED THEN 
    UPDATE SET 
			StockDocumentID = TDSD.Salvage_Document_ID,
			StockID = TDSD.StockID,
			CreateDateTime = TDSD.Create_DateTime,
			IsHasDocumentImage = TDSD.IsHasDocumentImage,
			StockDocumentReferenceID = TDSD.Salvage_Document_Reference_ID,
			StockDocumentReferenceName = TDSD.Salvage_Document_Reference_Name,
			StockDocumentTypeCode = TDSD.StockDocumentTypeCode,
			StockDocumentTypeDescription = TDSD.StockDocumentTypeDescription,
			ETLLoadUpdateID = @ETLLoadID	
   WHEN NOT MATCHED THEN
   INSERT(StockDocumentID
			,StockID
			,CreateDateTime
			,IsHasDocumentImage
			,StockDocumentReferenceID
			,StockDocumentReferenceName
			,StockDocumentTypeCode
			,StockDocumentTypeDescription
			,ETLLoadID
			,ETLLoadUpdateID)	 
	VALUES (TDSD.Salvage_Document_ID,
			TDSD.StockID,
			TDSD.Create_DateTime,
			TDSD.IsHasDocumentImage,
			TDSD.Salvage_Document_Reference_ID,
			TDSD.Salvage_Document_Reference_Name,
			TDSD.StockDocumentTypeCode,
			TDSD.StockDocumentTypeDescription,			
			@ETLLoadID,
			@ETLLoadID)


OUTPUT $action INTO #temp_DimStockDocumentCount;
	  
/*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #temp_DimStockDocumentCount

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN
			DROP TABLE #TMP_DimStockDocument
			DROP TABLE #temp_DimStockDocumentCount
		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
			ROLLBACK TRAN
		
			IF OBJECT_ID('tempdb..#TMP_DimStockDocument') IS NOT NULL DROP TABLE #TMP_DimStockDocument
			IF OBJECT_ID('tempdb..#temp_DimStockDocumentCount') IS NOT NULL DROP TABLE #temp_DimStockDocumentCount
		EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	END CATCH
	END


