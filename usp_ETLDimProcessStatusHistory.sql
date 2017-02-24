
/*
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 08/29/2016
Description	: Populate the EDW DimProcessStatusHistory table from CDC tables.
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number	

===================================================================================================================================================================
Usage Example:
Exec EDW.dbo.[usp_ETLDimProcessStatusHistory]
===================================================================================================================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimProcessStatusHistory]

AS     
			
SET NOCOUNT ON;
BEGIN TRY

CREATE TABLE #TMPDimProcessStatusHistory(
	[ProcessStatusUpdateDateTime] [datetime] NULL,
	[StockID] [int] NOT NULL,
	[ProcessStatusCode] [char](3) NULL,
	[ProcessStatusDescription] [varchar](40) NULL) 

INSERT INTO #TMPDimProcessStatusHistory
           ([ProcessStatusUpdateDateTime]
           ,[StockID]
           ,[ProcessStatusCode]
           ,[ProcessStatusDescription])
SELECT
ProcessStatusUpdateDateTime = MIN([BI_ASAP_Rep].sys.fn_cdc_map_lsn_to_time ( [__$start_lsn] ))
      ,DS.StockID
      ,SI.Process_Status_Code AS ProcessStatusCode, 
	  S2.Status_Description AS ProcessStatusDescription
  FROM [BI_ASAP_Rep].[cdc].[dbo_Salvage_Info_CT] SI WITH (NOLOCK)
	LEFT OUTER JOIN BI_ASAP_Rep.dbo.[Status] AS S2 WITH(NOLOCK)  
		ON SI.Process_Status_Code = S2.Status_Code
INNER JOIN EDW.dbo.dimstock DS WITH (NOLOCK)
	ON SI.Salvage_ID = DS.ASAPSalvageID
WHERE SI.Process_Status_Code IS NOT NULL
  GROUP BY      DS.StockID      ,SI.[Process_Status_Code], S2.Status_Description
  ORDER BY 2,1

BEGIN TRANSACTION 
  
INSERT INTO [dbo].[DimProcessStatusHistory]
           ([ProcessStatusUpdateDateTime]
           ,[StockID]
           ,[ProcessStatusCode]
           ,[ProcessStatusDescription])
SELECT TDPSH.[ProcessStatusUpdateDateTime]
      ,TDPSH.[StockID]
      ,TDPSH.[ProcessStatusCode]
      ,TDPSH.[ProcessStatusDescription]
FROM #TMPDimProcessStatusHistory TDPSH WITH (NOLOCK)
LEFT OUTER JOIN  [dbo].[DimProcessStatusHistory] DPSH WITH (NOLOCK)
	ON TDPSH.[ProcessStatusUpdateDateTime] = DPSH.[ProcessStatusUpdateDateTime]
		AND TDPSH.[StockID] = DPSH.[StockID]
		AND  TDPSH.[ProcessStatusCode] = DPSH.[ProcessStatusCode]
WHERE DPSH.DimProcessStatusHistoryID IS NULL

--DELETE FROM [dbo].[DimProcessStatusHistory] where [ProcessStatusCode] = 'ACT'ORDER BY [StockID],[ProcessStatusUpdateDateTime],[ProcessStatusCode]

COMMIT TRAN

IF OBJECT_ID('tempdb..#TMPDimProcessStatusHistory') IS NOT NULL DROP TABLE #TMPDimProcessStatusHistory

RETURN 0

END TRY

BEGIN CATCH

	SET NOCOUNT OFF
	IF @@TRANCOUNT > 0 ROLLBACK TRAN
		
		IF OBJECT_ID('tempdb..#TMPDimProcessStatusHistory') IS NOT NULL DROP TABLE #TMPDimProcessStatusHistory


		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	

END CATCH