CREATE PROCEDURE [dbo].[usp_EDWArchiveSelectStocks]
/*
--====================================================================================--
--====================================================================================--
-- Description:	Archive stocks older than 10 years from current day that fulfill the following conditions:
--					    1) Sold and Cancelled dates
--      				2) The archiving stocks should not present in Salvage table from BI_Asap_Rep DB
-- Usage:			EXEC [usp_EDWArchiveSelectStocks]
-- Author:			Mohammad Waheed
-- Revision History:
-- 	Date		Programmer		Description
--	-------		------------------	-------------------

--====================================================================================--
Select * from dbo.EDWArchiveStockSelection
--====================================================================================--
*/
AS

SET NOCOUNT ON

DECLARE @RecordCounter INT, @err INT, @msg VARCHAR(200)
--, @SPROC_Name VARCHAR(100)
SELECT @RecordCounter = 0, @err = 0, @msg = ''
--, @SPROC_Name = OBJECT_NAME(@@PROCID) 


DECLARE @TableName VARCHAR(150), @DBName CHAR(20), @Sql VARCHAR(8000), @ArchiveInsertColumns VARCHAR(8000), @JoinColumns VARCHAR(150)
--, @NoColumns INT, 
--@ArchiveColumn VARCHAR(200)
		, @NoRecordsArchived INT,@params NVARCHAR(MAX),@ErrorMessage NVARCHAR(4000), @ErrorSeverity INT, @ErrorState INT;
SELECT @Sql = '', @ArchiveInsertColumns = ''

DECLARE @ArchiveDate Varchar(20)
SET @ArchiveDate =dbo.fn_archive_date()
--Convert(varchar(20),DateAdd(mm,0,DateAdd(yy,-15,GETDATE())),112)



	BEGIN TRY -- Selecting Stocks from DimStock based on condition not exists in Bi_Asap_Rep.Dbo.Salvage table and the archive date filter for sold and Canceled Stocks

			INSERT INTO dbo.EDWArchiveStockSelection ( StockId, ArchiveDate )
			SELECT ds.StockId,@ArchiveDate
			FROM [EDW].[dbo].FactStockCycleTime  fsct With (Nolock)
			INNER JOIN EDW.dbo.DimStock ds with (Nolock) on fsct.StockID=ds.StockID  
			WHERE  ([CancelDayID]<@ArchiveDate or [SoldDayID]<@ArchiveDate)
			                AND NOT EXISTS ( SELECT TOP 1 * 
															  FROM BI_ASAP_Rep.dbo.Salvage s With (nolock)
															  WHERE ds.AsapSalvageID=s.Salvage_ID)

			SELECT @RecordCounter = @@ROWCOUNT

	END TRY 
	BEGIN CATCH 
		SELECT 
					@ErrorMessage = 'Unable to Insert into EDWArchiveStockSelection!! System Error Message: ' + ERROR_MESSAGE(),
					@ErrorSeverity = ERROR_SEVERITY(),
					@ErrorState = ERROR_STATE();

				-- Use RAISERROR inside the CATCH block to return error
				-- information about the original error that caused
				-- execution to jump to the CATCH block.
				RAISERROR (@ErrorMessage, -- Message text.
					   @ErrorSeverity, -- Severity.
					   @ErrorState -- State.
					   );
	END CATCH 	   


SET NOCOUNT OFF




