
/*
USE [EDW]
GO
==============================================================================
Author		: Bruce Kutnick
Create date	: 1/14/2014
Description	: Updates DIMSTOCK table with cleansed Vehicle Models
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number
3-10-2014	B Kutnick		Added delete statement so that we are not updating MAKENAME in DimStock with any NULL Values
==============================================================================
Usage Example:
 
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_dqsUpdateDimStockVehicleModel]
AS

BEGIN

DECLARE @RecordCounter int
SELECT 	@RecordCounter = Count(Record_Status) from  [STAGING].[dbo].[dqsModelCleansingOutput] 

/***********************************************************************************************/
-- Only execute the code if there is data in the STAGING.[dbo].[dqsModelCleansingOutput] table
/***********************************************************************************************/

IF		@RecordCounter > 0
BEGIN	TRY
	BEGIN TRANSACTION

	UPDATE	[Staging].[dbo].[dqsModelToCorrect]
	SET		ModelNameCleansed = o.[ModelName_Output]
	FROM	STAGING.[dbo].[dqsModelCleansingOutput] o WITH (NOLOCK)
			INNER JOIN [Staging].[dbo].[dqsModelToCorrect] c WITH (NOLOCK)
				ON c.ModelName = o.[ModelName_Source]
	WHERE	o.[ModelName_Status] = 'Corrected'
		OR 
	([ModelName_Reason] = 'DQS cleansing' AND [ModelName_Status] = 'Corrected')
	OR ([ModelName_Reason] = 'Modified by user' AND [ModelName_Status] = 'Corrected')
	

	-- added 3-10-2014
	DELETE FROM [Staging].[dbo].[dqsModelToCorrect] where ModelNameCleansed IS NULL

	UPDATE	EDW.dbo.DimStock
	SET		ModelName =  a.[ModelNameCleansed] 
	FROM	EDW.dbo.DimStock d WITH (NOLOCK)
			INNER JOIN STAGING.dbo.[dqsModelToCorrect] a WITH (NOLOCK)
				ON a.StockID = d.StockID
	WHERE D.IsEVMUpdated = 0
		
	TRUNCATE TABLE [STAGING].[dbo].[dqsModelCleansingOutput] 
	
	COMMIT TRANSACTION
END TRY


BEGIN CATCH        
	IF @@TRANCOUNT > 0
	BEGIN
		
		TRUNCATE TABLE [STAGING].[dbo].[dqsMakeCleansingOutput]
		ROLLBACK TRANSACTION 
	END
		
	-- Raise an error with the details of the exception
	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	SELECT @ErrMsg = ERROR_MESSAGE(),
			@ErrSeverity = ERROR_SEVERITY()

	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH

END

 -- exec usp_dqsUpdateDimStockVehicleModel


GO


