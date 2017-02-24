/*
==============================================================================
Author		: Bruce Kutnick
Create date	: 12/4/2013
Description	: Maintains up to date master vehicle make list for DQS projects
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number

==============================================================================
Usage Example:
 
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_dqsUpdateDimStockVehicleMake]
AS

BEGIN


SET	NOCOUNT ON

TRUNCATE TABLE STAGING.[dbo].[dqsMakeNameStaging]

DECLARE @Temp1 TABLE
		(
		MakeName_Source nvarchar(35),
		MakeName_Output nvarchar(35)
		)
INSERT	INTO @Temp1
		(
		MakeName_Source,
		MakeName_Output 
		)

SELECT	DISTINCT [MakeName_Source]
				,[MakeName_Output]
FROM	[STAGING].[dbo].[dqsMakeCleansingOutput] WITH (NOLOCK)
--WHERE	MakeName_reason like '%Corrected%' OR ([MakeName_Reason] = 'Modified by user' AND [MakeName_Status] = 'Corrected')
WHERE	MakeName_reason like '%Corrected%' OR 
([MakeName_Reason] = 'DQS cleansing' AND [MakeName_Status] = 'Corrected')
OR ([MakeName_Reason] = 'Modified by user' AND [MakeName_Status] = 'Corrected')
 
INSERT	INTO  STAGING.[dbo].[dqsMakeNameStaging]
		(
		[StockID_Output],
		[MakeName_Source],
		[MakeName_Output],
		StockNumber,
		ASAPSalvageID,
		[LastModifiedDate]
		)
SELECT	a.stockid,
		b.MakeName_Source,
		b.[MakeName_Output],
		a.StockNumber,
		a.ASAPSalvageID,
		GETDATE()
		 
FROM	STAGING.[dbo].[dqsMakeToCorrect] a
		INNER JOIN @Temp1 b
			ON b.[MakeName_Source] = a.MakeName


BEGIN TRY
    BEGIN TRANSACTION

	UPDATE	EDW.dbo.DimStock
	SET		MakeName =  a.MakeName_Output
	FROM	EDW.dbo.DimStock d WITH (NOLOCK)
			INNER JOIN STAGING.dbo.[dqsMakeNameStaging] a WITH (NOLOCK)
				ON a.StockID_Output = d.StockID
	WHERE D.IsEVMUpdated = 0

	TRUNCATE TABLE [STAGING].[dbo].[dqsMakeCleansingOutput] 
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


GO


