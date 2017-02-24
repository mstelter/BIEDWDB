/* 
USE EDW
GO 
==============================================================================
Author		: Bruce Kutnick
Create date	: 1/30/2014
Description	: Process a Nightly update to errant vehicle makes that are mapped to
              correct entries in the KBase
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	
2/4/2014	BKutnick		Added recursive code to find MAX(ETLLoadID) from 2 runs prior to last run
==============================================================================

*/

CREATE	PROCEDURE [dbo].[usp_dqsUpdateDimStockVehicleMakeNIGHTLY]
--ALTER 	PROCEDURE [dbo].[dqsUpdateDimStockVehicleMakeNIGHTLY]
AS
BEGIN 

SET NOCOUNT ON

/*********************************************/
/*   Declare variables for processing data   */
/*********************************************/
--DECLARE	@ETLLoadUpdateID int
DECLARE @RecordCounter int

DECLARE @MaxETLLoadUpdateID int
DECLARE @PriorETLLoadUpdateID int
DECLARE  @FloorETLLoadUpdateID int

SET		@MaxETLLoadUpdateID = (select  max(ETLLoadUpdateID) from EDW.dbo.DimStock WITH (NOLOCK) ) 
SET		@PriorETLLoadUpdateID =(select  max(ETLLoadUpdateID) from EDW.dbo.DimStock WITH (NOLOCK) WHERE ETLLoadUpdateID < @MaxEtlLoadUpdateID )
SET		@FloorETLLoadUpdateID =  (select  max(ETLLoadUpdateID) from EDW.dbo.DimStock WITH (NOLOCK) WHERE ETLLoadUpdateID < @PriorETLLoadUpdateID )



/****************************************************/
/* Find the latest ETLLoadUpdateID for DIMSTOCK		*/
/****************************************************/
----SELECT  @MaxETLLoadUpdateID = MAX(EtlLoadUpdateID)   
----FROM	[EDW].[dbo].[DimStock] WITH (NOLOCK)

 
/****************************************************/
/* Create a temp table and put all of the rows that */
/* need to be corrected in this temp table			*/
/****************************************************/
CREATE	TABLE #tempUpdateVehicleMake
		(
		StockID int,
		MakeName varchar(25),
		MapToMake varchar(25),
		FloorLoadUpdateID int
		)
INSERT INTO #tempUpdateVehicleMake
		(
		StockID,
		MakeName,
		MapToMake,
		FloorLoadUpdateID 
		)
SELECT	StockID,
		a.Makename,
		vmm.MapToMake,
		@FloorETLLoadUpdateID
FROM	EDW.dbo.DimStock  a WITH (NOLOCK)
		INNER JOIN DQS_STAGING_DATA.dbo.VehicleMakesMapping vmm WITH (NOLOCK)
			ON vmm.MakeName = a.MakeName
--WHERE	a.etlLoadUpdateID = @ETLLoadUpdateID 
WHERE	a.etlLoadUpdateID >= @FloorETLLoadUpdateID
AND		a.StockType = 1
AND		NOT a.MakeName = vmm.MapToMake


CREATE	CLUSTERED INDEX ix1 ON #TempUpdateVehicleMake(StockID)      
/***********************************************************************************************/
-- Only execute the code if there is data in the temp table
/***********************************************************************************************/
SELECT 	@RecordCounter = Count(*) from #tempUpdateVehicleMake

IF		@RecordCounter > 0
BEGIN	TRY
	BEGIN TRANSACTION

	UPDATE	EDW.dbo.DimStock
	SET		MakeName =  a.[MapToMake] 
	FROM	EDW.dbo.DimStock d WITH (NOLOCK)
			INNER JOIN #tempUpdateVehicleMake a WITH (NOLOCK)
				ON a.StockID = d.StockID
	WHERE D.IsEVMUpdated = 0

	COMMIT TRANSACTION
END TRY


BEGIN CATCH        
	IF @@TRANCOUNT > 0
	BEGIN
		
		ROLLBACK TRANSACTION 
	END
		
	-- Raise an error with the details of the exception
	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	SELECT @ErrMsg = ERROR_MESSAGE(),
			@ErrSeverity = ERROR_SEVERITY()

	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH

DROP TABLE #tempUpdateVehicleMake
END


GO


