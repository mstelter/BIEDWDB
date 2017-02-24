/* 
USE EDW
GO 
==============================================================================
Author		: Bruce Kutnick
Create date	: 1/30/2014
Description	: Process a Nightly update to errant vehicle MODELS that are mapped to
              correct entries in the KBase
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	
2/4/2014	BKutnick		Added recursive code to find MAX(ETLLoadID) from 2 runs prior to last run
==============================================================================

*/
CREATE	PROCEDURE [dbo].[usp_dqsUpdateDimStockVehicleModelNIGHTLY]
--ALTER 	PROCEDURE [dbo].[dqsUpdateDimStockVehicleModelNIGHTLY]
AS
BEGIN

SET NOCOUNT ON

/*********************************************/
/*   Declare variables for processing data   */
/*********************************************/
 
DECLARE @RecordCounter  int
DECLARE @MaxETLLoadUpdateID int
DECLARE @PriorETLLoadUpdateID int
DECLARE  @FloorETLLoadUpdateID int

SET		@MaxETLLoadUpdateID = (select  max(ETLLoadUpdateID) from EDW.dbo.DimStock WITH (NOLOCK) ) 
SET		@PriorETLLoadUpdateID =(select  max(ETLLoadUpdateID) from EDW.dbo.DimStock WITH (NOLOCK) WHERE ETLLoadUpdateID < @MaxEtlLoadUpdateID )
SET		@FloorETLLoadUpdateID =  (select  max(ETLLoadUpdateID) from EDW.dbo.DimStock WITH (NOLOCK) WHERE ETLLoadUpdateID < @PriorETLLoadUpdateID )
 
 
 
/****************************************************/
/* Create a temp table and put all of the rows that */
/* need to be corrected in this temp table			*/
/****************************************************/
CREATE	TABLE #tempUpdateVehicleModel
		(
		StockID int,
		ModelName varchar(25),
		MapToModel varchar(25),
		FloorLoadUpdateID int
		)
INSERT INTO #tempUpdateVehicleModel
		(
		StockID,
		ModelName,
		MapToModel,
		FloorLoadUpdateID 
		)
SELECT	StockID,
		a.Modelname,
		vmm.MapToModel,
		@FloorETLLoadUpdateID 
FROM	EDW.dbo.DimStock  a WITH (NOLOCK)
		INNER JOIN DQS_STAGING_DATA.dbo.VehicleModelsMapping vmm WITH (NOLOCK)
			ON vmm.ModelName = a.ModelName
WHERE	a.etlLoadUpdateID >= @FloorETLLoadUpdateID
AND		a.StockType = 1
AND		NOT a.ModelName = vmm.MapToModel

CREATE	CLUSTERED INDEX ix1 ON #TempUpdateVehicleModel (StockID)

 
/***********************************************************************************************/
-- Only execute the code if there is data in the temp table
/***********************************************************************************************/
SELECT 	@RecordCounter = Count(*) from #tempUpdateVehicleModel

IF		@RecordCounter > 0
BEGIN	TRY
	BEGIN TRANSACTION

	UPDATE	EDW.dbo.DimStock
	SET		ModelName =  a.[MapToModel] 
	FROM	EDW.dbo.DimStock d WITH (NOLOCK)
			INNER JOIN #tempUpdateVehicleModel a WITH (NOLOCK)
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

DROP TABLE #tempUpdateVehicleModel
END

