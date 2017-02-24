CREATE PROCEDURE [dbo].[usp_EDWArchiveStockSelection]

--====================================================================================--
--====================================================================================--
-- Description:	Migrates data (archives) from EDW table to EDW_Archive table
-- Usage:		EXEC usp_EDWArchiveStockSelection @Debug = 1
-- Author:		Mohammad Waheed
-- Revision History:
-- 	Date		Programmer		Description
--	-------		------------------	-------------------


--====================================================================================--
--====================================================================================--
( 
	@Debug BIT = 0 
)

AS

SET NOCOUNT ON

DECLARE @RecordCounter INT, @err INT, @msg VARCHAR(200), @SPROC_Name VARCHAR(100)
SELECT @RecordCounter = 0, @err = 0, @msg = '', @SPROC_Name = OBJECT_NAME(@@PROCID) 

IF @Debug = 1
BEGIN
	SELECT [EDWArchiveVehicleSelectionID]
      ,[StockID]
      ,[ArchiveDate]
      ,[CDDate]
      ,[CDUser]
		FROM [dbo].[EDWArchiveStockSelection] WITH(NOLOCK)	
END 

MERGE EDW_Archive.[dbo].[EDWArchiveStockSelectionArchive] AS DST
USING [EDW].[dbo].[EDWArchiveStockSelection] AS SRC		   ON SRC.[StockID] = DST.[StockID] 
WHEN MATCHED THEN
	UPDATE SET  [ArchiveDate]=SRC.[ArchiveDate],
							 [CDDate]=SRC.[CDDate],
	                         [CDUser]=SRC.[CDUser]
WHEN NOT MATCHED THEN
 INSERT  ([EDWArchiveVehicleSelectionID]
				,[StockID]
				,[ArchiveDate]
				,[CDDate]
				,[CDUser]
				)
VALUES(  SRC.[EDWArchiveVehicleSelectionID]
				,SRC.[StockID]
				,SRC.[ArchiveDate]
				,SRC.[CDDate]
				,SRC.[CDUser]
			  );

	SELECT @err = @@ERROR, @RecordCounter = @@ROWCOUNT

	IF @err <> 0
		BEGIN
			SELECT @msg = 'Unable to Insert into StockSelectionArchive !!'

			GOTO FAILED
		END

SELECT @msg = 'Finish: StockSelectionArchive Insert. No of recs inserted: ' + RTRIM(LTRIM(CONVERT(VARCHAR(20), @RecordCounter)))


SET NOCOUNT OFF

SUCCESS:
	SELECT @msg = 'Successful Run: ' + RTRIM(LTRIM(@SPROC_Name))
	RETURN 0
FAILED:
	RAISERROR( '%s @@error=%d', 16, 1, @msg, @err )
	RETURN -1

