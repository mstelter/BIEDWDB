CREATE PROCEDURE [dbo].[usp_EDWArchiveTables]

--====================================================================================--
--====================================================================================--
-- Description:	Migrates data (archives) from EDW table to EDW_Archive table
-- Usage:		EXEC [usp_EDWArchiveTables] @Debug = 1
-- Author:		Mohammad Waheed
--Revision History:
--ModifiedDate		Programmer		Description
--	-------		        ---------------	        -------------------

--====================================================================================--
--====================================================================================--
( 
	@Debug BIT = 0 
)

AS

SET NOCOUNT ON

/*
Declare @Debug BIT
SET @Debug = 0
--*/

DECLARE @RecordCounter INT, @err INT, @msg VARCHAR(200)
--, @SPROC_Name VARCHAR(100)
SELECT @RecordCounter = 0, @err = 0, @msg = ''
--, @SPROC_Name = OBJECT_NAME(@@PROCID) 

DECLARE @TableName VARCHAR(150), @DBName CHAR(20), @Sql VARCHAR(Max), @ArchiveInsertColumns VARCHAR(Max), @JoinColumns VARCHAR(150)
, @NoColumns INT, 
  @ArchiveColumn VARCHAR(200)
		, @NoRecordsArchived INT,@ErrorMessage NVARCHAR(4000), @ErrorSeverity INT, @ErrorState INT;
		
SELECT @Sql = '', @ArchiveInsertColumns = ''

BEGIN TRY 

	Create TABLE #Tables_To_Arhive  (
		[Table_Name] VARCHAR(150),
		[DBName] CHAR(20),
		[Join_Columns] VARCHAR(150) )
		
	INSERT INTO #Tables_To_Arhive ( [Table_Name], [DBName], [Join_Columns])
	SELECT 
		[Table_Name] = [TableName],
		[DBName] = [DBName],
		[Join_Columns] = [JoinColumns]
	FROM [dbo].[EDWArchiveTableSelection] WITH (NOLOCK) --WHERE [TableName] <> 'CSAT_Vehicle_Detail_load'
		SELECT @RecordCounter = @@ROWCOUNT
		
	CREATE TABLE #TableSchema_Archive  ( 
		[DBName] NVARCHAR(256),
		[Table_Name] NVARCHAR(256),
		[Column_ID] INT,
		[Column_Name] NVARCHAR(256),
		[Type] NVARCHAR(256),
		[Computed] VARCHAR(30),
		[Length] INT,
		[Prec] VARCHAR(5),
		[Scale]  VARCHAR(5),
		[Nullable]  VARCHAR(35),
		[TrimTrailingBlanks]  VARCHAR(35),
		[FixedLenNullInSource]  VARCHAR(35),
		[Collation] NVARCHAR(256) )

	WHILE @RecordCounter > 0
		BEGIN
			SELECT TOP 1
				@TableName = [Table_Name],
				@DBName = [DBName],
				@JoinColumns = [Join_Columns]
			FROM #Tables_To_Arhive

	-- if table is dropped from Actual Database , then no need to check tables existance in Archive Database
			IF EXISTS (SELECT 1 FROM dbo.sysobjects WITH (NOLOCK) WHERE NAME = @tablename AND xtype = 'U') 
			BEGIN 	
				SELECT @msg = 'Starting: ' + RTRIM(LTRIM(@DBName)) + '.dbo.' + RTRIM(LTRIM(@TableName)) + ' Data Move'
				--EXEC usp_csat_dw_log @SPROC_Name, @msg
				
				SELECT @Sql = '
				SELECT 
					[DBName],
					[Table_Name],
					[Column_ID],
					[Column_Name],
					[Type],
					[Computed],
					[Length],
					[Prec],
					[Scale],
					[Nullable],
					[TrimTrailingBlanks],
					[FixedLenNullInSource],
					[Collation]
				FROM ' + RTRIM(LTRIM(@DBName)) + '_Archive.dbo.[uf_Get_Table_Schema]( ''' + RTRIM(LTRIM(@TableName)) + '_Archive''); '
				
				Print @sql
				INSERT INTO #TableSchema_Archive
				EXEC ( @Sql )
				
					
			BEGIN TRY 
				-- step 1: build columns for insert
					SELECT @Sql = '', @ArchiveInsertColumns = ''
					
					SELECT 	@ArchiveInsertColumns = @ArchiveInsertColumns + '[' + LTRIM(RTRIM(tsa.[Column_Name])) + ']' + ', '
					FROM #TableSchema_Archive tsa WITH (NOLOCK)

					SELECT @ArchiveInsertColumns = SUBSTRING(@ArchiveInsertColumns, 1, LEN(@ArchiveInsertColumns) - 1)
					
					SELECT @Sql = 'INSERT INTO ' + RTRIM(LTRIM(@DBName)) + '_Archive.dbo.' + RTRIM(LTRIM(@TableName)) + '_Archive ( ' + RTRIM(LTRIM(@ArchiveInsertColumns)) + ')'
					
		-- step 2: build columns for select, a different approach to building the string is applied here because we run into issues if the string is greater than 4000 chars as the trick
		-- to loop on itself (i.e. @v = @v + 'string') code doubles in size w/ each pass
					SELECT @ArchiveInsertColumns = '', @NoColumns = 1, @ArchiveColumn = ''
					
					WHILE @NoColumns BETWEEN ( SELECT MIN(Column_ID) FROM #TableSchema_Archive) AND (SELECT  MAX(Column_ID) FROM #TableSchema_Archive)
						BEGIN

							SELECT @ArchiveColumn = '[' + LTRIM(RTRIM(tsa.[Column_Name])) + ']=' + 'b.[' + LTRIM(RTRIM(tsa.[Column_Name])) + ']' + ', '
							FROM #TableSchema_Archive tsa WITH (NOLOCK)
							WHERE tsa.Column_ID = @NoColumns
							
							SELECT @ArchiveInsertColumns = @ArchiveInsertColumns + @ArchiveColumn
							
							SELECT @NoColumns = @NoColumns + 1
						END

					SELECT @ArchiveInsertColumns = SUBSTRING(@ArchiveInsertColumns, 1, LEN(@ArchiveInsertColumns) - 1)
					
					SELECT @Sql = @Sql + ' SELECT ' + RTRIM(LTRIM(@ArchiveInsertColumns))
					
					SELECT @Sql = @Sql + ' FROM ' + RTRIM(LTRIM(@DBName)) + '.dbo.' + RTRIM(LTRIM(@TableName)) + ' b  WITH (NOLOCK)'
				
			
					IF @JoinColumns IS NOT NULL 
						BEGIN -- step 3 (optional): when there are join tables then create the join statement
							SELECT @ArchiveInsertColumns = ''

							SELECT @ArchiveInsertColumns = @ArchiveInsertColumns + 'b.[' + RTRIM(LTRIM(cb.Tbl_Column)) + '] = avs.['  +RTRIM(LTRIM(cb.Tbl_Column)) + '] AND '
							FROM [dbo].[uf_EDW_DelimitedToTable](@JoinColumns, ',') cb

							SELECT @ArchiveInsertColumns = SUBSTRING(@ArchiveInsertColumns, 1, LEN(@ArchiveInsertColumns) - 4)

							SELECT @Sql = @Sql + ' INNER JOIN EDW.[dbo].[EDWArchiveStockSelection] avs WITH (NOLOCK) ON ' + @ArchiveInsertColumns
									Print @sql
						END
				END TRY 
				BEGIN CATCH
					SELECT 
								@ErrorMessage = 'Error Executing Dynamic SQL: ' + RTRIM(LTRIM(@Sql)) + ';' + ' for table: ' + RTRIM(LTRIM(@TableName)) + '; System Error Message: ' + ERROR_MESSAGE(),
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
				
					

				IF @Sql <> ''
					BEGIN
						BEGIN TRY  -- step 4: execute the sql statement to move the data from EDW to EDW_Archive
							IF @Debug = 1 
								BEGIN 
									SELECT @Sql
								END
							ELSE 
								BEGIN
									SELECT @NoRecordsArchived = 0
									--SELECT @SQL
									EXEC ( @Sql )
									SELECT @NoRecordsArchived = @@ROWCOUNT
									
									SELECT @msg = 'Finished: ' + RTRIM(LTRIM(@DBName)) + '.dbo.' + RTRIM(LTRIM(@TableName)) + ' Data Move.  No. Of Records Archived: ' + RTRIM(LTRIM(@NoRecordsArchived))
									--EXEC usp_csat_dw_log @SPROC_Name, @msg
								END
						END TRY 
						BEGIN CATCH
							SELECT 
								@ErrorMessage = 'Unable to insert into ' + RTRIM(LTRIM(@DBName)) + '.dbo.' + RTRIM(LTRIM(@TableName)) + '- !! System Error Message: ' + ERROR_MESSAGE(),
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
					END

			
	END 
			-- clean up for next pass
			DELETE FROM [#TableSchema_Archive]
			DELETE FROM #Tables_To_Arhive WHERE [Table_Name] = @TableName AND [DBName] = @DBName
			SELECT @RecordCounter = @RecordCounter - 1
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

	-- drop tables from tempdb
	DROP TABLE #TableSchema_Archive,#Tables_To_Arhive

END TRY
BEGIN CATCH

	--EXEC usp_csat_dw_log @SPROC_Name, @msg

	SELECT 
		@ErrorMessage = ERROR_MESSAGE(),
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

GO


