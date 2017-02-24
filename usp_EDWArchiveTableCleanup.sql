CREATE PROCEDURE [dbo].[usp_EDWArchiveTableCleanup]

--====================================================================================--
--====================================================================================--
-- Description:	Deletes records from active database and move tables from active database to archive database
-- Usage:		EXEC [usp_EDWArchiveTableCleanup]  @Debug = 1
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

DECLARE @RecordCounter INT, @err INT, @msg VARCHAR(200)
--, @SPROC_Name VARCHAR(100)
SELECT @RecordCounter = 0, @err = 0, @msg = ''
--, @SPROC_Name = OBJECT_NAME(@@PROCID) 

DECLARE @TableName VARCHAR(150), @DBName CHAR(20), @Sql VARCHAR(8000), @ArchiveInsertColumns VARCHAR(8000), @JoinColumns VARCHAR(150)
--, @NoColumns INT, 
--  @ArchiveColumn VARCHAR(200)
, @NoRecordsArchived INT, @ArchiveTypeCode CHAR(3), @PurgeDate DATETIME ,@ErrorMessage NVARCHAR(4000), @ErrorSeverity INT, @ErrorState INT;
SELECT @Sql = '', @ArchiveInsertColumns = ''

BEGIN TRY 

	Create TABLE #Tables_To_Arhive  (
		[Table_Name] VARCHAR(150),
		[DBName] CHAR(20),
		[Join_Columns] VARCHAR(150),
		[Archive_Type_Code] CHAR(3),
		[Purge_Date] DATETIME,
		EDWArchiveTableSelectionID int )
		
	INSERT INTO #Tables_To_Arhive ( [Table_Name], [DBName], [Join_Columns], [Archive_Type_Code], [Purge_Date],EDWArchiveTableSelectionID)
	SELECT 
		[Table_Name] = [TableName],
		[DBName] = [DBName],
		[Join_Columns] = [JoinColumns],
		[Archive_Type_Code] = [ArchiveTypeCode],
		[Purge_Date] = [PurgeDate],
		EDWArchiveTableSelectionID
	FROM dbo.EDWArchiveTableSelection WITH (NOLOCK)
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
				@JoinColumns = [Join_Columns],
				@ArchiveTypeCode = [Archive_Type_Code],
				@PurgeDate = [Purge_Date]
			FROM #Tables_To_Arhive order by EDWArchiveTableSelectionID
			
	SELECT @msg = 'Starting: ' + RTRIM(LTRIM(@DBName)) + '..' + RTRIM(LTRIM(@TableName)) + ' Data Move'
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
			FROM ' + RTRIM(LTRIM(@DBName)) + '.dbo.[uf_Get_Table_Schema]( ''' + RTRIM(LTRIM(@TableName)) + '''); '
			--'_Archive''); '
			--Print @sql
			INSERT INTO #TableSchema_Archive
			EXEC ( @Sql )
				SELECT @err = @@ERROR

		BEGIN TRY
			IF @JoinColumns IS NOT NULL 
				BEGIN 				
					 -- When there are join tables then create the join statement to delete existing records in CSA_ETL DB
					
									
						SELECT @Sql = 'DELETE FROM b'
						
						SELECT @ArchiveInsertColumns = ''

						SELECT @ArchiveInsertColumns = @ArchiveInsertColumns + 'b.[' + RTRIM(LTRIM(cb.Tbl_Column)) + '] = avs.['  +	RTRIM(LTRIM(cb.Tbl_Column)) + '] AND '
						FROM [dbo].[uf_EDW_DelimitedToTable](@JoinColumns, ',') cb

						SELECT @ArchiveInsertColumns = SUBSTRING(@ArchiveInsertColumns, 1, LEN(@ArchiveInsertColumns) - 4)

						SELECT @Sql = @Sql + ' FROM ' + RTRIM(LTRIM(@DBName)) + '.dbo.' + RTRIM(LTRIM(@TableName)) + ' b  WITH (NOLOCK)'
						SELECT @Sql = @Sql + ' WHERE EXISTS ( SELECT 1 FROM [dbo].[EDWArchiveStockSelection] avs WITH (NOLOCK) WHERE ' + @ArchiveInsertColumns + '  )'
						
						SELECT @msg = 'Finished: ' + RTRIM(LTRIM(@DBName)) + '.dbo.' + RTRIM(LTRIM(@TableName)) + ' Base Table Removal. No. Of Records Deleted: ' 			
					END
				ELSE 
					BEGIN
						SELECT @Sql = ''
						

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
					BEGIN TRY 
						
						IF @Debug = 1 
							BEGIN 
								SELECT @Sql
							END
						ELSE 
							BEGIN
								SELECT @NoRecordsArchived = 0
								print @sql
								EXEC ( @Sql )
								SELECT @NoRecordsArchived = @@ROWCOUNT
								
								SELECT @msg = 'Finished: ' + RTRIM(LTRIM(@DBName)) + '..' + RTRIM(LTRIM(@TableName)) + ' Data Move.  No. Of Records Archived: ' + RTRIM(LTRIM(@NoRecordsArchived))
									--EXEC usp_csat_dw_log @SPROC_Name, @msg
								END
					END TRY 
					BEGIN CATCH
							SELECT 
								@ErrorMessage = 'Unable to insert into ' + RTRIM(LTRIM(@DBName)) + '..' + RTRIM(LTRIM(@TableName)) + '- !! System Error Message: ' + ERROR_MESSAGE(),
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
	
				

		

			-- clean up for next pass
			DELETE FROM [#TableSchema_Archive]
			DELETE FROM #Tables_To_Arhive WHERE [Table_Name] = @TableName AND [DBName] = @DBName
			SELECT @RecordCounter = @RecordCounter - 1
		END

	-- drop tables from tempdb
	DROP TABLE #TableSchema_Archive
	
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
