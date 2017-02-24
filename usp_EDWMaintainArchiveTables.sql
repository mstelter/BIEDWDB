CREATE PROCEDURE [dbo].[usp_EDWMaintainArchiveTables]

--====================================================================================--
--====================================================================================--
-- Description:	Creates and Alters Archive Tables
-- Usage:		EXEC [usp_EDWMaintainArchiveTables] @Debug = 1
-- Author:		Mohammad Waheed
-- Revision History:
-- 	Date		Programmer		Description
--	-------		--------------  ------------------

--====================================================================================--
--====================================================================================--
( 
	@Debug BIT = 0 
)

AS

--DECLARE @Debug BIT
--SET @Debug = 0

SET NOCOUNT ON

DECLARE @RecordCounter INT, @err INT, @msg VARCHAR(200)
--, @SPROC_Name VARCHAR(100)
SELECT @RecordCounter = 0, @err = 0, @msg = ''
--, @SPROC_Name = OBJECT_NAME(@@PROCID) 

DECLARE @TableName VARCHAR(150), @DBName CHAR(20), @Sql VARCHAR(Max),@NewColumnsForArchiveTbl VARCHAR(Max),@ErrorMessage NVARCHAR(4000), @ErrorSeverity INT, @ErrorState INT;
SELECT @Sql = '', @NewColumnsForArchiveTbl = ''

BEGIN TRY

		Create Table #Tables_To_Arhive  (
			[Table_Name] VARCHAR(150),
			[DBName] CHAR(20) )
			
		INSERT INTO #Tables_To_Arhive ( [Table_Name], [DBName] )
		SELECT 
			[Table_Name] = [TableName],
			[DBName] = [DBName]
		FROM dbo.EDWArchiveTableSelection WITH (NOLOCK) --where [TableName]='DimStock'
			
		SELECT @RecordCounter = @@ROWCOUNT
			
		CREATE TABLE #TableSchema  ( 
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
					@DBName = [DBName]
				FROM #Tables_To_Arhive
				
				-- if table is dropped from Actual Database , then no need to check tables existance in Archive Database
				
				IF EXISTS (SELECT 1 FROM dbo.sysobjects WITH (nolock) WHERE NAME = @tablename AND xtype = 'U')
								
					BEGIN  
				
						IF @Debug = 1 
							BEGIN 
								SELECT TableName=@TableName, DatabaseName=@DBName
							END

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
						--print @sql
						-- Get the Table Schema from Actual Database
						INSERT INTO #TableSchema
						EXEC (@Sql) 
							SELECT @err = @@ERROR

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
							--print @sql
						-- Get the Table Schema from Actual Database
						INSERT INTO #TableSchema_Archive
						EXEC ( @Sql)
							SELECT @err = @@ERROR

									
						BEGIN TRY 
			
								SELECT @Sql = '', @NewColumnsForArchiveTbl = ''
								
							
							-- if archive table does not exists, then create table in EDW_Archive DB
									IF ISNULL(( SELECT COUNT(*) FROM #TableSchema_Archive WITH (NOLOCK)), 0) = 0
										BEGIN
											SELECT 
												@NewColumnsForArchiveTbl = @NewColumnsForArchiveTbl + '[' + LTRIM(RTRIM(ts.[Column_Name])) + '] ' + 
													CASE 
														WHEN ts.[Type] LIKE '%CHAR%' THEN RTRIM(LTRIM(ts.[Type])) + '(' + RTRIM(LTRIM(ts.[Length])) + ')'
														WHEN ts.[Type] LIKE '%VCHAR%' THEN RTRIM(LTRIM(ts.[Type])) + '(' + RTRIM(LTRIM(ts.[Length])) + ')'
														WHEN ts.[Type] = 'DECIMAL' THEN RTRIM(LTRIM(ts.[Type])) + '(' + RTRIM(LTRIM(ts.[Prec])) + ', ' + RTRIM(LTRIM(ts.[Scale])) + ')'
														ELSE RTRIM(LTRIM(ts.[Type]))
													END + ' ' +
													CASE [Nullable]
														WHEN 'YES' THEN 'NULL'
														ELSE 'NOT NULL'
													END + ','
											FROM #TableSchema ts WITH (NOLOCK)
										
											SELECT @NewColumnsForArchiveTbl = SUBSTRING(@NewColumnsForArchiveTbl, 1, LEN(@NewColumnsForArchiveTbl) - 1)
										--Select @NewColumnsForArchiveTbl
											SELECT @Sql = 'USE EDW_Archive ' + 'CREATE TABLE dbo.' + RTRIM(LTRIM(@TableName)) + '_Archive ( ' + @NewColumnsForArchiveTbl + ' )'
											Print @sql
										END
									
									ELSE 
										BEGIN
											-- if archive table exists, but the archive table does not have the same no. of columns and column names as base table then add new columns to archive table
											IF ( SELECT COUNT(*) FROM [#TableSchema] ts WITH (NOLOCK) WHERE NOT EXISTS ( SELECT 1 FROM [#TableSchema_Archive] tsa WITH (NOLOCK) WHERE ts.[Column_Name] = tsa.[Column_Name] ) ) > 0
												BEGIN
													SELECT 
														@NewColumnsForArchiveTbl = @NewColumnsForArchiveTbl + '[' + LTRIM(RTRIM(ts.[Column_Name])) + '] ' + 
															CASE 
																WHEN ts.[Type] LIKE '%CHAR%' THEN RTRIM(LTRIM(ts.[Type])) + '(' + RTRIM(LTRIM(ts.[Length])) + ')'
																WHEN ts.[Type] LIKE '%VCHAR%' THEN RTRIM(LTRIM(ts.[Type])) + '(' + RTRIM(LTRIM(ts.[Length])) + ')'
																WHEN ts.[Type] = 'DECIMAL' THEN RTRIM(LTRIM(ts.[Type])) + '(' + RTRIM(LTRIM(ts.[Prec])) + ', ' + RTRIM(LTRIM(ts.[Scale])) + ')'
																ELSE RTRIM(LTRIM(ts.[Type]))
															END + ','
													FROM #TableSchema ts WITH (NOLOCK)
													WHERE NOT EXISTS (
														SELECT * FROM #TableSchema_Archive tsa WITH (NOLOCK) 
														WHERE tsa.[Column_Name] = ts.[Column_Name] )
														
													SELECT @NewColumnsForArchiveTbl = SUBSTRING(@NewColumnsForArchiveTbl, 1, LEN(@NewColumnsForArchiveTbl) - 1)
												
													SELECT @Sql = 'USE EDW_Archive ' + 'ALTER TABLE dbo.' + RTRIM(LTRIM(@TableName)) + '_Archive ADD ' + @NewColumnsForArchiveTbl
												END
											ELSE 
												BEGIN
													-- if archive table exists, but the . table does not have the same base column schema for each 
													-- matching column then alter the existing archive column to reflect the base column schema
													IF ( SELECT COUNT(*) FROM [#TableSchema] ts WITH (NOLOCK) WHERE NOT EXISTS ( SELECT 1 FROM [#TableSchema_Archive] tsa WITH (NOLOCK) WHERE tsa.[Column_Name] = ts.[Column_Name] AND tsa.[Type] = ts.[Type] AND tsa.[Prec] = ts.[Prec] AND tsa.[Scale] = ts.[Scale] AND tsa.[Nullable] = ts.[Nullable] ) ) >0
														BEGIN
															SELECT 
																@NewColumnsForArchiveTbl = @NewColumnsForArchiveTbl + 'ALTER TABLE ' + RTRIM(LTRIM(@TableName)) + '_Archive ALTER COLUMN ' +  '[' + LTRIM(RTRIM(ts.[Column_Name])) + '] ' + 
																	CASE 
																		WHEN ts.[Type] LIKE '%CHAR%' THEN RTRIM(LTRIM(ts.[Type])) + '(' + RTRIM(LTRIM(ts.[Length])) + ')'
																		WHEN ts.[Type] LIKE '%VCHAR%' THEN RTRIM(LTRIM(ts.[Type])) + '(' + RTRIM(LTRIM(ts.[Length])) + ')'
																		WHEN ts.[Type] = 'DECIMAL' THEN RTRIM(LTRIM(ts.[Type])) + '(' + RTRIM(LTRIM(ts.[Prec])) + ', ' + RTRIM(LTRIM(ts.[Scale])) + ')'
																		ELSE RTRIM(LTRIM(ts.[Type]))
																	END + '; '
															FROM #TableSchema ts WITH (NOLOCK)
															WHERE NOT EXISTS (
																SELECT * FROM #TableSchema_Archive tsa WITH (NOLOCK) 
																WHERE tsa.[Column_Name] = ts.[Column_Name]
																AND tsa.[Type] = ts.[Type]
																AND tsa.[Prec] = ts.[Prec]
																AND tsa.[Scale] = ts.[Scale]
																AND tsa.[Nullable] = ts.[Nullable] )
																
															SELECT @NewColumnsForArchiveTbl = SUBSTRING(@NewColumnsForArchiveTbl, 1, LEN(@NewColumnsForArchiveTbl) - 1)
														
															SELECT @Sql = 'USE EDW_Archive ' + @NewColumnsForArchiveTbl
															
														END
													END
									
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
									EXEC ( @Sql)
									SELECT @err = @@ERROR
										
								END 
							END TRY 
							BEGIN CATCH
							SELECT 
								@ErrorMessage = 'Unable to add/update schema for ' + RTRIM(LTRIM(@DBName)) + '..' + RTRIM(LTRIM(@TableName)) + '- !! System Error Message: ' + ERROR_MESSAGE(),
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
		DELETE FROM [#TableSchema]
		DELETE FROM [#TableSchema_Archive]
		DELETE FROM #Tables_To_Arhive WHERE [Table_Name] = @TableName AND [DBName] = @DBName
		SELECT @RecordCounter = @RecordCounter - 1
		
END

-- drop tables from tempdb
DROP TABLE #TableSchema, #TableSchema_Archive

END TRY
BEGIN CATCH

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


