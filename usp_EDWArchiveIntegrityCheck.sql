CREATE PROCEDURE [dbo].[usp_EDWArchiveIntegrityCheck]
/*
--====================================================================================--
--====================================================================================--
-- Description:	Checks the Integrity between Base Table ( CSAT_Vehicle_Inv ) and other Vehicle Tables on DW
-- Usage:		EXEC [usp_EDW_Archive_Integrity_Check] 
-- Author:		Mohammad Waheed
--Created Date 01/19/2017
-- Revision History:
-- 	Date		     Developer		    Description
--	-------		------------------	    -------------------
--====================================================================================--
Select * from dbo.EDWArchiveStockSelection

--====================================================================================--
*/
( 
	@Debug BIT = 0 
)
AS

SET NOCOUNT ON

DECLARE @RecordCounter INT, @err INT, @msg VARCHAR(200), @SPROC_Name VARCHAR(100)
SELECT @RecordCounter = 0, @err = 0, @msg = '', @SPROC_Name = OBJECT_NAME(@@PROCID) 


DECLARE @TableName VARCHAR(150), @DBName CHAR(20), @Sql NVARCHAR(MAX), @ArchiveInsertColumns VARCHAR(8000), @JoinColumns VARCHAR(150), @NoColumns INT, 
		@ArchiveColumn VARCHAR(200), @NoRecordsArchived INT, @params NVARCHAR(MAX),@ErrorMessage NVARCHAR(4000), @ErrorSeverity INT, @ErrorState INT;
		
SELECT @Sql = '', @ArchiveInsertColumns = ''

DECLARE @ArchiveDate varchar(20)
SET @ArchiveDate = Convert(varchar(20),DateAdd(mm,0,DateAdd(yy,-15,GETDATE())),112) -- dbo.fn_archive_date()

BEGIN TRY

	TRUNCATE TABLE dbo.EDWArchiveStockSelection

	--***************************************
	-- Integrity Logic Starts
	--***************************************

	DECLARE @Tables_To_Archive TABLE (
		[Table_Name] VARCHAR(150),
		[DBName] CHAR(20),
		[Join_Columns] VARCHAR(150) )
		
	INSERT INTO @Tables_To_Archive ( [Table_Name], [DBName], [Join_Columns])
	SELECT 
		[Table_Name] = [TableName],
		[DBName] = [DBName],
		[Join_Columns] = [JoinColumns]
	FROM [dbo].[EDWArchiveTableSelection] WITH (NOLOCK)
	WHERE ArchiveTypeCode = 'AO'

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

Select ds.StockId
 into #Stocks 
 from [EDW].[dbo].FactStockCycleTime  fsct With (Nolock)
 inner join EDW.dbo.DimStock ds with (Nolock) on fsct.StockID=ds.StockID  where  ([CancelDayID]<@ArchiveDate or [SoldDayID]<@ArchiveDate)
 and Not Exists ( select top 1 * from BI_ASAP_Rep.dbo.Salvage s With (nolock) where ds.AsapSalvageID=s.Salvage_ID)

	
	WHILE @RecordCounter > 0
		BEGIN
			SELECT TOP 1
				@TableName = [Table_Name],
				@DBName = [DBName],
				@JoinColumns = [Join_Columns]
			FROM @Tables_To_Archive
			
	-- We are doing Integrity Check between Base Table ( CSAT_Vehicle_Inv ) and other Vehicle Tables on DW. as CSAT_Vehicle_Inv is a base table , we are excluding from looping of Integrity Check		
	-- if there are stocks in DW Vehicle Tables which are not in CSAT_Vehicle_Inv then those stocks should Archive from Vehicle DW Tables
			
	--IF @TableName <> 'CSAT_Vehicle_Detail_load'  

	BEGIN
			
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
			FROM EDW_Archive.dbo.[uf_Get_Table_Schema]( ''' + RTRIM(LTRIM(@TableName)) + '_Archive''); '
			
			INSERT INTO #TableSchema_Archive
			EXEC ( @Sql )
				SELECT @err = @@ERROR

				IF @JoinColumns IS NOT NULL 

					 BEGIN 
						BEGIN TRY 
					-- Step 1 -- Build a Insert Statement 
					
							SELECT @ArchiveInsertColumns = ''
							
							SELECT @ArchiveInsertColumns = @ArchiveInsertColumns + '[' + RTRIM(LTRIM(cb.Tbl_Column))+ '],'
										FROM EDW.[dbo].[uf_EDW_DelimitedToTable](@JoinColumns, ',') cb
							
							SELECT @Sql = 'INSERT INTO dbo.EDWArchiveStockSelection (' + @ArchiveInsertColumns + '[ArchiveDate]) '
							
							-- step 2 - Build Columns for Select Statement 
							
							SELECT @ArchiveInsertColumns = ''

							SELECT @ArchiveInsertColumns = @ArchiveInsertColumns + 'b.[' + RTRIM(LTRIM(cb.Tbl_Column)) + '], '
							FROM EDW.[dbo].[uf_EDW_DelimitedToTable](@JoinColumns, ',') cb
							
							SELECT @sql = @sql + 'SELECT DISTINCT ' + @ArchiveInsertColumns + '@ArchiveDate'

							
							-- Step 3 -- Build from ,Where and join columns on csat_vehicle_inv statement
							
							SELECT @ArchiveInsertColumns = ''

							SELECT @ArchiveInsertColumns = @ArchiveInsertColumns + 'b.[' + RTRIM(LTRIM(cb.Tbl_Column)) + '] = vi.['  +RTRIM(LTRIM(cb.Tbl_Column))+ '] AND '
							FROM EDW.[dbo].[uf_EDW_DelimitedToTable](@JoinColumns, ',') cb
							
							SELECT @ArchiveInsertColumns = SUBSTRING(@ArchiveInsertColumns, 1, LEN(@ArchiveInsertColumns) - 4)

							SELECT @Sql = @Sql + ' FROM ' + RTRIM(LTRIM(@DBName)) + '.dbo.' + RTRIM(LTRIM(@TableName)) + ' b WITH (NOLOCK)
							inner join #Stocks ds With(nolock) on ds.StockId=b.StockId' 
					
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
						BEGIN TRY -- step 4: execute the sql statement to move the data from CSA_ETL to CSA_ETL_Archive
							IF @Debug = 1 
								BEGIN 
									SELECT @Sql
								END
							ELSE 
								BEGIN
									SELECT @NoRecordsArchived = 0
									
									SET @params = N'@archivedate datetime';
									print @sql 
									EXEC SP_ExecuteSQL @Sql,@params,@archivedate 
									
									SELECT @NoRecordsArchived = @@ROWCOUNT
										
								END
							
						END TRY 
						BEGIN CATCH
							SELECT 
								@ErrorMessage = 'Unable to insert into EDWArchiveTableSelection - records needs to archive based on Integrity Check !! System Error Message: ' + ERROR_MESSAGE(),
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
		END
		
		-- clean up for next pass
		DELETE FROM [#TableSchema_Archive]
		DELETE FROM @Tables_To_Archive WHERE [Table_Name] = @TableName AND [DBName] = @DBName
		SELECT @RecordCounter = @RecordCounter - 1
					 	
				  
	END 

--***************************************
-- Integrity Logic Ends
--***************************************


-- drop tables from tempdb
DROP TABLE #TableSchema_Archive

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


