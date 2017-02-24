/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 5/13/2013
Description	: Populate the EDW Charity Question table from TEMP Charity tables	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number

==============================================================================
Usage Example:
Exec EDW.dbo.usp_ETLCharityQuestion
==============================================================================

TRUNCATE TABLE EDW.dbo.CharityQuestion

*/

CREATE PROCEDURE [dbo].[usp_ETLCharityQuestion]
AS     
BEGIN
DECLARE @ETLLoadID INT,
		@InsertRowCount INT,
		@UpdateRowCount INT

----Run the Audit begin
EXEC @ETLLoadID = EDW.[dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLCharityQuestion'

DECLARE @TableName    VARCHAR(50),
        @Fields       VARCHAR(max),
        @InsertValues VARCHAR(max),
        @FieldName    VARCHAR(50),
        @Datatype     VARCHAR(50),
        @MaxLength    INT,
        @SQL          VARCHAR(8000),
        @RecordCount  INT,
        @FileName     VARCHAR(255),
        @BIUpdateDatetime DATETIME	
        
SET NOCOUNT ON;
BEGIN TRY

SET @BIUpdateDatetime = GETDATE()

/********************************************************************/
/************Remove trailing "Z" on date values**********************/
/********************************************************************/
IF OBJECT_ID('tempdb..#tmp_RecordCount') IS NOT NULL DROP TABLE #tmp_RecordCount
CREATE TABLE #tmp_RecordCount(ChangeType varchar(20)) 

SET @TableName = 'Staging.dbo.Charity_SF_Question'

      SET @Fields = ''
      SET @InsertValues = ''
      SET @FieldName = ''
      SET @MaxLength = 0

      DECLARE TableFields CURSOR FOR
        SELECT c.name 'ColumnName'
        FROM   Staging.sys.columns c
        WHERE  c.object_id=Object_id(@TableName)

      OPEN TableFields;

      FETCH NEXT FROM TableFields INTO @FieldName

      WHILE @@FETCH_STATUS=0 BEGIN
            IF @FieldName LIKE '%date%'
                OR @FieldName LIKE '%modstamp%' BEGIN
                  SET @SQL = 'UPDATE '+@TableName+' SET '+@FieldName+' = 
				CASE WHEN RIGHT(['+@FieldName+'],1) = ''z'' 
					THEN LEFT(['+@FieldName+
                  '],LEN(['+
                             @FieldName
                             +'])-1) 
					ELSE ['+
                             @FieldName+'] 
					END'

                  EXEC (@SQL)
              END

            FETCH NEXT FROM TableFields INTO @FieldName
        END

      CLOSE TableFields;

      DEALLOCATE TableFields;

BEGIN TRAN
/********************************************************************/
/************Merge Question****************************************/
/********************************************************************/
    MERGE EDW.[dbo].[CharityQuestion] AS CQ
    USING Staging.[dbo].[Charity_SF_Question] AS SQ
          ON CQ.[ID] = SQ.[Id]
    WHEN MATCHED THEN 
        UPDATE SET 
			[SYSTEMMODSTAMP]=SQ.[SYSTEMMODSTAMP]
           ,[QUESTION_TEXT__C]=SQ.[QUESTION_TEXT__C]
           ,[OWNERID]=SQ.[OWNERID]
           ,[NAME]=SQ.[NAME]
           ,[LASTMODIFIEDDATE]=SQ.[LASTMODIFIEDDATE]
           ,[LASTMODIFIEDBYID]=SQ.[LASTMODIFIEDBYID]
           ,[ISDELETED]=SQ.[ISDELETED]
           ,[ID]=SQ.[ID]
           ,[CREATEDDATE]=SQ.[CREATEDDATE]
           ,[CREATEDBYID]=SQ.[CREATEDBYID]
           ,[CHARITY__C]=SQ.[CHARITY__C]
  	WHEN NOT MATCHED THEN
    INSERT([SYSTEMMODSTAMP]
           ,[QUESTION_TEXT__C]
           ,[OWNERID]
           ,[NAME]
           ,[LASTMODIFIEDDATE]
           ,[LASTMODIFIEDBYID]
           ,[ISDELETED]
           ,[ID]
           ,[CREATEDDATE]
           ,[CREATEDBYID]
           ,[CHARITY__C]) 
	  VALUES ([SYSTEMMODSTAMP]
           ,[QUESTION_TEXT__C]
           ,[OWNERID]
           ,[NAME]
           ,[LASTMODIFIEDDATE]
           ,[LASTMODIFIEDBYID]
           ,[ISDELETED]
           ,[ID]
           ,[CREATEDDATE]
           ,[CREATEDBYID]
           ,[CHARITY__C])
	     
OUTPUT $action INTO #tmp_RecordCount;

SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #tmp_RecordCount

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC EDW.[dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		COMMIT TRAN

		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
			ROLLBACK TRAN
		EXEC edw.[dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	END CATCH
	END


