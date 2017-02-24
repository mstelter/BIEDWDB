/* 
USE EDW
GO 
==============================================================================
Author		: Matt Stelter
Create date	: 2/28/2013
Description	: Populate the EDW Charity Activities tables from TEMP Charity tables	
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number

==============================================================================
Usage Example:
Exec EDW.dbo.usp_ETLCharityActivities
==============================================================================

TRUNCATE TABLE EDW.dbo.CharityActivities

*/

CREATE PROCEDURE [dbo].[usp_ETLCharityActivities]
AS     
BEGIN
DECLARE @ETLLoadID INT,
		--@ExtractRowCount_sp INT,
		@InsertRowCount INT,
		@UpdateRowCount INT
		--,@DeleteRowCount INT

----Run the Audit begin
EXEC @ETLLoadID = EDW.[dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'usp_ETLCharityActivities'

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

SET @TableName = 'Staging.dbo.Charity_SF_Activities'

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
/************Merge Activities****************************************/
/********************************************************************/
UPDATE Staging.[dbo].[Charity_SF_Activities] SET [AccountId] = CASE WHEN RIGHT([AccountId],1) = '"' THEN LEFT([AccountId],LEN([AccountId])-1) ELSE [AccountId] END
UPDATE Staging.[dbo].[Charity_SF_Activities] SET [ZIP_CODE__C] = CASE WHEN LEFT([ZIP_CODE__C],1) = '"' THEN RIGHT([ZIP_CODE__C],LEN([ZIP_CODE__C])-1) ELSE [ZIP_CODE__C] END

UPDATE Staging.dbo.Charity_SF_Activities SET SPID = NULL

UPDATE Activities SET SPID = Entities.SP_ID__c
--SELECT * 
FROM Staging.dbo.Charity_SF_Activities Activities WITH (NOLOCK)
inner join EDW.dbo.CharityEntities Entities WITH (NOLOCK)
ON Activities.WhatId = Entities.[CharityEntitiesId]

    MERGE EDW.[dbo].[CharityActivities] AS CA
    USING Staging.[dbo].[Charity_SF_Activities] AS SCA
          ON CA.[CharityActivitiesId] = SCA.[Id]
    WHEN MATCHED THEN 
        UPDATE SET 
			[CharityActivitiesId]=SCA.[Id],
			[AccountId]=SCA.[AccountId],
			[Activity_Disposition__c]=SCA.[Activity_Disposition__c],
			[Activity_Type__c]=SCA.[Activity_Type__c],
			[ActivityDate]=SCA.[ActivityDate],
			[CallDisposition]=SCA.[CallDisposition],
			[CallDurationInSeconds]=SCA.[CallDurationInSeconds],
			[CallObject]=SCA.[CallObject],
			[CallType]=SCA.[CallType],
			[Campaign__c]=SCA.[Campaign__c],
			[CreatedById]=SCA.[CreatedById],
			[CreatedDate]=SCA.[CreatedDate],
			[Description]=SCA.[Description],
			[Dialled_Number__c]=SCA.[Dialled_Number__c],
			[Direction__c]=SCA.[Direction__c],
			[Five9__Five9Agent__c]=SCA.[Five9__Five9Agent__c],
			[Five9__Five9AgentExtension__c]=SCA.[Five9__Five9AgentExtension__c],
			[Five9__Five9AgentName__c]=SCA.[Five9__Five9AgentName__c],
			[Five9__Five9ANI__c]=SCA.[Five9__Five9ANI__c],
			[Five9__Five9CallbackCampaignId__c]=SCA.[Five9__Five9CallbackCampaignId__c],
			[Five9__Five9CallbackCampaignName__c]=SCA.[Five9__Five9CallbackCampaignName__c],
			[Five9__Five9CallbackId__c]=SCA.[Five9__Five9CallbackId__c],
			[Five9__Five9CallbackNumber__c]=SCA.[Five9__Five9CallbackNumber__c],
			[Five9__Five9CallType__c]=SCA.[Five9__Five9CallType__c],
			[Five9__Five9Campaign__c]=SCA.[Five9__Five9Campaign__c],
			[Five9__Five9Cost__c]=SCA.[Five9__Five9Cost__c],
			[Five9__Five9DNIS__c]=SCA.[Five9__Five9DNIS__c],
			[Five9__Five9HandleTime__c]=SCA.[Five9__Five9HandleTime__c],
			[Five9__Five9SessionId__c]=SCA.[Five9__Five9SessionId__c],
			[Five9__Five9WrapTime__c]=SCA.[Five9__Five9WrapTime__c],
			[IsArchived]=SCA.[IsArchived],
			[IsClosed]=SCA.[IsClosed],
			[IsDeleted]=SCA.[IsDeleted],
			[IsRecurrence]=SCA.[IsRecurrence],
			[IsReminderSet]=SCA.[IsReminderSet],
			[IsVisibleInSelfService]=SCA.[IsVisibleInSelfService],
			[LastModifiedById]=SCA.[LastModifiedById],
			[LastModifiedDate]=SCA.[LastModifiedDate],
			[OwnerId]=SCA.[OwnerId],
			[Priority]=SCA.[Priority],
			[RecurrenceActivityId]=SCA.[RecurrenceActivityId],
			[RecurrenceDayOfMonth]=SCA.[RecurrenceDayOfMonth],
			[RecurrenceDayOfWeekMask]=SCA.[RecurrenceDayOfWeekMask],
			[RecurrenceEndDateOnly]=SCA.[RecurrenceEndDateOnly],
			[RecurrenceInstance]=SCA.[RecurrenceInstance],
			[RecurrenceInterval]=SCA.[RecurrenceInterval],
			[RecurrenceMonthOfYear]=SCA.[RecurrenceMonthOfYear],
			[RecurrenceStartDateOnly]=SCA.[RecurrenceStartDateOnly],
			[RecurrenceTimeZoneSidKey]=SCA.[RecurrenceTimeZoneSidKey],
			[RecurrenceType]=SCA.[RecurrenceType],
			[ReminderDateTime]=SCA.[ReminderDateTime],
			[Status]=SCA.[Status],
			[Subject]=SCA.[Subject],
			[SystemModstamp]=SCA.[SystemModstamp],
			[Time_Call_Received__c]=SCA.[Time_Call_Received__c],
			[Type]=SCA.[Type],
			[WhatId]=SCA.[WhatId],
			[WhoId]=SCA.[WhoId],
			BIUpdateDatetime = @BIUpdateDatetime,
			SPID = SCA.SPID,
			[LEAD__C] = SCA.[LEAD__C],
			[STATE__C] = SCA.[STATE__C],
			[Sub_Lead__C] = SCA.[Sub_Lead__C],
			[ZIP_CODE__C] = SCA.[ZIP_CODE__C]
	WHEN NOT MATCHED THEN
    INSERT([CharityActivitiesId]
      ,[AccountId]
      ,[Activity_Disposition__c]
      ,[Activity_Type__c]
      ,[ActivityDate]
      ,[CallDisposition]
      ,[CallDurationInSeconds]
      ,[CallObject]
      ,[CallType]
      ,[Campaign__c]
      ,[CreatedById]
      ,[CreatedDate]
      ,[Description]
      ,[Dialled_Number__c]
      ,[Direction__c]
      ,[Five9__Five9Agent__c]
      ,[Five9__Five9AgentExtension__c]
      ,[Five9__Five9AgentName__c]
      ,[Five9__Five9ANI__c]
      ,[Five9__Five9CallbackCampaignId__c]
      ,[Five9__Five9CallbackCampaignName__c]
      ,[Five9__Five9CallbackId__c]
      ,[Five9__Five9CallbackNumber__c]
      ,[Five9__Five9CallType__c]
      ,[Five9__Five9Campaign__c]
      ,[Five9__Five9Cost__c]
      ,[Five9__Five9DNIS__c]
      ,[Five9__Five9HandleTime__c]
      ,[Five9__Five9SessionId__c]
      ,[Five9__Five9WrapTime__c]
      ,[IsArchived]
      ,[IsClosed]
      ,[IsDeleted]
      ,[IsRecurrence]
      ,[IsReminderSet]
      ,[IsVisibleInSelfService]
      ,[LastModifiedById]
      ,[LastModifiedDate]
      ,[OwnerId]
      ,[Priority]
      ,[RecurrenceActivityId]
      ,[RecurrenceDayOfMonth]
      ,[RecurrenceDayOfWeekMask]
      ,[RecurrenceEndDateOnly]
      ,[RecurrenceInstance]
      ,[RecurrenceInterval]
      ,[RecurrenceMonthOfYear]
      ,[RecurrenceStartDateOnly]
      ,[RecurrenceTimeZoneSidKey]
      ,[RecurrenceType]
      ,[ReminderDateTime]
      ,[Status]
      ,[Subject]
      ,[SystemModstamp]
      ,[Time_Call_Received__c]
      ,[Type]
      ,[WhatId]
      ,[WhoId]
      ,BIUpdateDatetime
      ,SPID
      ,[LEAD__C]
      ,[STATE__C]
   ,[Sub_Lead__C]
   ,[ZIP_CODE__C]) 
	  VALUES ([Id]
      ,[AccountId]
      ,[Activity_Disposition__c]
      ,[Activity_Type__c]
      ,[ActivityDate]
      ,[CallDisposition]
      ,[CallDurationInSeconds]
      ,[CallObject]
      ,[CallType]
      ,[Campaign__c]
      ,[CreatedById]
      ,[CreatedDate]
      ,[Description]
      ,[Dialled_Number__c]
      ,[Direction__c]
      ,[Five9__Five9Agent__c]
      ,[Five9__Five9AgentExtension__c]
      ,[Five9__Five9AgentName__c]
      ,[Five9__Five9ANI__c]
      ,[Five9__Five9CallbackCampaignId__c]
      ,[Five9__Five9CallbackCampaignName__c]
      ,[Five9__Five9CallbackId__c]
      ,[Five9__Five9CallbackNumber__c]
      ,[Five9__Five9CallType__c]
      ,[Five9__Five9Campaign__c]
      ,[Five9__Five9Cost__c]
      ,[Five9__Five9DNIS__c]
      ,[Five9__Five9HandleTime__c]
      ,[Five9__Five9SessionId__c]
      ,[Five9__Five9WrapTime__c]
      ,[IsArchived]
      ,[IsClosed]
      ,[IsDeleted]
      ,[IsRecurrence]
      ,[IsReminderSet]
      ,[IsVisibleInSelfService]
      ,[LastModifiedById]
      ,[LastModifiedDate]
      ,[OwnerId]
      ,[Priority]
      ,[RecurrenceActivityId]
      ,[RecurrenceDayOfMonth]
      ,[RecurrenceDayOfWeekMask]
      ,[RecurrenceEndDateOnly]
      ,[RecurrenceInstance]
      ,[RecurrenceInterval]
      ,[RecurrenceMonthOfYear]
      ,[RecurrenceStartDateOnly]
      ,[RecurrenceTimeZoneSidKey]
      ,[RecurrenceType]
      ,[ReminderDateTime]
      ,[Status]
      ,[Subject]
      ,[SystemModstamp]
      ,[Time_Call_Received__c]
      ,[Type]
      ,[WhatId]
      ,[WhoId]
	  ,@BIUpdateDatetime
	  ,SPID
	  ,[LEAD__C]
	  ,[STATE__C]
	  ,[Sub_Lead__C]
	  ,[ZIP_CODE__C])
	     
OUTPUT $action INTO #tmp_RecordCount;

UPDATE Activities SET SPID = Entities.SP_ID__c
--SELECT * 
FROM EDW.dbo.CharityActivities Activities WITH (NOLOCK)
inner join Staging.dbo.Charity_SF_Entities Entities WITH (NOLOCK)
ON Activities.WhatId = Entities.[Id]

UPDATE Activities SET SPID = Donations.SPID
--SELECT Activities.SPID, Donations.SPID
FROM EDW.dbo.CharityActivities Activities WITH (NOLOCK)
inner join EDW.dbo.CharityDonations Donations WITH (NOLOCK)
ON Activities.WhatId = Donations.ID
WHERE ISNULL(Activities.SPID,0)<> ISNULL(Donations.SPID,0)
--WHERE Activities.BIUpdateDatetime >=@BIUpdateDatetime

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


