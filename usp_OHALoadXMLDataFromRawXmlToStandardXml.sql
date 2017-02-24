

Create PROCEDURE [dbo].[usp_OHALoadXMLDataFromRawXmlToStandardXml] 
/*************************************************************
USE EDW
GO 
Description: Updates the BusinessDay table.

Revision History
Date			Author			 Revision Description

Usage:
EXEC [dbo].[usp_LoadXMLData] 
Select * from [dbo].[OHALoadRawXML]
--Truncate table [dbo].[OHALoadRawXML]
Select * from [dbo].[OHAFormsXMLData]
--Truncate table [dbo].[OHAFormsXMLData]
*************************************************************/
AS
BEGIN
SET NOCOUNT ON;
BEGIN TRY
 

 
delete FROM [EDW].[DBO].[OHAFormsXMLData]  
WHERE    Id  in ( SELECT x.Id 
				  FROM  [EDW].[DBO].[OHAFormsXMLData] x
						Inner join EDW.[dbo].[OHALoadRawXML] xr on x.[BranchName]=xr.[BranchName]
												                AND x.[QtrYear] =xr.[QtrYear]
																AND x.FileType =xr.FileType
				)


INSERT INTO [EDW].[DBO].[OHAFormsXMLData] 
(
	 [XMLData]
	,[BranchName]
	,[QtrYear]
	,FileModifiedDateTime
	,FileType
)
SELECT 
	 CONVERT (XML,REPLACE(REPLACE(REPLACE(REPLACE (TEXTXML,'’',''),'“',''),'”',''),'MY:','') ) TEXTXML
	,[BranchName]
	,[QtrYear]
	,FileModifiedDateTime
	,FileType
FROM [EDW].[DBO].[OHALoadRawXML]
 
RETURN 0
END TRY

BEGIN CATCH
RETURN -1
END CATCH
 
END;

