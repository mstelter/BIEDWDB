

Create PROCEDURE [dbo].[usp_ETLBusinessDay] 
/*************************************************************
USE EDW
GO 
Description: Updates the BusinessDay table.

Revision History
Date			Author			 Revision Description
12/05/2011	  Venkata Vempali			Created
12/22/2011        VV             Impletemented Try/Catch.(DBA Code Review)

Usage:
EXEC [dbo].[usp_ETLBusinessDay];
*************************************************************/
AS
BEGIN
SET NOCOUNT ON;
BEGIN TRY
 
  UPDATE dbo.BusinessDay
  SET BusinessDateTime =  DATEADD(DAY,DATEDIFF(DAY,0,GETDATE()),0) 	
 
  Return 0
  
END TRY
BEGIN CATCH

	Return -1

END CATCH
 
END;


