/* 
USE EDW
GO 
==============================================================================
Author		: Mohammad Waheed
Create date	: 12/30/2016
Description	: Populate the EDW DimVendor table
==============================================================================
REVISION History
ChangeDate	Developer	    	Release/Problem Number

==============================================================================
Usage Example:
Exec usp_ETLDimVendor
Select * from edw.dbo.DimVendor With (Nolock)
Select * from edw.dbo.[FactFinancialTransaction] With (Nolock) where DimVendorId is not null or VendorID is not null
==============================================================================
*/

CREATE PROCEDURE [dbo].[usp_ETLDimVendor]
AS     
BEGIN


DECLARE @ETLLoadID INT,
		@ExtractRowCount_sp INT,
		@InsertRowCount INT,
		@UpdateRowCount INT,
		@DeleteRowCount INT,
		@StartLSN BINARY(10),
		@EndLSN BINARY(10)
		
SET NOCOUNT ON;
BEGIN TRY

----Run the Audit begin
----Run the Audit begin
EXEC @ETLLoadID = [dbo].[Usp_etlloadaudit] @StartEnd = 'S',@PackageName = 'uspETLDimVendor'

IF OBJECT_ID('tempdb..#temp_TowerRecordCount') IS NOT NULL DROP TABLE #temp_TowerRecordCount
CREATE TABLE #temp_TowerRecordCount(ChangeType VARCHAR(30)) 


Select Salvage_Expense_ID,Salvage_ID,asap_object_id,ASAP_Object_Type_ID
Into #Salvage_Expense
From [BI_ASAP_Rep].[cdc].[dbo_Salvage_Expense_CT] --where Update_DateTime>=DATEADD(yy,-1,getdate())
--Union 
--Select Salvage_Expense_ID,Salvage_ID,asap_object_id,ASAP_Object_Type_ID
--From Pasapdb.[ASAP_SalvAr].[dbo].[Salvage_Expense_Archive]  where Update_DateTime>=DATEADD(yy,-1,getdate())
----From Qasapdb.[ASAP_SalvAr].[dbo].[Salvage_Expense_Archive]  --where Update_DateTime>=DATEADD(yy,-1,getdate())





Select  se.Salvage_Expense_ID ,se.Salvage_ID,Se.[asap_object_id]  ,Se.[ASAP_Object_Type_ID], SPP.[Salvage_Provider_Name],af.[Affiliate_Name]
,T.Tower_Name,v.Vendor_Name,sl.Location_Name, DMV.send_to_Department,NDMV.Non_State_DMV_Title_Processor_Name,GP.Vendor_ID
into #VendorNames
 from  #Salvage_Expense se 
 			left JOIN [BI_ASAP_Rep].dbo.dmv DMV   WITH(nolock)   On DMV.DMV_ID=   se.[asap_object_id]  
			left JOIN [BI_ASAP_Rep].dbo.[Non_State_DMV_Title_Processor]  NDMV   WITH(nolock)   On NDMV.Non_State_DMV_Title_Processor_ID=   se.[asap_object_id]  
			--left JOIN [PASAPDB].[ASAPSQL].dbo.dmv DMV   WITH(nolock)   On DMV.DMV_ID=   se.[asap_object_id]  
			--left JOIN [PASAPDB].[ASAPSQL].dbo.[Non_State_DMV_Title_Processor]  NDMV   WITH(nolock)   On NDMV.Non_State_DMV_Title_Processor_ID=   se.[asap_object_id]  
			LEFT OUTER JOIN [BI_ASAP_Rep].[dbo].[Tower] T WITH(nolock)  on T.Tower_ID=se.[asap_object_id] 
			LEFT OUTER JOIN [BI_ASAP_Rep]. [dbo].[vendor] V WITH(nolock)   ON v.[vendor_id] = Se.[asap_object_id] 
			LEFT OUTER JOIN [BI_ASAP_Rep]. [dbo].[Salvage_Provider] SPP WITH (NOLOCK)  On SPP.[Salvage_Provider_ID]=se.[asap_object_id]  
			LEFT OUTER JOIN [BI_ASAP_Rep].[dbo].[Affiliate] AF WITH (NOLOCK) on AF.[Affiliate_ID]=Se.[asap_object_id] 
			LEFT OUTER JOIN  [BI_ASAP_Rep].[dbo].[Storage_Location] SL WITH (NOLOCK) on SL.Storage_Location_ID=Se.[asap_object_id] 
			LEFT OUTER JOIN  (Select [ASAP_Object_ID],ASAP_Object_Type_ID,Vendor_ID
			                                   From[BI_ASAP_Rep].[dbo].[ASAP_GP_XRef]
			                                   Group by [ASAP_Object_ID],ASAP_Object_Type_ID,Vendor_ID)GP  on se.[ASAP_Object_ID]=GP.[ASAP_Object_ID] and GP.ASAP_Object_Type_ID=se.ASAP_Object_Type_ID 			
			--where GP.Vendor_ID=1265

--Select * from (select distinct Vendor_ID,ASAP_Object_Type_ID,ROW_NUMBER() over (partition by Salvage_ID order by Salvage_Expense_ID desc) Ranks, 
--Case 
--		when [ASAP_Object_Type_ID]=2 then  [Salvage_Provider_Name]
--		when [ASAP_Object_Type_ID]= 3 then [Affiliate_Name]
--		when [ASAP_Object_Type_ID]=4 then  Tower_Name
--		when [ASAP_Object_Type_ID]=5 then    Vendor_Name
--			when [ASAP_Object_Type_ID]=6 then  Location_Name
--		when [ASAP_Object_Type_ID]=11 then send_to_Department
--		when [ASAP_Object_Type_ID]=18 then Non_State_DMV_Title_Processor_Name
--		else '' end 		VendorName
-- from #VendorNames
-- where Vendor_ID=1265
-- )x 
-- Where Ranks=1



Begin Tran
MERGE edw.dbo.DimVendor  AS DV
USING (

Select * from (select distinct Vendor_ID,ASAP_Object_Type_ID,ROW_NUMBER() over (partition by Vendor_ID,ASAP_Object_Type_ID order by Salvage_Expense_ID desc) Ranks, 
						Case 
								when [ASAP_Object_Type_ID]=2 then  [Salvage_Provider_Name]
								when [ASAP_Object_Type_ID]= 3 then [Affiliate_Name]
								when [ASAP_Object_Type_ID]=4 then  Tower_Name
								when [ASAP_Object_Type_ID]=5 then    Vendor_Name
								when [ASAP_Object_Type_ID]=6 then  Location_Name
								when [ASAP_Object_Type_ID]=11 then send_to_Department
								when [ASAP_Object_Type_ID]=18 then Non_State_DMV_Title_Processor_Name
								else '' end 		VendorName
						 from #VendorNames where   Vendor_ID is not null
						 )x
 Where Ranks=1
 
 )  AS SE ON DV.[VendorID] = SE.[Vendor_ID]	  and DV.ASAP_Object_Type_ID=SE.ASAP_Object_Type_ID
WHEN NOT MATCHED 
			THEN INSERT (ASAP_Object_Type_ID,[VendorID],   [VendorName],	[ETLLoadID],	[ETLLoadUpdateID]  )  
					  VALUES (ASAP_Object_Type_ID,[Vendor_ID], ISNULL([VendorName],'') ,  @ETLLoadID, @ETLLoadID) 

OUTPUT $action INTO #temp_TowerRecordCount;

Update FFT
set [DimVendorID]=dv.[DimVendorID]
From Edw.Dbo.FactFinancialTransaction  FFT
inner join   [EDW].[dbo].[DimVendor] dv on FFT.[ASAP_Object_Type_ID]=dv.[ASAP_Object_Type_ID] and FFT.VendorID=dv.VendorID
inner join #VendorNames se on DV.[VendorID] = SE.[Vendor_ID]	  and DV.ASAP_Object_Type_ID=SE.ASAP_Object_Type_ID
Where fft.[VendorID] is not null  and fft.[ASAP_Object_Type_ID] is not null
	  
  /*********************************************************************************************
Get the counts from the MERGE operation
*********************************************************************************************/
SELECT 
@UpdateRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'UPDATE' THEN 1 ELSE 0 END),0),
@InsertRowCount = ISNULL(SUM(CASE WHEN ChangeType = 'INSERT' THEN 1 ELSE 0 END),0)
from #temp_TowerRecordCount

/*********************************************************************************************
Return the change counts
*********************************************************************************************/
EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

COMMIT TRAN
		IF OBJECT_ID('tempdb..#VendorNames') IS NOT NULL DROP TABLE #VendorNames
		IF OBJECT_ID('tempdb..#Salvage_Expense') IS NOT NULL DROP TABLE #Salvage_Expense
		RETURN 0
	END TRY     

	BEGIN CATCH
		SET NOCOUNT OFF
		IF @@TRANCOUNT > 0 
			ROLLBACK TRAN
		
		IF OBJECT_ID('tempdb..#VendorNames') IS NOT NULL DROP TABLE #VendorNames	
		IF OBJECT_ID('tempdb..#Salvage_Expense') IS NOT NULL DROP TABLE #Salvage_Expense

		EXEC [dbo].[Usp_etlloadaudit] @StartEnd = 'E',@ETLLoadID = @ETLLoadID,@ErrorCode = @@ERROR, @UpdateRowCount = @UpdateRowCount,@InsertRowCount = @InsertRowCount

		DECLARE @ErrorMessage NVARCHAR(4000),@ErrorSeverity INT, @ErrorState INT;
		SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);

		RETURN -1
	END CATCH
	END

