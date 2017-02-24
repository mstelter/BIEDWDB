
/* 

==============================================================================
Author		: Mohammad Waheed
Create date	: 09/13/2016
Description	: Parse the XML data for file Envirnmental and Load it to the [EDW].[DBO].OHAInformation
==============================================================================
REVISION History
ChangeDate	Developer		Release/Problem Number	

==============================================================================
Usage Example:
--Exec [dbo].[usp_OHALoadXMLDataToOHAInformation_OP]

--EXEC EDW.[dbo].[usp_LoadXMLData] 
Select * from Edw.[dbo].[OHALoadRawXML]
--Truncate table EDW.[dbo].[OHALoadRawXML]
Select * from EDW.[dbo].[OHAFormsXMLData]
--Truncate table EDW.[dbo].[OHAFormsXMLData]
Select * from [EDW].[DBO].OHAInformation
--Truncate Table  [EDW].[DBO].OHAInformation
--Drop Table #XmlData
==============================================================================
*/
Create PROCEDURE [dbo].[usp_OHALoadXMLDataToOHAInformation_OP]
AS
  BEGIN

--select *
delete 
FROM [EDW].[DBO].OHAInformation  WHERE   Id in (	select oi.id
														from [EDW].[DBO].OHAInformation oi
															inner join EDW.[dbo].[OHALoadRawXML] lrx 
															          on  oi.[QtrYear]=lrx.[QtrYear] and
																	      oi.[BranchName]=lrx.[BranchName] and
												                          oi.FileModifiedDateTime<lrx.FileModifiedDateTime and
																		  oi.FileType=lrx.FileType
													   )



SELECT 
     FileType
    ,BranchName
	,QtrYear
	,FileModifiedDateTime

--<GOP>
,x.value('(/myFields/GQ1)[1]','varchar(2000)') AS GOP_Q1Question
,x.value('(/myFields/GQ2)[1]','varchar(2000)') AS GOP_Q2Question
,x.value('(/myFields/GQ3)[1]','varchar(2000)') AS GOP_Q3Question
,x.value('(/myFields/GQ4)[1]','varchar(2000)') AS GOP_Q4Question
,x.value('(/myFields/GQ5)[1]','varchar(2000)') AS GOP_Q5Question
,x.value('(/myFields/GQ6)[1]','varchar(2000)') AS GOP_Q6Question
,x.value('(/myFields/GQ7)[1]','varchar(2000)') AS GOP_Q7Question
,x.value('(/myFields/GQ8)[1]','varchar(2000)') AS GOP_Q8Question
,x.value('(/myFields/GQ9)[1]','varchar(2000)') AS GOP_Q9Question
,x.value('(/myFields/GQ10)[1]','varchar(2000)') AS GOP_Q10Question
,x.value('(/myFields/GQ11)[1]','varchar(2000)') AS GOP_Q11Question
,x.value('(/myFields/GQ12)[1]','varchar(2000)') AS GOP_Q12Question
,x.value('(/myFields/GQ13)[1]','varchar(2000)') AS GOP_Q13Question
,x.value('(/myFields/GQ14)[1]','varchar(2000)') AS GOP_Q14Question
,x.value('(/myFields/GQ15)[1]','varchar(2000)') AS GOP_Q15Question
,x.value('(/myFields/GQ16)[1]','varchar(2000)') AS GOP_Q16Question
,x.value('(/myFields/GQ17)[1]','varchar(2000)') AS GOP_Q17Question
,x.value('(/myFields/GQ18)[1]','varchar(2000)') AS GOP_Q18Question

	
,x.value('(/myFields/GOP/Q1YN)[1]','Int') AS GOP_Q1Answer
,x.value('(/myFields/GOP/Q2YN)[1]','Int') AS GOP_Q2Answer
,x.value('(/myFields/GOP/Q3YN)[1]','Int') AS GOP_Q3Answer
,x.value('(/myFields/GOP/Q4YN)[1]','Int') AS GOP_Q4Answer
,x.value('(/myFields/GOP/Q5YN)[1]','Int') AS GOP_Q5Answer
,x.value('(/myFields/GOP/Q6YN)[1]','Int') AS GOP_Q6Answer
,x.value('(/myFields/GOP/Q7YN)[1]','Int') AS GOP_Q7Answer
,x.value('(/myFields/GOP/Q8YN)[1]','Int') AS GOP_Q8Answer
,x.value('(/myFields/GOP/Q9YN)[1]','Int') AS GOP_Q9Answer
,x.value('(/myFields/GOP/Q10YN)[1]','Int') AS GOP_Q10Answer
,x.value('(/myFields/GOP/Q11YN)[1]','Int') AS GOP_Q11Answer
,x.value('(/myFields/GOP/Q12YN)[1]','Int') AS GOP_Q12Answer
,x.value('(/myFields/GOP/Q13YN)[1]','Int') AS GOP_Q13Answer
,x.value('(/myFields/GOP/Q14YN)[1]','Int') AS GOP_Q14Answer
,x.value('(/myFields/GOP/Q15YN)[1]','Int') AS GOP_Q15Answer
,x.value('(/myFields/GOP/Q16YN)[1]','Int') AS GOP_Q16Answer
,x.value('(/myFields/GOP/Q17YN)[1]','Int') AS GOP_Q17Answer
,x.value('(/myFields/GOP/Q18YN)[1]','Int') AS GOP_Q18Answer


,x.value('(/myFields/GQ1S)[1]','Float') AS GOP_Q1Score	
,x.value('(/myFields/GQ2S)[1]','Float') AS GOP_Q2Score	
,x.value('(/myFields/GQ3S)[1]','Float') AS GOP_Q3Score	
,x.value('(/myFields/GQ4S)[1]','Float') AS GOP_Q4Score	
,x.value('(/myFields/GQ5S)[1]','Float') AS GOP_Q5Score	
,x.value('(/myFields/GQ6S)[1]','Float') AS GOP_Q6Score	
,x.value('(/myFields/GQ7S)[1]','Float') AS GOP_Q7Score	
,x.value('(/myFields/GQ8S)[1]','Float') AS GOP_Q8Score	
,x.value('(/myFields/GQ9S)[1]','Float') AS GOP_Q9Score	
,x.value('(/myFields/GQ10S)[1]','Float') AS GOP_Q10Score	
,x.value('(/myFields/GQ11S)[1]','Float') AS GOP_Q11Score
,x.value('(/myFields/GQ12S)[1]','Float') AS GOP_Q12Score	
,x.value('(/myFields/GQ13S)[1]','Float') AS GOP_Q13Score	
,x.value('(/myFields/GQ14S)[1]','Float') AS GOP_Q14Score	
,x.value('(/myFields/GQ15S)[1]','Float') AS GOP_Q15Score	
,x.value('(/myFields/GQ16S)[1]','Float') AS GOP_Q16Score	
,x.value('(/myFields/GQ17S)[1]','Float') AS GOP_Q17Score	
,x.value('(/myFields/GQ18S)[1]','Float') AS GOP_Q18Score


,x.value('(/myFields/GOP/Q1OPP)[1]','varchar(2000)') AS GOP_Q1Comment
,x.value('(/myFields/GOP/Q2OPP)[1]','varchar(2000)') AS GOP_Q2Comment
,x.value('(/myFields/GOP/Q3OPP)[1]','varchar(2000)') AS GOP_Q3Comment
,x.value('(/myFields/GOP/Q4OPP)[1]','varchar(2000)') AS GOP_Q4Comment
,x.value('(/myFields/GOP/Q5OPP)[1]','varchar(2000)') AS GOP_Q5Comment
,x.value('(/myFields/GOP/Q6OPP)[1]','varchar(2000)') AS GOP_Q6Comment
,x.value('(/myFields/GOP/Q7OPP)[1]','varchar(2000)') AS GOP_Q7Comment
,x.value('(/myFields/GOP/Q8OPP)[1]','varchar(2000)') AS GOP_Q8Comment
,x.value('(/myFields/GOP/Q9OPP)[1]','varchar(2000)') AS GOP_Q9Comment
,x.value('(/myFields/GOP/Q10OPP)[1]','varchar(2000)') AS GOP_Q10Comment
,x.value('(/myFields/GOP/Q11OPP)[1]','varchar(2000)') AS GOP_Q11Comment
,x.value('(/myFields/GOP/Q12OPP)[1]','varchar(2000)') AS GOP_Q12Comment
,x.value('(/myFields/GOP/Q13OPP)[1]','varchar(2000)') AS GOP_Q13Comment
,x.value('(/myFields/GOP/Q14OPP)[1]','varchar(2000)') AS GOP_Q14Comment
,x.value('(/myFields/GOP/Q15OPP)[1]','varchar(2000)') AS GOP_Q15Comment
,x.value('(/myFields/GOP/Q16OPP)[1]','varchar(2000)') AS GOP_Q16Comment
,x.value('(/myFields/GOP/Q17OPP)[1]','varchar(2000)') AS GOP_Q17Comment
,x.value('(/myFields/GOP/Q18OPP)[1]','varchar(2000)') AS GOP_Q18Comment
---------------------------</GOP>-----------------------------

------------------------<REIT>--------------------------------
,x.value('(/myFields/RIQ1)[1]','varchar(2000)') AS REIT_Q1Question
,x.value('(/myFields/RIQ2)[1]','varchar(2000)') AS REIT_Q2Question
,x.value('(/myFields/RIQ3)[1]','varchar(2000)') AS REIT_Q3Question
,x.value('(/myFields/RIQ4)[1]','varchar(2000)') AS REIT_Q4Question
,x.value('(/myFields/RIQ5)[1]','varchar(2000)') AS REIT_Q5Question
,x.value('(/myFields/RIQ6)[1]','varchar(2000)') AS REIT_Q6Question
,x.value('(/myFields/RIQ7)[1]','varchar(2000)') AS REIT_Q7Question
,x.value('(/myFields/RIQ8)[1]','varchar(2000)') AS REIT_Q8Question
,x.value('(/myFields/RIQ9)[1]','varchar(2000)') AS REIT_Q9Question
,x.value('(/myFields/RIQ10)[1]','varchar(2000)') AS REIT_Q10Question
,x.value('(/myFields/RIQ11)[1]','varchar(2000)') AS REIT_Q11Question
,x.value('(/myFields/RIQ12)[1]','varchar(2000)') AS REIT_Q12Question
,x.value('(/myFields/RIQ13)[1]','varchar(2000)') AS REIT_Q13Question
,x.value('(/myFields/RIQ14)[1]','varchar(2000)') AS REIT_Q14Question
,x.value('(/myFields/RIQ15)[1]','varchar(2000)') AS REIT_Q15Question
,x.value('(/myFields/RIQ16)[1]','varchar(2000)') AS REIT_Q16Question
,x.value('(/myFields/RIQ17)[1]','varchar(2000)') AS REIT_Q17Question
,x.value('(/myFields/RIQ18)[1]','varchar(2000)') AS REIT_Q18Question
,x.value('(/myFields/RIQ19)[1]','varchar(2000)') AS REIT_Q19Question

	
,x.value('(/myFields/REIT/RIQ1YN)[1]','Int') AS REIT_Q1Answer
,x.value('(/myFields/REIT/RIQ2YN)[1]','Int') AS REIT_Q2Answer
,x.value('(/myFields/REIT/RIQ3YN)[1]','Int') AS REIT_Q3Answer
,x.value('(/myFields/REIT/RIQ4YN)[1]','Int') AS REIT_Q4Answer
,x.value('(/myFields/REIT/RIQ5YN)[1]','Int') AS REIT_Q5Answer
,x.value('(/myFields/REIT/RIQ6YN)[1]','Int') AS REIT_Q6Answer
,x.value('(/myFields/REIT/RIQ7YN)[1]','Int') AS REIT_Q7Answer
,x.value('(/myFields/REIT/RIQ8YN)[1]','Int') AS REIT_Q8Answer
,x.value('(/myFields/REIT/RIQ9YN)[1]','Int') AS REIT_Q9Answer
,x.value('(/myFields/REIT/RIQ10YN)[1]','Int') AS REIT_Q10Answer
,x.value('(/myFields/REIT/RIQ11YN)[1]','Int') AS REIT_Q11Answer
,x.value('(/myFields/REIT/RIQ12YN)[1]','Int') AS REIT_Q12Answer
,x.value('(/myFields/REIT/RIQ13YN)[1]','Int') AS REIT_Q13Answer
,x.value('(/myFields/REIT/RIQ14YN)[1]','Int') AS REIT_Q14Answer
,x.value('(/myFields/REIT/RIQ15YN)[1]','Int') AS REIT_Q15Answer
,x.value('(/myFields/REIT/RIQ16YN)[1]','Int') AS REIT_Q16Answer
,x.value('(/myFields/REIT/RIQ17YN)[1]','Int') AS REIT_Q17Answer
,x.value('(/myFields/REIT/RIQ18YN)[1]','Int') AS REIT_Q18Answer
,x.value('(/myFields/REIT/RIQ19YN)[1]','Int') AS REIT_Q19Answer

,x.value('(/myFields/RIQ1S)[1]','Float') AS REIT_Q1Score	
,x.value('(/myFields/RIQ2S)[1]','Float') AS REIT_Q2Score	
,x.value('(/myFields/RIQ3S)[1]','Float') AS REIT_Q3Score	
,x.value('(/myFields/RIQ4S)[1]','Float') AS REIT_Q4Score	
,x.value('(/myFields/RIQ5S)[1]','Float') AS REIT_Q5Score	
,x.value('(/myFields/RIQ6S)[1]','Float') AS REIT_Q6Score	
,x.value('(/myFields/RIQ7S)[1]','Float') AS REIT_Q7Score	
,x.value('(/myFields/RIQ8S)[1]','Float') AS REIT_Q8Score	
,x.value('(/myFields/RIQ9S)[1]','Float') AS REIT_Q9Score	
,x.value('(/myFields/RIQ10S)[1]','Float') AS REIT_Q10Score	
,x.value('(/myFields/RIQ11S)[1]','Float') AS REIT_Q11Score
,x.value('(/myFields/RIQ12S)[1]','Float') AS REIT_Q12Score	
,x.value('(/myFields/RIQ13S)[1]','Float') AS REIT_Q13Score	
,x.value('(/myFields/RIQ14S)[1]','Float') AS REIT_Q14Score	
,x.value('(/myFields/RIQ15S)[1]','Float') AS REIT_Q15Score	
,x.value('(/myFields/RIQ16S)[1]','Float') AS REIT_Q16Score	
,x.value('(/myFields/RIQ17S)[1]','Float') AS REIT_Q17Score	
,x.value('(/myFields/RIQ18S)[1]','Float') AS REIT_Q18Score
,x.value('(/myFields/RIQ19S)[1]','Float') AS REIT_Q19Score


,x.value('(/myFields/REIT/RIQ1OPP)[1]','varchar(2000)') AS REIT_Q1Comment
,x.value('(/myFields/REIT/RIQ2OPP)[1]','varchar(2000)') AS REIT_Q2Comment
,x.value('(/myFields/REIT/RIQ3OPP)[1]','varchar(2000)') AS REIT_Q3Comment
,x.value('(/myFields/REIT/RIQ4OPP)[1]','varchar(2000)') AS REIT_Q4Comment
,x.value('(/myFields/REIT/RIQ5OPP)[1]','varchar(2000)') AS REIT_Q5Comment
,x.value('(/myFields/REIT/RIQ6OPP)[1]','varchar(2000)') AS REIT_Q6Comment
,x.value('(/myFields/REIT/RIQ7OPP)[1]','varchar(2000)') AS REIT_Q7Comment
,x.value('(/myFields/REIT/RIQ8OPP)[1]','varchar(2000)') AS REIT_Q8Comment
,x.value('(/myFields/REIT/RIQ9OPP)[1]','varchar(2000)') AS REIT_Q9Comment
,x.value('(/myFields/REIT/RIQ10OPP)[1]','varchar(2000)') AS REIT_Q10Comment
,x.value('(/myFields/REIT/RIQ11OPP)[1]','varchar(2000)') AS REIT_Q11Comment
,x.value('(/myFields/REIT/RIQ12OPP)[1]','varchar(2000)') AS REIT_Q12Comment
,x.value('(/myFields/REIT/RIQ13OPP)[1]','varchar(2000)') AS REIT_Q13Comment
,x.value('(/myFields/REIT/RIQ14OPP)[1]','varchar(2000)') AS REIT_Q14Comment
,x.value('(/myFields/REIT/RIQ15OPP)[1]','varchar(2000)') AS REIT_Q15Comment
,x.value('(/myFields/REIT/RIQ16OPP)[1]','varchar(2000)') AS REIT_Q16Comment
,x.value('(/myFields/REIT/RIQ17OPP)[1]','varchar(2000)') AS REIT_Q17Comment
,x.value('(/myFields/REIT/RIQ18OPP)[1]','varchar(2000)') AS REIT_Q18Comment
,x.value('(/myFields/REIT/RIQ19OPP)[1]','varchar(2000)') AS REIT_Q19Comment
-------------------</REIT>-----------------------------------------

----------------------<RealEstate OutSide>--------------------------------
,x.value('(/myFields/Q1)[1]','varchar(2000)') AS RealEstateOutside_Q1Question
,x.value('(/myFields/Q2)[1]','varchar(2000)') AS RealEstateOutside_Q2Question
,x.value('(/myFields/Q3)[1]','varchar(2000)') AS RealEstateOutside_Q3Question
,x.value('(/myFields/Q4)[1]','varchar(2000)') AS RealEstateOutside_Q4Question
,x.value('(/myFields/Q5)[1]','varchar(2000)') AS RealEstateOutside_Q5Question
,x.value('(/myFields/Q6)[1]','varchar(2000)') AS RealEstateOutside_Q6Question
,x.value('(/myFields/Q7)[1]','varchar(2000)') AS RealEstateOutside_Q7Question
,x.value('(/myFields/Q8)[1]','varchar(2000)') AS RealEstateOutside_Q8Question
,x.value('(/myFields/Q9)[1]','varchar(2000)') AS RealEstateOutside_Q9Question
,x.value('(/myFields/Q10)[1]','varchar(2000)') AS RealEstateOutside_Q10Question
,x.value('(/myFields/Q11)[1]','varchar(2000)') AS RealEstateOutside_Q11Question
,x.value('(/myFields/Q12)[1]','varchar(2000)') AS RealEstateOutside_Q12Question
,x.value('(/myFields/Q13)[1]','varchar(2000)') AS RealEstateOutside_Q13Question
,x.value('(/myFields/Q14)[1]','varchar(2000)') AS RealEstateOutside_Q14Question
,x.value('(/myFields/Q15)[1]','varchar(2000)') AS RealEstateOutside_Q15Question
,x.value('(/myFields/Q16)[1]','varchar(2000)') AS RealEstateOutside_Q16Question
,x.value('(/myFields/Q17)[1]','varchar(2000)') AS RealEstateOutside_Q17Question
,x.value('(/myFields/Q18)[1]','varchar(2000)') AS RealEstateOutside_Q18Question
,x.value('(/myFields/Q19)[1]','varchar(2000)') AS RealEstateOutside_Q19Question
,x.value('(/myFields/Q20)[1]','varchar(2000)') AS RealEstateOutside_Q20Question
,x.value('(/myFields/Q21)[1]','varchar(2000)') AS RealEstateOutside_Q21Question
,x.value('(/myFields/Q22)[1]','varchar(2000)') AS RealEstateOutside_Q22Question

	
,x.value('(/myFields/RealEstate/ROQ1YN)[1]','Int') AS RealEstateOutside_Q1Answer
,x.value('(/myFields/RealEstate/ROQ2YN)[1]','Int') AS RealEstateOutside_Q2Answer
,x.value('(/myFields/RealEstate/ROQ3YN)[1]','Int') AS RealEstateOutside_Q3Answer
,x.value('(/myFields/RealEstate/ROQ4YN)[1]','Int') AS RealEstateOutside_Q4Answer
,x.value('(/myFields/RealEstate/ROQ5YN)[1]','Int') AS RealEstateOutside_Q5Answer
,x.value('(/myFields/RealEstate/ROQ6YN)[1]','Int') AS RealEstateOutside_Q6Answer
,x.value('(/myFields/RealEstate/ROQ7YN)[1]','Int') AS RealEstateOutside_Q7Answer
,x.value('(/myFields/RealEstate/ROQ8YN)[1]','Int') AS RealEstateOutside_Q8Answer
,x.value('(/myFields/RealEstate/ROQ9YN)[1]','Int') AS RealEstateOutside_Q9Answer
,x.value('(/myFields/RealEstate/ROQ10YN)[1]','Int') AS RealEstateOutside_Q10Answer
,x.value('(/myFields/RealEstate/ROQ11YN)[1]','Int') AS RealEstateOutside_Q11Answer
,x.value('(/myFields/RealEstate/ROQ12YN)[1]','Int') AS RealEstateOutside_Q12Answer
,x.value('(/myFields/RealEstate/ROQ13YN)[1]','Int') AS RealEstateOutside_Q13Answer
,x.value('(/myFields/RealEstate/ROQ14YN)[1]','Int') AS RealEstateOutside_Q14Answer
,x.value('(/myFields/RealEstate/ROQ15YN)[1]','Int') AS RealEstateOutside_Q15Answer
,x.value('(/myFields/RealEstate/ROQ16YN)[1]','Int') AS RealEstateOutside_Q16Answer
,x.value('(/myFields/RealEstate/ROQ17YN)[1]','Int') AS RealEstateOutside_Q17Answer
,x.value('(/myFields/RealEstate/ROQ18YN)[1]','Int') AS RealEstateOutside_Q18Answer
,x.value('(/myFields/RealEstate/ROQ19YN)[1]','Int') AS RealEstateOutside_Q19Answer
,x.value('(/myFields/RealEstate/ROQ20YN)[1]','Int') AS RealEstateOutside_Q20Answer
,x.value('(/myFields/RealEstate/ROQ21YN)[1]','Int') AS RealEstateOutside_Q21Answer
,x.value('(/myFields/RealEstate/ROQ22YN)[1]','Int') AS RealEstateOutside_Q22Answer


,x.value('(/myFields/ROQ1S)[1]','Float') AS RealEstateOutside_Q1Score	
,x.value('(/myFields/ROQ2S)[1]','Float') AS RealEstateOutside_Q2Score	
,x.value('(/myFields/ROQ3S)[1]','Float') AS RealEstateOutside_Q3Score	
,x.value('(/myFields/ROQ4S)[1]','Float') AS RealEstateOutside_Q4Score	
,x.value('(/myFields/ROQ5S)[1]','Float') AS RealEstateOutside_Q5Score	
,x.value('(/myFields/ROQ6S)[1]','Float') AS RealEstateOutside_Q6Score	
,x.value('(/myFields/ROQ7S)[1]','Float') AS RealEstateOutside_Q7Score	
,x.value('(/myFields/ROQ8S)[1]','Float') AS RealEstateOutside_Q8Score	
,x.value('(/myFields/ROQ9S)[1]','Float') AS RealEstateOutside_Q9Score	
,x.value('(/myFields/ROQ10S)[1]','Float') AS RealEstateOutside_Q10Score	
,x.value('(/myFields/ROQ11S)[1]','Float') AS RealEstateOutside_Q11Score
,x.value('(/myFields/ROQ12S)[1]','Float') AS RealEstateOutside_Q12Score	
,x.value('(/myFields/ROQ13S)[1]','Float') AS RealEstateOutside_Q13Score	
,x.value('(/myFields/ROQ14S)[1]','Float') AS RealEstateOutside_Q14Score	
,x.value('(/myFields/ROQ15S)[1]','Float') AS RealEstateOutside_Q15Score	
,x.value('(/myFields/ROQ16S)[1]','Float') AS RealEstateOutside_Q16Score	
,x.value('(/myFields/ROQ17S)[1]','Float') AS RealEstateOutside_Q17Score	
,x.value('(/myFields/ROQ18S)[1]','Float') AS RealEstateOutside_Q18Score
,x.value('(/myFields/ROQ19S)[1]','Float') AS RealEstateOutside_Q19Score	
,x.value('(/myFields/ROQ20S)[1]','Float') AS RealEstateOutside_Q20Score	
,x.value('(/myFields/ROQ21S)[1]','Float') AS RealEstateOutside_Q21Score	
,x.value('(/myFields/ROQ22S)[1]','Float') AS RealEstateOutside_Q22Score


,x.value('(/myFields/RealEstate/ROQ1OPP)[1]','varchar(2000)') AS RealEstateOutside_Q1Comment
,x.value('(/myFields/RealEstate/ROQ2OPP)[1]','varchar(2000)') AS RealEstateOutside_Q2Comment
,x.value('(/myFields/RealEstate/ROQ3OPP)[1]','varchar(2000)') AS RealEstateOutside_Q3Comment
,x.value('(/myFields/RealEstate/ROQ4OPP)[1]','varchar(2000)') AS RealEstateOutside_Q4Comment
,x.value('(/myFields/RealEstate/ROQ5OPP)[1]','varchar(2000)') AS RealEstateOutside_Q5Comment
,x.value('(/myFields/RealEstate/ROQ6OPP)[1]','varchar(2000)') AS RealEstateOutside_Q6Comment
,x.value('(/myFields/RealEstate/ROQ7OPP)[1]','varchar(2000)') AS RealEstateOutside_Q7Comment
,x.value('(/myFields/RealEstate/ROQ8OPP)[1]','varchar(2000)') AS RealEstateOutside_Q8Comment
,x.value('(/myFields/RealEstate/ROQ9OPP)[1]','varchar(2000)') AS RealEstateOutside_Q9Comment
,x.value('(/myFields/RealEstate/ROQ10OPP)[1]','varchar(2000)') AS RealEstateOutside_Q10Comment
,x.value('(/myFields/RealEstate/ROQ11OPP)[1]','varchar(2000)') AS RealEstateOutside_Q11Comment
,x.value('(/myFields/RealEstate/ROQ12OPP)[1]','varchar(2000)') AS RealEstateOutside_Q12Comment
,x.value('(/myFields/RealEstate/ROQ13OPP)[1]','varchar(2000)') AS RealEstateOutside_Q13Comment
,x.value('(/myFields/RealEstate/ROQ14OPP)[1]','varchar(2000)') AS RealEstateOutside_Q14Comment
,x.value('(/myFields/RealEstate/ROQ15OPP)[1]','varchar(2000)') AS RealEstateOutside_Q15Comment
,x.value('(/myFields/RealEstate/ROQ16OPP)[1]','varchar(2000)') AS RealEstateOutside_Q16Comment
,x.value('(/myFields/RealEstate/ROQ17OPP)[1]','varchar(2000)') AS RealEstateOutside_Q17Comment
,x.value('(/myFields/RealEstate/ROQ18OPP)[1]','varchar(2000)') AS RealEstateOutside_Q18Comment
,x.value('(/myFields/RealEstate/ROQ19OPP)[1]','varchar(2000)') AS RealEstateOutside_Q19Comment
,x.value('(/myFields/RealEstate/ROQ20OPP)[1]','varchar(2000)') AS RealEstateOutside_Q20Comment
,x.value('(/myFields/RealEstate/ROQ21OPP)[1]','varchar(2000)') AS RealEstateOutside_Q21Comment
,x.value('(/myFields/RealEstate/ROQ22OPP)[1]','varchar(2000)') AS RealEstateOutside_Q22Comment
-----------------------</RealEstate>----------------------------------------


-----------------------<Titles>-------------------------------------------
,x.value('(/myFields/TQ1)[1]','varchar(2000)') AS Titles_Q1Question
,x.value('(/myFields/TQ2)[1]','varchar(2000)') AS Titles_Q2Question
,x.value('(/myFields/TQ3)[1]','varchar(2000)') AS Titles_Q3Question
,x.value('(/myFields/TQ4)[1]','varchar(2000)') AS Titles_Q4Question
,x.value('(/myFields/TQ5)[1]','varchar(2000)') AS Titles_Q5Question
,x.value('(/myFields/TQ6)[1]','varchar(2000)') AS Titles_Q6Question
,x.value('(/myFields/TQ7)[1]','varchar(2000)') AS Titles_Q7Question
,x.value('(/myFields/TQ8)[1]','varchar(2000)') AS Titles_Q8Question
,x.value('(/myFields/TQ9)[1]','varchar(2000)') AS Titles_Q9Question
,x.value('(/myFields/TQ10)[1]','varchar(2000)') AS Titles_Q10Question
,x.value('(/myFields/TQ11)[1]','varchar(2000)') AS Titles_Q11Question
,x.value('(/myFields/TQ12)[1]','varchar(2000)') AS Titles_Q12Question
,x.value('(/myFields/TQ13)[1]','varchar(2000)') AS Titles_Q13Question
,x.value('(/myFields/TQ14)[1]','varchar(2000)') AS Titles_Q14Question
,x.value('(/myFields/TQ15)[1]','varchar(2000)') AS Titles_Q15Question
,x.value('(/myFields/TQ16)[1]','varchar(2000)') AS Titles_Q16Question


	
,x.value('(/myFields/Titles/TQ1YN)[1]','Int') AS Titles_Q1Answer
,x.value('(/myFields/Titles/TQ2YN)[1]','Int') AS Titles_Q2Answer
,x.value('(/myFields/Titles/TQ3YN)[1]','Int') AS Titles_Q3Answer
,x.value('(/myFields/Titles/TQ4YN)[1]','Int') AS Titles_Q4Answer
,x.value('(/myFields/Titles/TQ5YN)[1]','Int') AS Titles_Q5Answer
,x.value('(/myFields/Titles/TQ6YN)[1]','Int') AS Titles_Q6Answer
,x.value('(/myFields/Titles/TQ7YN)[1]','Int') AS Titles_Q7Answer
,x.value('(/myFields/Titles/TQ8YN)[1]','Int') AS Titles_Q8Answer
,x.value('(/myFields/Titles/TQ9YN)[1]','Int') AS Titles_Q9Answer
,x.value('(/myFields/Titles/TQ10YN)[1]','Int') AS Titles_Q10Answer
,x.value('(/myFields/Titles/TQ11YN)[1]','Int') AS Titles_Q11Answer
,x.value('(/myFields/Titles/TQ12YN)[1]','Int') AS Titles_Q12Answer
,x.value('(/myFields/Titles/TQ13YN)[1]','Int') AS Titles_Q13Answer
,x.value('(/myFields/Titles/TQ14YN)[1]','Int') AS Titles_Q14Answer
,x.value('(/myFields/Titles/TQ15YN)[1]','Int') AS Titles_Q15Answer
,x.value('(/myFields/Titles/TQ16YN)[1]','Int') AS Titles_Q16Answer



,x.value('(/myFields/TQ1S)[1]','Float') AS Titles_Q1Score	
,x.value('(/myFields/TQ2S)[1]','Float') AS Titles_Q2Score	
,x.value('(/myFields/TQ3S)[1]','Float') AS Titles_Q3Score	
,x.value('(/myFields/TQ4S)[1]','Float') AS Titles_Q4Score	
,x.value('(/myFields/TQ5S)[1]','Float') AS Titles_Q5Score	
,x.value('(/myFields/TQ6S)[1]','Float') AS Titles_Q6Score	
,x.value('(/myFields/TQ7S)[1]','Float') AS Titles_Q7Score	
,x.value('(/myFields/TQ8S)[1]','Float') AS Titles_Q8Score	
,x.value('(/myFields/TQ9S)[1]','Float') AS Titles_Q9Score	
,x.value('(/myFields/TQ10S)[1]','Float') AS Titles_Q10Score	
,x.value('(/myFields/TQ11S)[1]','Float') AS Titles_Q11Score
,x.value('(/myFields/TQ12S)[1]','Float') AS Titles_Q12Score	
,x.value('(/myFields/TQ13S)[1]','Float') AS Titles_Q13Score	
,x.value('(/myFields/TQ14S)[1]','Float') AS Titles_Q14Score	
,x.value('(/myFields/TQ15S)[1]','Float') AS Titles_Q15Score	
,x.value('(/myFields/TQ16S)[1]','Float') AS Titles_Q16Score	



,x.value('(/myFields/Titles/TQ1OPP)[1]','varchar(2000)') AS Titles_Q1Comment
,x.value('(/myFields/Titles/TQ2OPP)[1]','varchar(2000)') AS Titles_Q2Comment
,x.value('(/myFields/Titles/TQ3OPP)[1]','varchar(2000)') AS Titles_Q3Comment
,x.value('(/myFields/Titles/TQ4OPP)[1]','varchar(2000)') AS Titles_Q4Comment
,x.value('(/myFields/Titles/TQ5OPP)[1]','varchar(2000)') AS Titles_Q5Comment
,x.value('(/myFields/Titles/TQ6OPP)[1]','varchar(2000)') AS Titles_Q6Comment
,x.value('(/myFields/Titles/TQ7OPP)[1]','varchar(2000)') AS Titles_Q7Comment
,x.value('(/myFields/Titles/TQ8OPP)[1]','varchar(2000)') AS Titles_Q8Comment
,x.value('(/myFields/Titles/TQ9OPP)[1]','varchar(2000)') AS Titles_Q9Comment
,x.value('(/myFields/Titles/TQ10OPP)[1]','varchar(2000)') AS Titles_Q10Comment
,x.value('(/myFields/Titles/TQ11OPP)[1]','varchar(2000)') AS Titles_Q11Comment
,x.value('(/myFields/Titles/TQ12OPP)[1]','varchar(2000)') AS Titles_Q12Comment
,x.value('(/myFields/Titles/TQ13OPP)[1]','varchar(2000)') AS Titles_Q13Comment
,x.value('(/myFields/Titles/TQ14OPP)[1]','varchar(2000)') AS Titles_Q14Comment
,x.value('(/myFields/Titles/TQ15OPP)[1]','varchar(2000)') AS Titles_Q15Comment
,x.value('(/myFields/Titles/TQ16OPP)[1]','varchar(2000)') AS Titles_Q16Comment
-----------------------</Titles>-------------------------------------------

--------------------------<AI>---------------------------------------------
,x.value('(/myFields/AIQ1)[1]','varchar(2000)') AS AI_Q1Question
,x.value('(/myFields/AIQ2)[1]','varchar(2000)') AS AI_Q2Question
,x.value('(/myFields/AIQ3)[1]','varchar(2000)') AS AI_Q3Question
,x.value('(/myFields/AIQ4)[1]','varchar(2000)') AS AI_Q4Question
,x.value('(/myFields/AIQ5)[1]','varchar(2000)') AS AI_Q5Question
,x.value('(/myFields/AIQ6)[1]','varchar(2000)') AS AI_Q6Question
,x.value('(/myFields/AIQ7)[1]','varchar(2000)') AS AI_Q7Question
,x.value('(/myFields/AIQ8)[1]','varchar(2000)') AS AI_Q8Question
,x.value('(/myFields/AIQ9)[1]','varchar(2000)') AS AI_Q9Question
,x.value('(/myFields/AIQ10)[1]','varchar(2000)') AS AI_Q10Question
,x.value('(/myFields/AIQ11)[1]','varchar(2000)') AS AI_Q11Question
,x.value('(/myFields/AIQ12)[1]','varchar(2000)') AS AI_Q12Question
,x.value('(/myFields/AIQ13)[1]','varchar(2000)') AS AI_Q13Question

	
,x.value('(/myFields/AI/AIQ1YN)[1]','Int') AS AI_Q1Answer
,x.value('(/myFields/AI/AIQ2YN)[1]','Int') AS AI_Q2Answer
,x.value('(/myFields/AI/AIQ3YN)[1]','Int') AS AI_Q3Answer
,x.value('(/myFields/AI/AIQ4YN)[1]','Int') AS AI_Q4Answer
,x.value('(/myFields/AI/AIQ5YN)[1]','Int') AS AI_Q5Answer
,x.value('(/myFields/AI/AIQ6YN)[1]','Int') AS AI_Q6Answer
,x.value('(/myFields/AI/AIQ7YN)[1]','Int') AS AI_Q7Answer
,x.value('(/myFields/AI/AIQ8YN)[1]','Int') AS AI_Q8Answer
,x.value('(/myFields/AI/AIQ9YN)[1]','Int') AS AI_Q9Answer
,x.value('(/myFields/AI/AIQ10YN)[1]','Int') AS AI_Q10Answer
,x.value('(/myFields/AI/AIQ11YN)[1]','Int') AS AI_Q11Answer
,x.value('(/myFields/AI/AIQ12YN)[1]','Int') AS AI_Q12Answer
,x.value('(/myFields/AI/AIQ13YN)[1]','Int') AS AI_Q13Answer



,x.value('(/myFields/AIQ1S)[1]','Float') AS AI_Q1Score	
,x.value('(/myFields/AIQ2S)[1]','Float') AS AI_Q2Score	
,x.value('(/myFields/AIQ3S)[1]','Float') AS AI_Q3Score	
,x.value('(/myFields/AIQ4S)[1]','Float') AS AI_Q4Score	
,x.value('(/myFields/AIQ5S)[1]','Float') AS AI_Q5Score	
,x.value('(/myFields/AIQ6S)[1]','Float') AS AI_Q6Score	
,x.value('(/myFields/AIQ7S)[1]','Float') AS AI_Q7Score	
,x.value('(/myFields/AIQ8S)[1]','Float') AS AI_Q8Score	
,x.value('(/myFields/AIQ9S)[1]','Float') AS AI_Q9Score	
,x.value('(/myFields/AIQ10S)[1]','Float') AS AI_Q10Score	
,x.value('(/myFields/AIQ11S)[1]','Float') AS AI_Q11Score
,x.value('(/myFields/AIQ12S)[1]','Float') AS AI_Q12Score	
,x.value('(/myFields/AIQ13S)[1]','Float') AS AI_Q13Score	


,x.value('(/myFields/AI/AIQ1OPP)[1]','varchar(2000)') AS AI_Q1Comment
,x.value('(/myFields/AI/AIQ2OPP)[1]','varchar(2000)') AS AI_Q2Comment
,x.value('(/myFields/AI/AIQ3OPP)[1]','varchar(2000)') AS AI_Q3Comment
,x.value('(/myFields/AI/AIQ4OPP)[1]','varchar(2000)') AS AI_Q4Comment
,x.value('(/myFields/AI/AIQ5OPP)[1]','varchar(2000)') AS AI_Q5Comment
,x.value('(/myFields/AI/AIQ6OPP)[1]','varchar(2000)') AS AI_Q6Comment
,x.value('(/myFields/AI/AIQ7OPP)[1]','varchar(2000)') AS AI_Q7Comment
,x.value('(/myFields/AI/AIQ8OPP)[1]','varchar(2000)') AS AI_Q8Comment
,x.value('(/myFields/AI/AIQ9OPP)[1]','varchar(2000)') AS AI_Q9Comment
,x.value('(/myFields/AI/AIQ10OPP)[1]','varchar(2000)') AS AI_Q10Comment
,x.value('(/myFields/AI/AIQ11OPP)[1]','varchar(2000)') AS AI_Q11Comment
,x.value('(/myFields/AI/AIQ12OPP)[1]','varchar(2000)') AS AI_Q12Comment
,x.value('(/myFields/AI/AIQ13OPP)[1]','varchar(2000)') AS AI_Q13Comment
--------------------------</AI>-------------------------------------------


--------------------------</AO>-------------------------------------------
,x.value('(/myFields/AOQ1)[1]','varchar(2000)') AS AO_Q1Question
,x.value('(/myFields/AOQ2)[1]','varchar(2000)') AS AO_Q2Question
,x.value('(/myFields/AOQ3)[1]','varchar(2000)') AS AO_Q3Question
,x.value('(/myFields/AOQ4)[1]','varchar(2000)') AS AO_Q4Question
,x.value('(/myFields/AOQ5)[1]','varchar(2000)') AS AO_Q5Question
,x.value('(/myFields/AOQ6)[1]','varchar(2000)') AS AO_Q6Question
,x.value('(/myFields/AOQ7)[1]','varchar(2000)') AS AO_Q7Question
,x.value('(/myFields/AOQ8)[1]','varchar(2000)') AS AO_Q8Question
,x.value('(/myFields/AOQ9)[1]','varchar(2000)') AS AO_Q9Question
,x.value('(/myFields/AOQ10)[1]','varchar(2000)') AS AO_Q10Question
,x.value('(/myFields/AOQ11)[1]','varchar(2000)') AS AO_Q11Question
,x.value('(/myFields/AOQ12)[1]','varchar(2000)') AS AO_Q12Question
,x.value('(/myFields/AOQ13)[1]','varchar(2000)') AS AO_Q13Question
,x.value('(/myFields/AOQ14)[1]','varchar(2000)') AS AO_Q14Question
,x.value('(/myFields/AOQ15)[1]','varchar(2000)') AS AO_Q15Question
,x.value('(/myFields/AOQ16)[1]','varchar(2000)') AS AO_Q16Question
,x.value('(/myFields/AOQ17)[1]','varchar(2000)') AS AO_Q17Question
,x.value('(/myFields/AOQ18)[1]','varchar(2000)') AS AO_Q18Question
,x.value('(/myFields/AOQ19)[1]','varchar(2000)') AS AO_Q19Question
,x.value('(/myFields/AOQ20)[1]','varchar(2000)') AS AO_Q20Question
	
,x.value('(/myFields/AOQ1YN)[1]','Int') AS AO_Q1Answer
,x.value('(/myFields/AOQ2YN)[1]','Int') AS AO_Q2Answer
,x.value('(/myFields/AOQ3YN)[1]','Int') AS AO_Q3Answer
,x.value('(/myFields/AOQ4YN)[1]','Int') AS AO_Q4Answer
,x.value('(/myFields/AOQ5YN)[1]','Int') AS AO_Q5Answer
,x.value('(/myFields/AOQ6YN)[1]','Int') AS AO_Q6Answer
,x.value('(/myFields/AOQ7YN)[1]','Int') AS AO_Q7Answer
,x.value('(/myFields/AOQ8YN)[1]','Int') AS AO_Q8Answer
,x.value('(/myFields/AOQ9YN)[1]','Int') AS AO_Q9Answer
,x.value('(/myFields/AOQ10YN)[1]','Int') AS AO_Q10Answer
,x.value('(/myFields/AOQ11YN)[1]','Int') AS AO_Q11Answer
,x.value('(/myFields/AOQ12YN)[1]','Int') AS AO_Q12Answer
,x.value('(/myFields/AOQ13YN)[1]','Int') AS AO_Q13Answer
,x.value('(/myFields/AOQ14YN)[1]','Int') AS AO_Q14Answer
,x.value('(/myFields/AOQ15YN)[1]','Int') AS AO_Q15Answer
,x.value('(/myFields/AOQ16YN)[1]','Int') AS AO_Q16Answer
,x.value('(/myFields/AOQ17YN)[1]','Int') AS AO_Q17Answer
,x.value('(/myFields/AOQ18YN)[1]','Int') AS AO_Q18Answer
,x.value('(/myFields/AOQ19YN)[1]','Int') AS AO_Q19Answer
,x.value('(/myFields/AOQ20YN)[1]','Int') AS AO_Q20Answer

,x.value('(/myFields/AOQ1S)[1]','Float') AS AO_Q1Score	
,x.value('(/myFields/AOQ2S)[1]','Float') AS AO_Q2Score	
,x.value('(/myFields/AOQ3S)[1]','Float') AS AO_Q3Score	
,x.value('(/myFields/AOQ4S)[1]','Float') AS AO_Q4Score	
,x.value('(/myFields/AOQ5S)[1]','Float') AS AO_Q5Score	
,x.value('(/myFields/AOQ6S)[1]','Float') AS AO_Q6Score	
,x.value('(/myFields/AOQ7S)[1]','Float') AS AO_Q7Score	
,x.value('(/myFields/AOQ8S)[1]','Float') AS AO_Q8Score	
,x.value('(/myFields/AOQ9S)[1]','Float') AS AO_Q9Score	
,x.value('(/myFields/AOQ10S)[1]','Float') AS AO_Q10Score	
,x.value('(/myFields/AOQ11S)[1]','Float') AS AO_Q11Score
,x.value('(/myFields/AOQ12S)[1]','Float') AS AO_Q12Score	
,x.value('(/myFields/AOQ13S)[1]','Float') AS AO_Q13Score	
,x.value('(/myFields/AOQ14S)[1]','Float') AS AO_Q14Score	
,x.value('(/myFields/AOQ15S)[1]','Float') AS AO_Q15Score	
,x.value('(/myFields/AOQ16S)[1]','Float') AS AO_Q16Score	
,x.value('(/myFields/AOQ17S)[1]','Float') AS AO_Q17Score	
,x.value('(/myFields/AOQ18S)[1]','Float') AS AO_Q18Score
,x.value('(/myFields/AOQ19S)[1]','Float') AS AO_Q19Score
,x.value('(/myFields/AOQ20S)[1]','Float') AS AO_Q20Score

,x.value('(/myFields/AOQ1OPP)[1]','varchar(2000)') AS AO_Q1Comment
,x.value('(/myFields/AOQ2OPP)[1]','varchar(2000)') AS AO_Q2Comment
,x.value('(/myFields/AOQ3OPP)[1]','varchar(2000)') AS AO_Q3Comment
,x.value('(/myFields/AOQ4OPP)[1]','varchar(2000)') AS AO_Q4Comment
,x.value('(/myFields/AOQ5OPP)[1]','varchar(2000)') AS AO_Q5Comment
,x.value('(/myFields/AOQ6OPP)[1]','varchar(2000)') AS AO_Q6Comment
,x.value('(/myFields/AOQ7OPP)[1]','varchar(2000)') AS AO_Q7Comment
,x.value('(/myFields/AOQ8OPP)[1]','varchar(2000)') AS AO_Q8Comment
,x.value('(/myFields/AOQ9OPP)[1]','varchar(2000)') AS AO_Q9Comment
,x.value('(/myFields/AOQ10OPP)[1]','varchar(2000)') AS AO_Q10Comment
,x.value('(/myFields/AOQ11OPP)[1]','varchar(2000)') AS AO_Q11Comment
,x.value('(/myFields/AOQ12OPP)[1]','varchar(2000)') AS AO_Q12Comment
,x.value('(/myFields/AOQ13OPP)[1]','varchar(2000)') AS AO_Q13Comment
,x.value('(/myFields/AOQ14OPP)[1]','varchar(2000)') AS AO_Q14Comment
,x.value('(/myFields/AOQ15OPP)[1]','varchar(2000)') AS AO_Q15Comment
,x.value('(/myFields/AOQ16OPP)[1]','varchar(2000)') AS AO_Q16Comment
,x.value('(/myFields/AOQ17OPP)[1]','varchar(2000)') AS AO_Q17Comment
,x.value('(/myFields/AOQ18OPP)[1]','varchar(2000)') AS AO_Q18Comment
,x.value('(/myFields/AOQ19OPP)[1]','varchar(2000)') AS AO_Q19Comment
,x.value('(/myFields/AOQ20OPP)[1]','varchar(2000)') AS AO_Q20Comment
--------------------------</AO>-------------------------------------------

into #XmlData	
from [EDW].[DBO].[OHAFormsXMLData] 
Cross Apply [XMLData].nodes('/myFields') AS T(x)
Where FileType='OP'
and (  FileType+[QtrYear]+[BranchName] not in (select FileType+[QtrYear]+[BranchName] from Edw.dbo.OHAInformation ) )


--select * from #XmlData


--------------------------GOP---------------------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #GOP_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,GOP_Q1Question,GOP_Q2Question,GOP_Q3Question,GOP_Q4Question,GOP_Q5Question,GOP_Q6Question,GOP_Q7Question,GOP_Q8Question,GOP_Q9Question,GOP_Q10Question,GOP_Q11Question,GOP_Q12Question,GOP_Q13Question,GOP_Q14Question,GOP_Q15Question,GOP_Q16Question,GOP_Q17Question,GOP_Q18Question
	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
									(  
										GOP_Q1Question,GOP_Q2Question,GOP_Q3Question,GOP_Q4Question,GOP_Q5Question,GOP_Q6Question,GOP_Q7Question,GOP_Q8Question,GOP_Q9Question,GOP_Q10Question,GOP_Q11Question,GOP_Q12Question,GOP_Q13Question,GOP_Q14Question,GOP_Q15Question,GOP_Q16Question,GOP_Q17Question,GOP_Q18Question
									)
	) as unpvt
--Select * from #GOP_Question


Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #GOP_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,GOP_Q1Answer,GOP_Q2Answer,GOP_Q3Answer,GOP_Q4Answer,GOP_Q5Answer,GOP_Q6Answer,GOP_Q7Answer,GOP_Q8Answer,GOP_Q9Answer,GOP_Q10Answer,GOP_Q11Answer,GOP_Q12Answer,GOP_Q13Answer,GOP_Q14Answer,GOP_Q15Answer,GOP_Q16Answer,GOP_Q17Answer,GOP_Q18Answer
	 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					( 
					GOP_Q1Answer,GOP_Q2Answer,GOP_Q3Answer,GOP_Q4Answer,GOP_Q5Answer,GOP_Q6Answer,GOP_Q7Answer,GOP_Q8Answer,GOP_Q9Answer,GOP_Q10Answer,GOP_Q11Answer,GOP_Q12Answer,GOP_Q13Answer,GOP_Q14Answer,GOP_Q15Answer,GOP_Q16Answer,GOP_Q17Answer,GOP_Q18Answer
					)
	) as unpvt
--Select * from #GOP_Answer


Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #GOP_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,GOP_Q1Score,GOP_Q2Score,GOP_Q3Score,GOP_Q4Score,GOP_Q5Score,GOP_Q6Score,GOP_Q7Score,GOP_Q8Score,GOP_Q9Score,GOP_Q10Score,GOP_Q11Score,GOP_Q12Score,GOP_Q13Score,GOP_Q14Score,GOP_Q15Score,GOP_Q16Score,GOP_Q17Score,GOP_Q18Score



	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					(  
					GOP_Q1Score,GOP_Q2Score,GOP_Q3Score,GOP_Q4Score,GOP_Q5Score,GOP_Q6Score,GOP_Q7Score,GOP_Q8Score,GOP_Q9Score,GOP_Q10Score,GOP_Q11Score,GOP_Q12Score,GOP_Q13Score,GOP_Q14Score,GOP_Q15Score,GOP_Q16Score,GOP_Q17Score,GOP_Q18Score
					)
	) as unpvt
--Select * from #GOP_Score

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #GOP_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,GOP_Q1Comment,GOP_Q2Comment,GOP_Q3Comment,GOP_Q4Comment,GOP_Q5Comment,GOP_Q6Comment,GOP_Q7Comment,GOP_Q8Comment,GOP_Q9Comment,GOP_Q10Comment,GOP_Q11Comment,GOP_Q12Comment,GOP_Q13Comment,GOP_Q14Comment,GOP_Q15Comment,GOP_Q16Comment,GOP_Q17Comment,GOP_Q18Comment
	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					GOP_Q1Comment,GOP_Q2Comment,GOP_Q3Comment,GOP_Q4Comment,GOP_Q5Comment,GOP_Q6Comment,GOP_Q7Comment,GOP_Q8Comment,GOP_Q9Comment,GOP_Q10Comment,GOP_Q11Comment,GOP_Q12Comment,GOP_Q13Comment,GOP_Q14Comment,GOP_Q15Comment,GOP_Q16Comment,GOP_Q17Comment,GOP_Q18Comment
					)
	) as unpvt
--Select * from #GOP_Comment
--------------------------End of GOP---------------------------------------


--------------------------<REIT>---------------------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #REIT_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,REIT_Q1Question,REIT_Q2Question,REIT_Q3Question,REIT_Q4Question,REIT_Q5Question,REIT_Q6Question,REIT_Q7Question,REIT_Q8Question,REIT_Q9Question,REIT_Q10Question,REIT_Q11Question,REIT_Q12Question,REIT_Q13Question,REIT_Q14Question,REIT_Q15Question,REIT_Q16Question,REIT_Q17Question,REIT_Q18Question,REIT_Q19Question
	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					REIT_Q1Question,REIT_Q2Question,REIT_Q3Question,REIT_Q4Question,REIT_Q5Question,REIT_Q6Question,REIT_Q7Question,REIT_Q8Question,REIT_Q9Question,REIT_Q10Question,REIT_Q11Question,REIT_Q12Question,REIT_Q13Question,REIT_Q14Question,REIT_Q15Question,REIT_Q16Question,REIT_Q17Question,REIT_Q18Question,REIT_Q19Question
					)
	) as unpvt
--Select * from #REIT_Question


Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #REIT_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,REIT_Q1Answer,REIT_Q2Answer,REIT_Q3Answer,REIT_Q4Answer,REIT_Q5Answer,REIT_Q6Answer,REIT_Q7Answer,REIT_Q8Answer,REIT_Q9Answer,REIT_Q10Answer,REIT_Q11Answer,REIT_Q12Answer,REIT_Q13Answer,REIT_Q14Answer,REIT_Q15Answer,REIT_Q16Answer,REIT_Q17Answer,REIT_Q18Answer,REIT_Q19Answer
		 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					( 
					REIT_Q1Answer,REIT_Q2Answer,REIT_Q3Answer,REIT_Q4Answer,REIT_Q5Answer,REIT_Q6Answer,REIT_Q7Answer,REIT_Q8Answer,REIT_Q9Answer,REIT_Q10Answer,REIT_Q11Answer,REIT_Q12Answer,REIT_Q13Answer,REIT_Q14Answer,REIT_Q15Answer,REIT_Q16Answer,REIT_Q17Answer,REIT_Q18Answer,REIT_Q19Answer
					)
	) as unpvt
	--Select * from #REIT_Answer


Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #REIT_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,REIT_Q1Score,REIT_Q2Score,REIT_Q3Score,REIT_Q4Score,REIT_Q5Score,REIT_Q6Score,REIT_Q7Score,REIT_Q8Score,REIT_Q9Score,REIT_Q10Score,REIT_Q11Score,REIT_Q12Score,REIT_Q13Score,REIT_Q14Score,REIT_Q15Score,REIT_Q16Score,REIT_Q17Score,REIT_Q18Score,REIT_Q19Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					(  
					REIT_Q1Score,REIT_Q2Score,REIT_Q3Score,REIT_Q4Score,REIT_Q5Score,REIT_Q6Score,REIT_Q7Score,REIT_Q8Score,REIT_Q9Score,REIT_Q10Score,REIT_Q11Score,REIT_Q12Score,REIT_Q13Score,REIT_Q14Score,REIT_Q15Score,REIT_Q16Score,REIT_Q17Score,REIT_Q18Score,REIT_Q19Score
					)
	) as unpvt
--Select * from #REIT_Score

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #REIT_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,REIT_Q1Comment,REIT_Q2Comment,REIT_Q3Comment,REIT_Q4Comment,REIT_Q5Comment,REIT_Q6Comment,REIT_Q7Comment,REIT_Q8Comment,REIT_Q9Comment,REIT_Q10Comment,REIT_Q11Comment,REIT_Q12Comment,REIT_Q13Comment,REIT_Q14Comment,REIT_Q15Comment,REIT_Q16Comment,REIT_Q17Comment,REIT_Q18Comment,REIT_Q19Comment

	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					REIT_Q1Comment,REIT_Q2Comment,REIT_Q3Comment,REIT_Q4Comment,REIT_Q5Comment,REIT_Q6Comment,REIT_Q7Comment,REIT_Q8Comment,REIT_Q9Comment,REIT_Q10Comment,REIT_Q11Comment,REIT_Q12Comment,REIT_Q13Comment,REIT_Q14Comment,REIT_Q15Comment,REIT_Q16Comment,REIT_Q17Comment,REIT_Q18Comment,REIT_Q19Comment
					)
	) as unpvt
--Select * from #REIT_Comment
--------------------------</REIT>---------------------------------------


--------------------------RealEstateOutside---------------------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #RealEstateOutside_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,RealEstateOutside_Q1Question,RealEstateOutside_Q2Question,RealEstateOutside_Q3Question,RealEstateOutside_Q4Question,RealEstateOutside_Q5Question,RealEstateOutside_Q6Question,RealEstateOutside_Q7Question,RealEstateOutside_Q8Question,RealEstateOutside_Q9Question,RealEstateOutside_Q10Question,RealEstateOutside_Q11Question,RealEstateOutside_Q12Question,RealEstateOutside_Q13Question,RealEstateOutside_Q14Question,RealEstateOutside_Q15Question,RealEstateOutside_Q16Question,RealEstateOutside_Q17Question,RealEstateOutside_Q18Question,RealEstateOutside_Q19Question,RealEstateOutside_Q20Question,RealEstateOutside_Q21Question,RealEstateOutside_Q22Question
	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					RealEstateOutside_Q1Question,RealEstateOutside_Q2Question,RealEstateOutside_Q3Question,RealEstateOutside_Q4Question,RealEstateOutside_Q5Question,RealEstateOutside_Q6Question,RealEstateOutside_Q7Question,RealEstateOutside_Q8Question,RealEstateOutside_Q9Question,RealEstateOutside_Q10Question,RealEstateOutside_Q11Question,RealEstateOutside_Q12Question,RealEstateOutside_Q13Question,RealEstateOutside_Q14Question,RealEstateOutside_Q15Question,RealEstateOutside_Q16Question,RealEstateOutside_Q17Question,RealEstateOutside_Q18Question,RealEstateOutside_Q19Question,RealEstateOutside_Q20Question,RealEstateOutside_Q21Question,RealEstateOutside_Q22Question
					)
	) as unpvt
--Select * from #RealEstateOutside_Question


Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #RealEstateOutside_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,RealEstateOutside_Q1Answer,RealEstateOutside_Q2Answer,RealEstateOutside_Q3Answer,RealEstateOutside_Q4Answer,RealEstateOutside_Q5Answer,RealEstateOutside_Q6Answer,RealEstateOutside_Q7Answer,RealEstateOutside_Q8Answer,RealEstateOutside_Q9Answer,RealEstateOutside_Q10Answer,RealEstateOutside_Q11Answer,RealEstateOutside_Q12Answer,RealEstateOutside_Q13Answer,RealEstateOutside_Q14Answer,RealEstateOutside_Q15Answer,RealEstateOutside_Q16Answer,RealEstateOutside_Q17Answer,RealEstateOutside_Q18Answer,RealEstateOutside_Q19Answer,RealEstateOutside_Q20Answer,RealEstateOutside_Q21Answer,RealEstateOutside_Q22Answer
		 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					( 
					RealEstateOutside_Q1Answer,RealEstateOutside_Q2Answer,RealEstateOutside_Q3Answer,RealEstateOutside_Q4Answer,RealEstateOutside_Q5Answer,RealEstateOutside_Q6Answer,RealEstateOutside_Q7Answer,RealEstateOutside_Q8Answer,RealEstateOutside_Q9Answer,RealEstateOutside_Q10Answer,RealEstateOutside_Q11Answer,RealEstateOutside_Q12Answer,RealEstateOutside_Q13Answer,RealEstateOutside_Q14Answer,RealEstateOutside_Q15Answer,RealEstateOutside_Q16Answer,RealEstateOutside_Q17Answer,RealEstateOutside_Q18Answer,RealEstateOutside_Q19Answer,RealEstateOutside_Q20Answer,RealEstateOutside_Q21Answer,RealEstateOutside_Q22Answer
					)
	) as unpvt
--Select * from #RealEstateOutside_Answer


Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #RealEstateOutside_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,RealEstateOutside_Q1Score,RealEstateOutside_Q2Score,RealEstateOutside_Q3Score,RealEstateOutside_Q4Score,RealEstateOutside_Q5Score,RealEstateOutside_Q6Score,RealEstateOutside_Q7Score,RealEstateOutside_Q8Score,RealEstateOutside_Q9Score,RealEstateOutside_Q10Score,RealEstateOutside_Q11Score,RealEstateOutside_Q12Score,RealEstateOutside_Q13Score,RealEstateOutside_Q14Score,RealEstateOutside_Q15Score,RealEstateOutside_Q16Score,RealEstateOutside_Q17Score,RealEstateOutside_Q18Score,RealEstateOutside_Q19Score,RealEstateOutside_Q20Score,RealEstateOutside_Q21Score,RealEstateOutside_Q22Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					(  
					RealEstateOutside_Q1Score,RealEstateOutside_Q2Score,RealEstateOutside_Q3Score,RealEstateOutside_Q4Score,RealEstateOutside_Q5Score,RealEstateOutside_Q6Score,RealEstateOutside_Q7Score,RealEstateOutside_Q8Score,RealEstateOutside_Q9Score,RealEstateOutside_Q10Score,RealEstateOutside_Q11Score,RealEstateOutside_Q12Score,RealEstateOutside_Q13Score,RealEstateOutside_Q14Score,RealEstateOutside_Q15Score,RealEstateOutside_Q16Score,RealEstateOutside_Q17Score,RealEstateOutside_Q18Score,RealEstateOutside_Q19Score,RealEstateOutside_Q20Score,RealEstateOutside_Q21Score,RealEstateOutside_Q22Score
					)
	) as unpvt
--Select * from #RealEstateOutside_Score

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #RealEstateOutside_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,RealEstateOutside_Q1Comment,RealEstateOutside_Q2Comment,RealEstateOutside_Q3Comment,RealEstateOutside_Q4Comment,RealEstateOutside_Q5Comment,RealEstateOutside_Q6Comment,RealEstateOutside_Q7Comment,RealEstateOutside_Q8Comment,RealEstateOutside_Q9Comment,RealEstateOutside_Q10Comment,RealEstateOutside_Q11Comment,RealEstateOutside_Q12Comment,RealEstateOutside_Q13Comment,RealEstateOutside_Q14Comment,RealEstateOutside_Q15Comment,RealEstateOutside_Q16Comment,RealEstateOutside_Q17Comment,RealEstateOutside_Q18Comment,RealEstateOutside_Q19Comment,RealEstateOutside_Q20Comment,RealEstateOutside_Q21Comment,RealEstateOutside_Q22Comment
	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					RealEstateOutside_Q1Comment,RealEstateOutside_Q2Comment,RealEstateOutside_Q3Comment,RealEstateOutside_Q4Comment,RealEstateOutside_Q5Comment,RealEstateOutside_Q6Comment,RealEstateOutside_Q7Comment,RealEstateOutside_Q8Comment,RealEstateOutside_Q9Comment,RealEstateOutside_Q10Comment,RealEstateOutside_Q11Comment,RealEstateOutside_Q12Comment,RealEstateOutside_Q13Comment,RealEstateOutside_Q14Comment,RealEstateOutside_Q15Comment,RealEstateOutside_Q16Comment,RealEstateOutside_Q17Comment,RealEstateOutside_Q18Comment,RealEstateOutside_Q19Comment,RealEstateOutside_Q20Comment,RealEstateOutside_Q21Comment,RealEstateOutside_Q22Comment
					)
	) as unpvt
--Select * from #RealEstateOutside_Comment
--------------------------</RealEstateOutside>---------------------------------------



--------------------------Titles---------------------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #Titles_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,Titles_Q1Question,Titles_Q2Question,Titles_Q3Question,Titles_Q4Question,Titles_Q5Question,Titles_Q6Question,Titles_Q7Question,Titles_Q8Question,Titles_Q9Question,Titles_Q10Question,Titles_Q11Question,Titles_Q12Question,Titles_Q13Question,Titles_Q14Question,Titles_Q15Question,Titles_Q16Question
	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					Titles_Q1Question,Titles_Q2Question,Titles_Q3Question,Titles_Q4Question,Titles_Q5Question,Titles_Q6Question,Titles_Q7Question,Titles_Q8Question,Titles_Q9Question,Titles_Q10Question,Titles_Q11Question,Titles_Q12Question,Titles_Q13Question,Titles_Q14Question,Titles_Q15Question,Titles_Q16Question
					)
	) as unpvt
--Select * from #Titles_Question


Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #Titles_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,Titles_Q1Answer,Titles_Q2Answer,Titles_Q3Answer,Titles_Q4Answer,Titles_Q5Answer,Titles_Q6Answer,Titles_Q7Answer,Titles_Q8Answer,Titles_Q9Answer,Titles_Q10Answer,Titles_Q11Answer,Titles_Q12Answer,Titles_Q13Answer,Titles_Q14Answer,Titles_Q15Answer,Titles_Q16Answer

	 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					( 
					Titles_Q1Answer,Titles_Q2Answer,Titles_Q3Answer,Titles_Q4Answer,Titles_Q5Answer,Titles_Q6Answer,Titles_Q7Answer,Titles_Q8Answer,Titles_Q9Answer,Titles_Q10Answer,Titles_Q11Answer,Titles_Q12Answer,Titles_Q13Answer,Titles_Q14Answer,Titles_Q15Answer,Titles_Q16Answer
					)
	) as unpvt
--Select * from #Titles_Answer


Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #Titles_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,Titles_Q1Score,Titles_Q2Score,Titles_Q3Score,Titles_Q4Score,Titles_Q5Score,Titles_Q6Score,Titles_Q7Score,Titles_Q8Score,Titles_Q9Score,Titles_Q10Score,Titles_Q11Score,Titles_Q12Score,Titles_Q13Score,Titles_Q14Score,Titles_Q15Score,Titles_Q16Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					(  
					Titles_Q1Score,Titles_Q2Score,Titles_Q3Score,Titles_Q4Score,Titles_Q5Score,Titles_Q6Score,Titles_Q7Score,Titles_Q8Score,Titles_Q9Score,Titles_Q10Score,Titles_Q11Score,Titles_Q12Score,Titles_Q13Score,Titles_Q14Score,Titles_Q15Score,Titles_Q16Score
					)
	) as unpvt
--Select * from #Titles_Score

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #Titles_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,Titles_Q1Comment,Titles_Q2Comment,Titles_Q3Comment,Titles_Q4Comment,Titles_Q5Comment,Titles_Q6Comment,Titles_Q7Comment,Titles_Q8Comment,Titles_Q9Comment,Titles_Q10Comment,Titles_Q11Comment,Titles_Q12Comment,Titles_Q13Comment,Titles_Q14Comment,Titles_Q15Comment,Titles_Q16Comment

	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					Titles_Q1Comment,Titles_Q2Comment,Titles_Q3Comment,Titles_Q4Comment,Titles_Q5Comment,Titles_Q6Comment,Titles_Q7Comment,Titles_Q8Comment,Titles_Q9Comment,Titles_Q10Comment,Titles_Q11Comment,Titles_Q12Comment,Titles_Q13Comment,Titles_Q14Comment,Titles_Q15Comment,Titles_Q16Comment
					)
	) as unpvt
--Select * from #Titles_Comment
--------------------------</Titles>---------------------------------------




--------------------------AI---------------------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #AI_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,AI_Q1Question,AI_Q2Question,AI_Q3Question,AI_Q4Question,AI_Q5Question,AI_Q6Question,AI_Q7Question,AI_Q8Question,AI_Q9Question,AI_Q10Question,AI_Q11Question,AI_Q12Question,AI_Q13Question
	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					AI_Q1Question,AI_Q2Question,AI_Q3Question,AI_Q4Question,AI_Q5Question,AI_Q6Question,AI_Q7Question,AI_Q8Question,AI_Q9Question,AI_Q10Question,AI_Q11Question,AI_Q12Question,AI_Q13Question
					)
	) as unpvt
--Select * from #AI_Question


Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #AI_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,AI_Q1Answer,AI_Q2Answer,AI_Q3Answer,AI_Q4Answer,AI_Q5Answer,AI_Q6Answer,AI_Q7Answer,AI_Q8Answer,AI_Q9Answer,AI_Q10Answer,AI_Q11Answer,AI_Q12Answer,AI_Q13Answer


	 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					( 
					AI_Q1Answer,AI_Q2Answer,AI_Q3Answer,AI_Q4Answer,AI_Q5Answer,AI_Q6Answer,AI_Q7Answer,AI_Q8Answer,AI_Q9Answer,AI_Q10Answer,AI_Q11Answer,AI_Q12Answer,AI_Q13Answer

					)
	) as unpvt
--Select * from #AI_Answer


Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #AI_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,AI_Q1Score,AI_Q2Score,AI_Q3Score,AI_Q4Score,AI_Q5Score,AI_Q6Score,AI_Q7Score,AI_Q8Score,AI_Q9Score,AI_Q10Score,AI_Q11Score,AI_Q12Score,AI_Q13Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					(  
					AI_Q1Score,AI_Q2Score,AI_Q3Score,AI_Q4Score,AI_Q5Score,AI_Q6Score,AI_Q7Score,AI_Q8Score,AI_Q9Score,AI_Q10Score,AI_Q11Score,AI_Q12Score,AI_Q13Score
					)
	) as unpvt
--Select * from #AI_Score

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #AI_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,AI_Q1Comment,AI_Q2Comment,AI_Q3Comment,AI_Q4Comment,AI_Q5Comment,AI_Q6Comment,AI_Q7Comment,AI_Q8Comment,AI_Q9Comment,AI_Q10Comment,AI_Q11Comment,AI_Q12Comment,AI_Q13Comment
	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					AI_Q1Comment,AI_Q2Comment,AI_Q3Comment,AI_Q4Comment,AI_Q5Comment,AI_Q6Comment,AI_Q7Comment,AI_Q8Comment,AI_Q9Comment,AI_Q10Comment,AI_Q11Comment,AI_Q12Comment,AI_Q13Comment
					)
	) as unpvt
--Select * from #AI_Comment
--------------------------</AI>---------------------------------------




--------------------------AO---------------------------------------
Select BranchName,	QtrYear,FileModifiedDateTime,FileType,QuestionName, Question
into #AO_Question
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,AO_Q1Question,AO_Q2Question,AO_Q3Question,AO_Q4Question,AO_Q5Question,AO_Q6Question,AO_Q7Question,AO_Q8Question,AO_Q9Question,AO_Q10Question,AO_Q11Question,AO_Q12Question,AO_Q13Question,AO_Q14Question,AO_Q15Question,AO_Q16Question,AO_Q17Question,AO_Q18Question,AO_Q19Question,AO_Q20Question
	 From #XmlData) as P
UNPIVOT
	(Question for QuestionName in 
					(  
					AO_Q1Question,AO_Q2Question,AO_Q3Question,AO_Q4Question,AO_Q5Question,AO_Q6Question,AO_Q7Question,AO_Q8Question,AO_Q9Question,AO_Q10Question,AO_Q11Question,AO_Q12Question,AO_Q13Question,AO_Q14Question,AO_Q15Question,AO_Q16Question,AO_Q17Question,AO_Q18Question,AO_Q19Question,AO_Q20Question
					)
	) as unpvt
--Select * from #AO_Question


Select BranchName,	QtrYear,FileModifiedDateTime,FileType, AnswerName, Answer
Into #AO_Answer
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,AO_Q1Answer,AO_Q2Answer,AO_Q3Answer,AO_Q4Answer,AO_Q5Answer,AO_Q6Answer,AO_Q7Answer,AO_Q8Answer,AO_Q9Answer,AO_Q10Answer,AO_Q11Answer,AO_Q12Answer,AO_Q13Answer,AO_Q14Answer,AO_Q15Answer,AO_Q16Answer,AO_Q17Answer,AO_Q18Answer,AO_Q19Answer
	 From #XmlData) as P
UNPIVOT
	(Answer for AnswerName in 
					( 
					AO_Q1Answer,AO_Q2Answer,AO_Q3Answer,AO_Q4Answer,AO_Q5Answer,AO_Q6Answer,AO_Q7Answer,AO_Q8Answer,AO_Q9Answer,AO_Q10Answer,AO_Q11Answer,AO_Q12Answer,AO_Q13Answer,AO_Q14Answer,AO_Q15Answer,AO_Q16Answer,AO_Q17Answer,AO_Q18Answer,AO_Q19Answer
					)
	) as unpvt
--Select * from #AO_Answer


Select BranchName,	QtrYear,FileModifiedDateTime,FileType,ScoreName, Score
into #AO_Score
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,AO_Q1Score,AO_Q2Score,AO_Q3Score,AO_Q4Score,AO_Q5Score,AO_Q6Score,AO_Q7Score,AO_Q8Score,AO_Q9Score,AO_Q10Score,AO_Q11Score,AO_Q12Score,AO_Q13Score,AO_Q14Score,AO_Q15Score,AO_Q16Score,AO_Q17Score,AO_Q18Score,AO_Q19Score,AO_Q20Score
	 From #XmlData) as P
UNPIVOT
	(Score for ScoreName in 
					(  
					AO_Q1Score,AO_Q2Score,AO_Q3Score,AO_Q4Score,AO_Q5Score,AO_Q6Score,AO_Q7Score,AO_Q8Score,AO_Q9Score,AO_Q10Score,AO_Q11Score,AO_Q12Score,AO_Q13Score,AO_Q14Score,AO_Q15Score,AO_Q16Score,AO_Q17Score,AO_Q18Score,AO_Q19Score,AO_Q20Score
					)
	) as unpvt
--Select * from #AO_Score

Select BranchName,	QtrYear,FileModifiedDateTime,FileType,CommentName, Comment
into #AO_Comment
From 
	(Select BranchName,	QtrYear,FileModifiedDateTime,FileType,AO_Q1Comment,AO_Q2Comment,AO_Q3Comment,AO_Q4Comment,AO_Q5Comment,AO_Q6Comment,AO_Q7Comment,AO_Q8Comment,AO_Q9Comment,AO_Q10Comment,AO_Q11Comment,AO_Q12Comment,AO_Q13Comment,AO_Q14Comment,AO_Q15Comment,AO_Q16Comment,AO_Q17Comment,AO_Q18Comment,AO_Q19Comment,AO_Q20Comment

	 From #XmlData) as P
UNPIVOT
	(Comment for CommentName in 
					( 
					AO_Q1Comment,AO_Q2Comment,AO_Q3Comment,AO_Q4Comment,AO_Q5Comment,AO_Q6Comment,AO_Q7Comment,AO_Q8Comment,AO_Q9Comment,AO_Q10Comment,AO_Q11Comment,AO_Q12Comment,AO_Q13Comment,AO_Q14Comment,AO_Q15Comment,AO_Q16Comment,AO_Q17Comment,AO_Q18Comment,AO_Q19Comment,AO_Q20Comment

					)
	) as unpvt
--Select * from #AO_Comment
--------------------------</AO>---------------------------------------
--select len(question),* from #RealEstateOutside_Question
       
insert into OHAInformation
(BranchName,QtrYear,FileModifiedDateTime,FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment)
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment
from #GOP_Question  q 
left join #GOP_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #GOP_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #GOP_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType
union all 
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment
from #REIT_Question  q 
left join #REIT_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #REIT_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #REIT_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType
union all 
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment
from #RealEstateOutside_Question  q 
left join #RealEstateOutside_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #RealEstateOutside_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #RealEstateOutside_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType
union all 
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment
from #Titles_Question  q 
left join #Titles_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #Titles_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #Titles_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType
union all 
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment
from #AI_Question  q 
left join #AI_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #AI_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #AI_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType
union all 
Select q.BranchName,q.QtrYear,q.FileModifiedDateTime,q.FileType,QuestionName,AnswerName,ScoreName,CommentName, Question,Answer,Score,Comment
from #AO_Question  q 
left join #AO_Score s on   Replace(Replace(s.ScoreName,'Q',''),'Score','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=s.BranchName and q.QtrYear=s.QtrYear and q.FileType=s.FileType
left join #AO_Answer a on  Replace(Replace(a.AnswerName,'Q',''),'Answer','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=a.BranchName and q.QtrYear=a.QtrYear and q.FileType=a.FileType
left join #AO_Comment c on Replace(Replace(c.CommentName,'Q',''),'Comment','')=Replace(Replace(q.QuestionName,'Question',''),'Q','') and q.BranchName=c.BranchName and q.QtrYear=c.QtrYear and q.FileType=c.FileType




End