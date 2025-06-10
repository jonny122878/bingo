USE [p89880749_test]
--GO

--SET ANSI_NULLS ON
--GO

--SET QUOTED_IDENTIFIER ON
--GO

--CREATE TABLE [dbo].[bingo](
--	[drawTerm] [int] NOT NULL,
--	[dDate] [varchar](100) NOT NULL,
--	[bigShowOrder] [varchar](100) NOT NULL,
--	createDate DATETIME DEFAULT GETDATE()
--) ON [PRIMARY]
--GO


--DELETE FROM [dbo].[bingo] 

SELECT * FROM [dbo].[bingo] 
--WHERE dDate = '2025-04-01'
ORDER BY drawTerm 


