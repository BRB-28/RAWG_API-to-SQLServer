USE [Staging_Games]
GO

/****** Object:  StoredProcedure [dbo].[Copy_Staging_To_Games]    Script Date: 5/29/2025 10:25:20 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[Copy_Staging_To_Games]
AS
BEGIN
	SET NOCOUNT ON;
	
	    BEGIN TRANSACTION;
	
	    BEGIN TRY
	        
	        DELETE FROM Games.dbo.GamePlatforms;
	        DELETE FROM Games.dbo.GameGenres;
	        DELETE FROM Games.dbo.GameStores;
	        DELETE FROM Games.dbo.GameTags;
	        DELETE FROM Games.dbo.Screenshots;
	        DELETE FROM Games.dbo.Platforms;
	        DELETE FROM Games.dbo.Genres;
	        DELETE FROM Games.dbo.Stores;
	        DELETE FROM Games.dbo.Tags;
	        DELETE FROM Games.dbo.Games;
	
	       
	        INSERT INTO Games.dbo.Games SELECT * FROM Staging_Games.dbo.Games;
	        INSERT INTO Games.dbo.Platforms SELECT * FROM Staging_Games.dbo.Platforms;
	        INSERT INTO Games.dbo.Genres SELECT * FROM Staging_Games.dbo.Genres;
	        INSERT INTO Games.dbo.Stores SELECT * FROM Staging_Games.dbo.Stores;
	        INSERT INTO Games.dbo.Tags SELECT * FROM Staging_Games.dbo.Tags;
	        INSERT INTO Games.dbo.GamePlatforms SELECT * FROM Staging_Games.dbo.GamePlatforms;
	        INSERT INTO Games.dbo.GameGenres SELECT * FROM Staging_Games.dbo.GameGenres;
	        INSERT INTO Games.dbo.GameStores SELECT * FROM Staging_Games.dbo.GameStores;
	        INSERT INTO Games.dbo.GameTags SELECT * FROM Staging_Games.dbo.GameTags;
	        INSERT INTO Games.dbo.Screenshots SELECT * FROM Staging_Games.dbo.Screenshots;
	
	        COMMIT TRANSACTION;
	    END TRY
	    BEGIN CATCH
	        ROLLBACK TRANSACTION;
	        THROW;
	    END CATCH
END;

GO


