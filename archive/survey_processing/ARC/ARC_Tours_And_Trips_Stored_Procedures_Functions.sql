--ARC Project on coding tours AND trips using Regional Household Travel Survey Data
--Stored Procedures AND User defined Functions
--Palvinder Singh				10/24/2012				singhp@pbworld.com
-------------------------------------------------------------------------------------------------------------------------------------

--Functions 
-------------------------------------------------------------------------------------------------------------------------------------
--1)Link Trips Fields Function
-------------------------------------------------------------------------------------------------------------------------------------
-- Called by Part-3 while linking trips for change mode activities

IF OBJECT_ID('dbo.LinkTripsFields') IS NOT NULL 
	DROP FUNCTION LinkTripsFields;
GO
CREATE FUNCTION dbo.LinkTripsFields(@i int,@k int,@hh int,@per int)
RETURNS @LinkTrips TABLE 
(Link_BEGCHMODE int,
 Link_ENDCHMODE int,
 Link_BOARDING_PLACENO int,
 Link_BOARDING_PNAME VARCHAR(500),
 Link_BOARDING_X real,
 Link_BOARDING_Y real,
 Link_ALIGHTING_PLACENO int,
 Link_ALIGHTING_PNAME VARCHAR(500),
 Link_ALIGHTING_X real,
 Link_ALIGHTING_Y real,
 Link_PARKING_PLACENO int,
 Link_PARKING_PNAME VARCHAR(500),
 Link_PARKING_X real,
 Link_PARKING_Y real,
 Link_TOLL int,
 Link_TOTTR int,
 Link_OTAZ int,
 Link_ORIGIN_LONG real,
 Link_ORIGIN_LAT real,
 Link_OPNAME VARCHAR(100),
 Link_ORIGIN_DEPTIME int,
 Link_TRPDUR int,
 Link_TRIPDIST real
 )
AS  
BEGIN
	DECLARE @Link_BEGCHMODE int,@Link_ENDCHMODE int,@Link_BOARDING_PLACENO int,
	@Link_BOARDING_PNAME VARCHAR(500),@Link_BOARDING_X real,@Link_BOARDING_Y real,
	@Link_ALIGHTING_PLACENO int,@Link_ALIGHTING_PNAME VARCHAR(500),@Link_ALIGHTING_X real,
	@Link_ALIGHTING_Y real,@Link_PARKING_PLACENO int,@Link_PARKING_PNAME VARCHAR(500),
	@Link_PARKING_X real,@Link_PARKING_Y real,@Link_TOLL int,@Link_TOTTR int,@Link_OTAZ int,
	@Link_ORIGIN_LONG real,@Link_ORIGIN_LAT real,@Link_OPNAME VARCHAR(100),
	@Link_ORIGIN_DEPTIME int,@Link_TRPDUR int,@Link_TRIPDIST real,@park1 int,@park2 int; 
		
	--Identify PLANO where ChMoIdentifier = 1 starts between two zeroes
	SET @Link_BEGCHMODE = (SELECT TOP(1) PLANO FROM templace
								WHERE ChMoIdentifier = 1 AND Ro_gen < @i AND Ro_gen > @k AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen);
	--Identify PLANO where ChMoIdentifier = 1 ends between two zeroes
	SET @Link_ENDCHMODE = (SELECT TOP(1) PLANO FROM templace
											WHERE ChMoIdentifier = 1 AND Ro_gen < @i AND Ro_gen > @k  AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen desc);									
	--Creating Boarding related fields
	SET @Link_BOARDING_PLACENO = (SELECT TOP(1) ff.PLANO - 1 
									FROM (SELECT * FROM templace
											WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
												AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per) AS ff 
									WHERE MODENAME IN ('LOCALBUS','EXPRESSBUS','RAIL') AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen);
	SET @Link_BOARDING_PNAME = (SELECT TOP(1) ff.OPNAME 
									FROM (SELECT * FROM templace 
											WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
												AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per) AS ff 
									WHERE MODENAME IN ('LOCALBUS','EXPRESSBUS','RAIL')AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen);
	SET @Link_BOARDING_X = (SELECT TOP(1) ff.ORIGIN_LONG 
								FROM (SELECT * FROM templace
										WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
											AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per) AS ff 
								WHERE MODENAME IN ('LOCALBUS','EXPRESSBUS','RAIL') AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen);
	SET @Link_BOARDING_Y = (SELECT TOP(1) ff.ORIGIN_LAT 
								FROM (SELECT * FROM templace
										WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
											AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per) AS ff 
								WHERE MODENAME IN ('LOCALBUS','EXPRESSBUS','RAIL') AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen);
	--Creating Alighting related fields																				
	SET @Link_ALIGHTING_PLACENO = (SELECT TOP(1) ff.PLANO 
									FROM (SELECT * FROM templace 
											WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
												AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per) AS ff 
										WHERE MODENAME IN ('LOCALBUS','EXPRESSBUS','RAIL') AND hhseq = @hh AND PERNO = @per ORDER BY PLANO DESC);
	SET @Link_ALIGHTING_PNAME = (SELECT TOP(1) ff.DPNAME 
									FROM (SELECT * FROM templace 
											WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
												AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per) AS ff 
									WHERE MODENAME IN ('LOCALBUS','EXPRESSBUS','RAIL') AND hhseq = @hh AND PERNO = @per ORDER BY PLANO DESC);										
	SET @Link_ALIGHTING_X = (SELECT TOP(1) ff.DEST_LONG 
								FROM (SELECT * FROM templace 
										WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
											AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per) AS ff 
								WHERE MODENAME IN ('LOCALBUS','EXPRESSBUS','RAIL') AND hhseq = @hh AND PERNO = @per ORDER BY PLANO DESC);
	SET @Link_ALIGHTING_Y = (SELECT TOP(1) ff.DEST_LAT
								FROM (SELECT * FROM templace
										WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
											AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per) AS ff 
								WHERE MODENAME IN ('LOCALBUS','EXPRESSBUS','RAIL') AND hhseq = @hh AND PERNO = @per ORDER BY PLANO DESC);
	
	--Creating parking related fields --Check whether taxi to be included or not
	SET @park1 = (SELECT TOP(1) ff.Ro_gen - 1 
					FROM (SELECT * FROM templace
							WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
								AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per) AS ff
					WHERE MODENAME IN ('RAIL','LOCALBUS','EXPRESSBUS','SCHOOLBUS','WALK','PASSENGER') AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen);
	SET @park2 = (SELECT TOP(1) ff.Ro_gen  
					FROM (SELECT * FROM templace
							WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
								AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per) AS ff
					WHERE MODENAME IN ('RAIL','LOCALBUS','EXPRESSBUS','SCHOOLBUS','WALK','PASSENGER') AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen desc);
	
	SET @Link_PARKING_PLACENO = (SELECT TOP(1) PLANO FROM templace
							WHERE (Ro_gen = @park1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per)
								OR (Ro_gen = @park2 AND (SELECT MODENAME FROM templace WHERE Ro_gen = @park2 + 1 AND hhseq = @hh AND PERNO = @per)= 'DRIVER' AND hhseq = @hh AND PERNO = @per) ORDER BY Ro_gen);	
	SET @Link_PARKING_PNAME = (SELECT TOP(1) PNAME FROM templace 
							WHERE (Ro_gen = @park1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per)
								OR (Ro_gen = @park2 AND (SELECT MODENAME FROM templace WHERE Ro_gen = @park2 + 1 AND hhseq = @hh AND PERNO = @per)= 'DRIVER' AND hhseq = @hh AND PERNO = @per) ORDER BY Ro_gen);
	SET @Link_PARKING_X = (SELECT TOP(1) XCORD FROM templace
						WHERE (Ro_gen = @park1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per)
							OR (Ro_gen = @park2 AND (SELECT MODENAME FROM templace WHERE Ro_gen = @park2 + 1 AND hhseq = @hh AND PERNO = @per)= 'DRIVER' AND hhseq = @hh AND PERNO = @per) ORDER BY Ro_gen);			
	SET @Link_PARKING_Y = (SELECT TOP(1) YCORD FROM templace
						WHERE (Ro_gen = @park1 AND MODENAME = 'DRIVER' AND hhseq = @hh AND PERNO = @per)
							OR (Ro_gen = @park2 AND (SELECT MODENAME FROM templace WHERE Ro_gen = @park2 + 1 AND hhseq = @hh AND PERNO = @per)= 'DRIVER' AND hhseq = @hh AND PERNO = @per) ORDER BY Ro_gen);	
							
	--Recode Toll field for linkedtrips where No toll('Free') is prefered over toll ('Pay')
	SET @Link_TOLL = (SELECT TOP(1) TOLL FROM templace 
					WHERE TOLL = (SELECT MIN(TOLL) FROM templace 
									WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
										AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per)
						AND PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
						AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen);									
	--Recode TOTTR field for linkedtrips to maximum of tottr on linkedtrip										
	SET @Link_TOTTR = (SELECT TOP(1) TOTTR FROM templace 
					WHERE TOTTR = (SELECT MAX(TOTTR) FROM templace 
										WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) 
											AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per)
						AND PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i)
						AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per ORDER BY Ro_gen);								
	--Recode fields where Origin of trip will change
	SET @Link_OTAZ = (SELECT OTAZ FROM templace
					WHERE PLANO = (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per);
	SET @Link_ORIGIN_LONG = (SELECT ORIGIN_LONG FROM templace 
							WHERE PLANO = (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per);
	SET @Link_ORIGIN_LAT = (SELECT ORIGIN_LAT FROM templace
							WHERE PLANO = (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per); 
	SET @Link_OPNAME = (SELECT OPNAME FROM templace
						WHERE PLANO = (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per); 
	SET @Link_ORIGIN_DEPTIME = (SELECT ORIGIN_DEPTIME FROM templace
								WHERE PLANO = (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per);
	SET @Link_TRPDUR = (SELECT SUM (TRPDUR) FROM templace 
						WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i)
							AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per);
	SET @Link_TRIPDIST = (SELECT SUM (TRIPDIST) FROM templace
						WHERE PLANO >= (SELECT BegChMode FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i)
							AND PLANO <= (SELECT (EndChMode + 1) FROM templace WHERE hhseq = @hh AND PERNO = @per AND Ro_gen = @i) AND hhseq = @hh AND PERNO = @per) 
							
	INSERT @LinkTrips SELECT @Link_BEGCHMODE,@Link_ENDCHMODE,@Link_BOARDING_PLACENO,@Link_BOARDING_PNAME,
	@Link_BOARDING_X,@Link_BOARDING_Y,@Link_ALIGHTING_PLACENO,@Link_ALIGHTING_PNAME,@Link_ALIGHTING_X,
	@Link_ALIGHTING_Y,@Link_PARKING_PLACENO,@Link_PARKING_PNAME,@Link_PARKING_X,@Link_PARKING_Y,@Link_TOLL,@Link_TOTTR,
	@Link_OTAZ,@Link_ORIGIN_LONG,@Link_ORIGIN_LAT,@Link_OPNAME,@Link_ORIGIN_DEPTIME,@Link_TRPDUR,@Link_TRIPDIST
	RETURN
END
GO
-------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------
--2) Out bound Stop Fields Function
-------------------------------------------------------------------------------------------------------------------------------------
-- Called by StopActivityFields stored procedure while coding trips AND tours in Part-4
IF OBJECT_ID('dbo.OutboundStopFields') IS NOT NULL 
	DROP FUNCTION OutboundStopFields;
GO
CREATE FUNCTION dbo.OutboundStopFields(@i int)
RETURNS @OutStopTable TABLE 
(STOP_PLACENO int,
 STOP_XCORD real,
 STOP_YCORD real,
 STOP_ARR_HR int,
 STOP_ARR_MIN int,
 STOP_DEP_HR int,
 STOP_DEP_MIN int,
 STOP_DUR_HR int,
 STOP_DUR_MIN int,
 STOP_PURP VARCHAR(75))
AS  
BEGIN
--STOP ACTIVITY Fields
	DECLARE @STOP_PLACENO AS int,@STOP_XCORD AS real,@STOP_YCORD AS real,@STOP_ARR_HR AS int,
	@STOP_ARR_MIN AS int,@STOP_DEP_HR AS int,@STOP_DEP_MIN AS int,@STOP_DUR_HR AS int,
	@STOP_DUR_MIN AS int,@STOP_PURP AS VARCHAR(75),@tempconstant AS int;
	
	IF EXISTS(SELECT * FROM ReqdTable WHERE BEGWORKSUBTOURNUM > 0)	
		SET @tempconstant = 0			--it will take care of case when at work sub tour exists
	ELSE
		SET @tempconstant = -2			--it will take care of case when at work sub tour does not exist	
	
	SET @STOP_PLACENO =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) PLANO FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS + (@i -1)  FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord, BegTourIndex) 
					AND OUTBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) PLANO FROM ReqdTable 
			WHERE BegTourIndex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS + (@i -1) FROM ReqdTable 
				WHERE PrimDest = 1) AND OUTBOUND_STOPS >= @i)  
						END)
	
	SET @STOP_XCORD =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) XCORD FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS + (@i -1)  FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord, BegTourIndex) 
					AND OUTBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) XCORD FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS + (@i -1) FROM ReqdTable 
				WHERE PrimDest = 1) AND OUTBOUND_STOPS >= @i)
						END)			  
	
	SET @STOP_YCORD =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) YCORD FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS + (@i -1)  FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord, BegTourIndex)
					AND OUTBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) YCORD FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS + (@i -1) FROM ReqdTable 
				WHERE PrimDest = 1) AND OUTBOUND_STOPS >= @i)
						END)		
	
	SET @STOP_ARR_HR =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) ARR_HR FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS + (@i -1) FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord, BegTourIndex) 
					AND OUTBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) ARR_HR FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS  + (@i -1) FROM ReqdTable 
				WHERE PrimDest = 1) AND OUTBOUND_STOPS >= @i)
						END)				   
					
	SET @STOP_ARR_MIN =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) ARR_MIN FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS + (@i -1) FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord, BegTourIndex) 
					AND OUTBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) ARR_MIN FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS  + (@i -1) FROM ReqdTable 
				WHERE PrimDest = 1) AND OUTBOUND_STOPS >= @i)
						END)					  
	
	SET @STOP_DEP_HR =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) DEP_HR FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS + (@i -1) FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord, BegTourIndex) 
					AND OUTBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) DEP_HR FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS  + (@i -1) FROM ReqdTable 
				WHERE PrimDest = 1) AND OUTBOUND_STOPS >= @i)
						END)				   
					
	SET @STOP_DEP_MIN =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) DEP_MIN FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS + (@i -1) FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord, BegTourIndex) 
					AND OUTBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) DEP_MIN FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS  + (@i -1) FROM ReqdTable 
				WHERE PrimDest = 1) AND OUTBOUND_STOPS >= @i)
						END)			
	
	IF @STOP_DEP_MIN < @STOP_ARR_MIN
		BEGIN
				SET @STOP_DUR_HR = (@STOP_DEP_HR - 1) - @STOP_ARR_HR 
				SET @STOP_DUR_MIN = (60 + @STOP_DEP_MIN) - @STOP_ARR_MIN
		END
	ELSE
		BEGIN
				SET @STOP_DUR_HR = @STOP_DEP_HR - @STOP_ARR_HR 
				SET @STOP_DUR_MIN = @STOP_DEP_MIN - @STOP_ARR_MIN
		END					  
						
	SET @STOP_PURP =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) AGGACT FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS + (@i -1) FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord, BegTourIndex)
					AND OUTBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) AGGACT FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex - OUTBOUND_STOPS  + (@i -1) FROM ReqdTable 
				WHERE PrimDest = 1) AND OUTBOUND_STOPS >= @i)
						END)
						
	INSERT @OutStopTable SELECT @STOP_PLACENO, @STOP_XCORD, @STOP_YCORD, @STOP_ARR_HR,
	@STOP_ARR_MIN, @STOP_DEP_HR, @STOP_DEP_MIN, @STOP_DUR_HR, @STOP_DUR_MIN, @STOP_PURP		
	RETURN
END
GO
-------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------
--3)Inbound Stop Fields Function
-------------------------------------------------------------------------------------------------------------------------------------
-- Called by StopActivityFields stored procedure while coding trips AND tours in Part-4
IF OBJECT_ID('dbo.InboundStopFields') IS NOT NULL 
	DROP FUNCTION InboundStopFields;
GO
CREATE FUNCTION dbo.InboundStopFields(@i int)
RETURNS @InStopTable TABLE 
(STOP_PLACENO int,
 STOP_XCORD real,
 STOP_YCORD real,
 STOP_ARR_HR int,
 STOP_ARR_MIN int,
 STOP_DEP_HR int,
 STOP_DEP_MIN int,
 STOP_DUR_HR int,
 STOP_DUR_MIN int,
 STOP_PURP VARCHAR(75))
AS  
BEGIN
--STOP ACTIVITY Fields
	DECLARE @STOP_PLACENO AS int,@STOP_XCORD AS real,@STOP_YCORD AS real,@STOP_ARR_HR AS int,
	@STOP_ARR_MIN AS int,@STOP_DEP_HR AS int,@STOP_DEP_MIN AS int,@STOP_DUR_HR AS int,
	@STOP_DUR_MIN AS int,@STOP_PURP AS VARCHAR(75),@tempconstant AS int;
	
	IF EXISTS(SELECT * FROM ReqdTable WHERE BEGWORKSUBTOURNUM > 0)	
		SET @tempconstant = 0			--it will take care of case when at work sub tour exists
	ELSE
		SET @tempconstant = -2			--it will take care of case when at work sub tour does not exist
	
	SET @STOP_PLACENO =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT TOP(1) PLANO FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord desc, BegTourIndex desc) 
					AND INBOUND_STOPS >= @i)
		ELSE (SELECT TOP(1) PLANO FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
				WHERE PrimDest = 1) AND INBOUND_STOPS >= @i)
						END)
						
	SET @STOP_XCORD =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT TOP(1) XCORD FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord desc, BegTourIndex desc) 
					AND INBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) XCORD FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
				WHERE PrimDest = 1) AND INBOUND_STOPS >= @i)
						END)	  
	
	SET @STOP_YCORD =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) YCORD FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord desc, BegTourIndex desc) 
					AND INBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) YCORD FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
				WHERE PrimDest = 1) AND INBOUND_STOPS >= @i)
						END)
	
	SET @STOP_ARR_HR =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) ARR_HR FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord desc, BegTourIndex desc) 
					AND INBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) ARR_HR FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
				WHERE PrimDest = 1) AND INBOUND_STOPS >= @i)
						END)		   
					
	SET @STOP_ARR_MIN =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) ARR_MIN FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord desc, BegTourIndex desc) 
					AND INBOUND_STOPS >= @i)
			ELSE (SELECT  TOP(1) ARR_MIN FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
					WHERE PrimDest = 1) AND INBOUND_STOPS >= @i)
						END)
						
	SET @STOP_DEP_HR =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) DEP_HR FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord desc, BegTourIndex desc) 
					AND INBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) DEP_HR FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
				WHERE PrimDest = 1) AND INBOUND_STOPS >= @i)			   
						END)
						
	SET @STOP_DEP_MIN =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) DEP_MIN FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord desc, BegTourIndex desc) 
					AND INBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) DEP_MIN FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
				WHERE PrimDest = 1) AND INBOUND_STOPS >= @i)
						END)	
	
	IF @STOP_DEP_MIN < @STOP_ARR_MIN
		BEGIN
				SET @STOP_DUR_HR = (@STOP_DEP_HR - 1) - @STOP_ARR_HR 
				SET @STOP_DUR_MIN = (60 + @STOP_DEP_MIN) - @STOP_ARR_MIN
		END
	ELSE
		BEGIN
				SET @STOP_DUR_HR = @STOP_DEP_HR - @STOP_ARR_HR 
				SET @STOP_DUR_MIN = @STOP_DEP_MIN - @STOP_ARR_MIN
		END					  
	
	SET @STOP_PURP =	(CASE
		WHEN EXISTS(SELECT * FROM ReqdTable WHERE AGGACT = 'Work' AND PrimDest = 1)
			THEN (SELECT  TOP(1) AGGACT FROM ReqdTable 
				WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
										WHERE PrimDest = 1 AND BEGWORKSUBTOURNUM > @tempconstant AND AGGACT = 'Work' ORDER BY WorkTourRecord desc, BegTourIndex desc) 
					AND INBOUND_STOPS >= @i)
		ELSE (SELECT  TOP(1) AGGACT FROM ReqdTable 
			WHERE begtourindex = (SELECT TOP(1) BegTourIndex + @i  FROM ReqdTable 
				WHERE PrimDest = 1) AND INBOUND_STOPS >= @i)
						END)
		
	INSERT @InStopTable SELECT @STOP_PLACENO, @STOP_XCORD, @STOP_YCORD, @STOP_ARR_HR,
	@STOP_ARR_MIN, @STOP_DEP_HR, @STOP_DEP_MIN, @STOP_DUR_HR, @STOP_DUR_MIN, @STOP_PURP
		
	RETURN
END
GO
-------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------
--4)CHARINDEX2 Function
-------------------------------------------------------------------------------------------------------------------------------------
--Finds a substring at nth location in the string 
/*
Example:
SELECT dbo.CHARINDEX2('S', 'PARSONS', 2) returns the location of the second occurrence of 'S' which is 7
*/
IF OBJECT_ID('dbo.CHARINDEX2') IS NOT NULL 
	DROP FUNCTION CHARINDEX2;
GO
CREATE FUNCTION CHARINDEX2
(
@TargetStr VARCHAR(8000), 
@SearchedStr VARCHAR(8000), 
@Occurrence int
)
RETURNS int
AS
BEGIN
	DECLARE @pos int, @counter int, @ret int
	
	SET @pos = CHARINDEX(@TargetStr, @SearchedStr)
	SET @counter = 1
	
	IF @Occurrence = 1 	
		SET @ret = @pos
	ELSE
		BEGIN
			WHILE @counter < @Occurrence
				BEGIN
					SELECT @ret = CHARINDEX(@TargetStr, @SearchedStr, @pos + 1)	
					SET @counter = @counter + 1
					SET @pos = @ret
				END
		END
	RETURN(@ret)
END
GO
-------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------
--5)Joint Person Fields Function
-------------------------------------------------------------------------------------------------------------------------------------
--Called by Part-5 while separating jointflag into separate person fields
IF OBJECT_ID('dbo.JointPersonFields') IS NOT NULL 
	DROP FUNCTION JointPersonFields;
GO
CREATE FUNCTION dbo.JointPersonFields(@i int)
RETURNS @JointPerson TABLE 
(Rec_Num bigint,
Per_ID int,
TOUR_ID int,
TRIP_ID int,
JointFlag VARCHAR(75),
NUMBER_HH int,
Jnt_Per int
 )
AS  
BEGIN
INSERT INTO @JointPerson(Rec_Num,Per_ID,TOUR_ID,TRIP_ID,JointFlag,NUMBER_HH)
	SELECT Rec_Num,Per_ID,TOUR_ID,TRIP_ID,JointFlag,NUMBER_HH FROM RevTripFile
UPDATE @JointPerson SET Jnt_Per = (CASE 
WHEN NUMBER_HH > @i THEN SUBSTRING(JointFlag, dbo.CHARINDEX2('.',JointFlag,@i-1) + 1, dbo.CHARINDEX2('.',JointFlag,@i) - dbo.CHARINDEX2('.',JointFlag,@i-1) - 1)
WHEN NUMBER_HH  = @i THEN SUBSTRING(JointFlag,dbo.CHARINDEX2('.',JointFlag,@i-1) + 1, len(JointFlag) - dbo.CHARINDEX2('.',JointFlag,@i-1))
ELSE NULL 	
		END)
 RETURN
END
GO			
-------------------------------------------------------------------------------------------------------------------------------------

--Procedures
-------------------------------------------------------------------------------------------------------------------------------------
--1)AddColumns Procedure  
-------------------------------------------------------------------------------------------------------------------------------------
--Creates new fields in templace --Used while coding trips AND tours in Part-4
IF OBJECT_ID('dbo.AddColumns') IS NOT NULL 
	DROP PROCEDURE AddColumns;
GO
CREATE PROCEDURE AddColumns 
	AS
	BEGIN
		ALTER TABLE templace ADD HomeIdentifier int, BEGTOURNUM int, ENDTOURNUM int, 
		TourRecord int, BegTourIndex int, BEGWORKSUBTOURNUM int, ENDWORKSUBTOURNUM int, WorkIdentifier int,
		WorkTourRecord int, BegWorkTourIndex int, PrimDest int, ORIG_PLACENO int, DEST_PLACENO int, 
		ORIG_X real, ORIG_Y real, ORIG_TAZ int, DEST_X real, DEST_Y real, DEST_TAZ int, TOURPURP VARCHAR(25), 
		TOURMODE VARCHAR(25), DRIVER int, ANCHOR_DEPART_HOUR int, ANCHOR_DEPART_MIN int, PRIMDEST_ARRIVE_HOUR int,
		PRIMDEST_ARRIVE_MIN int, PRIMDEST_DEPART_HOUR int, PRIMDEST_DEPART_MIN int, ANCHOR_ARRIVE_HOUR int,
		ANCHOR_ARRIVE_MIN int, TOUR_DUR_HR int, TOUR_DUR_MIN int, IS_SUBTOUR int, PARENT_TOUR_ID int,
		PARENT_TOUR_MODE VARCHAR(25), HAS_SUBTOUR int, CHILD_TOUR_ID VARCHAR(25), OUTBOUND_STOPS int, 
		INBOUND_STOPS int, OSTOP_1_PLACENO int, OSTOP_1_X real,OSTOP_1_Y real, OSTOP_1_ARR_HR int,
		OSTOP_1_ARR_MIN int, OSTOP_1_DEP_HR int, OSTOP_1_DEP_MIN int, OSTOP_1_DUR_HR int, OSTOP_1_DUR_MIN int,
		OSTOP_1_PURP VARCHAR(25), OSTOP_2_PLACENO int, OSTOP_2_X real, OSTOP_2_Y real, OSTOP_2_ARR_HR int,
		OSTOP_2_ARR_MIN int, OSTOP_2_DEP_HR int, OSTOP_2_DEP_MIN int, OSTOP_2_DUR_HR int, OSTOP_2_DUR_MIN int, 
		OSTOP_2_PURP VARCHAR(25), OSTOP_3_PLACENO int, OSTOP_3_X real, OSTOP_3_Y real, OSTOP_3_ARR_HR int,
		OSTOP_3_ARR_MIN int, OSTOP_3_DEP_HR int,OSTOP_3_DEP_MIN int, OSTOP_3_DUR_HR int, OSTOP_3_DUR_MIN int,
		OSTOP_3_PURP VARCHAR(25), OSTOP_4_PLACENO int, OSTOP_4_X real, OSTOP_4_Y real,OSTOP_4_ARR_HR int,
		OSTOP_4_ARR_MIN int, OSTOP_4_DEP_HR int, OSTOP_4_DEP_MIN int, OSTOP_4_DUR_HR int, OSTOP_4_DUR_MIN int, 
		OSTOP_4_PURP VARCHAR(25), ISTOP_1_PLACENO int, ISTOP_1_X real, ISTOP_1_Y real, ISTOP_1_ARR_HR int,
		ISTOP_1_ARR_MIN int, ISTOP_1_DEP_HR int,ISTOP_1_DEP_MIN int, ISTOP_1_DUR_HR int, ISTOP_1_DUR_MIN int,
		ISTOP_1_PURP VARCHAR(25), ISTOP_2_PLACENO int, ISTOP_2_X real, ISTOP_2_Y real, ISTOP_2_ARR_HR int,
		ISTOP_2_ARR_MIN int, ISTOP_2_DEP_HR int, ISTOP_2_DEP_MIN int, ISTOP_2_DUR_HR int, ISTOP_2_DUR_MIN int, 
		ISTOP_2_PURP VARCHAR(25), ISTOP_3_PLACENO int, ISTOP_3_X real, ISTOP_3_Y real, ISTOP_3_ARR_HR int,
		ISTOP_3_ARR_MIN int, ISTOP_3_DEP_HR int, ISTOP_3_DEP_MIN int, ISTOP_3_DUR_HR int, ISTOP_3_DUR_MIN int,
		ISTOP_3_PURP VARCHAR(25), ISTOP_4_PLACENO int, ISTOP_4_X real, ISTOP_4_Y real, ISTOP_4_ARR_HR int,
		ISTOP_4_ARR_MIN int, ISTOP_4_DEP_HR int, ISTOP_4_DEP_MIN int, ISTOP_4_DUR_HR int, ISTOP_4_DUR_MIN int,
		ISTOP_4_PURP VARCHAR(25), Trip_ID int,Tour_ID int, ORIG_ARR_HR int, ORIG_ARR_MIN int, ORIG_DEP_HR int,
		ORIG_DEP_MIN int, DEST_ARR_HR int, DEST_ARR_MIN int, DEST_DEP_HR int, DEST_DEP_MIN int, TRIP_DUR_HR int,
		TRIP_DUR_MIN int, ORIG_PURP VARCHAR(25), DEST_PURP VARCHAR(25), SUBTOUR int, IS_INBOUND int,
		AUTO_OCC int, TRIPS_ON_JOURNEY int, TRIPS_ON_TOUR int, ORIG_IS_TOUR_ORIG int, ORIG_IS_TOUR_DEST int,
		DEST_IS_TOUR_DEST int, DEST_IS_TOUR_ORIG int;
	END
	GO	
-------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------
--2)IdentifyTours Procedure
-------------------------------------------------------------------------------------------------------------------------------------
--Identifies tours for each individual --Used while coding trips AND tours in Part-4
IF OBJECT_ID('dbo.IdentifyTours') IS NOT NULL 
	DROP PROCEDURE IdentifyTours;
GO
CREATE PROCEDURE IdentifyTours
	AS 
	BEGIN
		UPDATE templace SET Ro_gen  = NULL;
		UPDATE templace SET Ro_gen = ff.Ro_gen
			FROM (SELECT hhseq,PERNO,PLANO, ROW_NUMBER() OVER (PARTITION BY hhseq,PERNO ORDER BY PLANO) AS Ro_gen FROM templace) AS ff
			WHERE templace.hhseq = ff.hhseq AND templace.PERNO  = ff.PERNO AND templace.PLANO = ff.PLANO
			
		UPDATE templace SET HomeIdentifier = (CASE
												WHEN AGGACT = 'Home' THEN 1
												ELSE 0
											  END)
		
		UPDATE templace SET BEGTOURNUM = tt.begtour 
			FROM (SELECT t1.hhseq,t1.perno,t1.Ro_gen, t1.HomeIdentifier, SUM(t2.HomeIdentifier) AS begtour FROM templace t1
				INNER JOIN  templace t2 ON t1.Ro_gen >= t2.Ro_gen AND t1.hhseq = t2.hhseq AND t1.PERNO = t2.PERNO GROUP BY t1.hhseq,t1.PERNO,t1.Ro_gen,t1.HomeIdentifier) AS tt 
			WHERE templace.hhseq = tt.hhseq AND templace.PERNO = tt.PERNO AND templace.Ro_gen = tt.Ro_gen

		UPDATE templace SET ENDTOURNUM = templace.BEGTOURNUM - tt.Diff 
			FROM templace,(SELECT Cur.hhseq,Cur.PERNO,Cur.Ro_gen, Cur.BEGTOURNUM-Prv.BEGTOURNUM AS Diff 
							FROM templace Cur 
								Left Outer Join templace Prv On Cur.Ro_gen = Prv.Ro_gen + 1 AND Cur.hhseq = Prv.hhseq AND Cur.PERNO = Prv.PERNO) AS tt 
			WHERE templace.hhseq = tt.hhseq AND templace.PERNO = tt.PERNO AND templace.Ro_gen = tt.Ro_gen
		
		UPDATE templace SET ENDTOURNUM = BEGTOURNUM WHERE ENDTOURNUM IS NULL
	
		--Adding extra records for tour end AND tour start activities	
		SELECT * INTO extrarows FROM templace WHERE BEGTOURNUM != ENDTOURNUM
		UPDATE extrarows SET BEGTOURNUM = ENDTOURNUM 
		INSERT INTO templace SELECT * FROM extrarows 		
		--Deleting the extrarows table
		IF OBJECT_ID('dbo.extrarows') IS NOT NULL 
			DROP TABLE extrarows;
		--Creating a unique record number for tours
		UPDATE templace SET TourRecord = tt.Tour 
			FROM (SELECT hhseq,PERNO,Ro_gen,BEGTOURNUM,ROW_NUMBER () OVER (PARTITION BY hhseq,PERNO ORDER BY Ro_gen,BEGTOURNUM) 
					AS Tour FROM templace) AS tt 
			WHERE templace.hhseq = tt.hhseq AND templace.PERNO = tt.PERNO AND templace.Ro_gen = tt.Ro_gen AND tt.BEGTOURNUM = templace.BEGTOURNUM 

		--NA (set to -2) invalid start tours			
		UPDATE templace SET BEGTOURNUM = -2, ENDTOURNUM = -2 FROM (SELECT hhseq,PERNO,MIN(TourRecord) AS TourRecord 
																	FROM templace WHERE BEGTOURNUM = 1 GROUP BY hhseq,PERNO) AS ff 
			WHERE templace.hhseq = ff.hhseq AND templace.PERNO = ff.PERNO AND templace.TourRecord < ff.TourRecord
								
		--if last activity is the only with that code then set to -1
		UPDATE templace SET ENDTOURNUM = -1 FROM templace,
			(SELECT hhseq,PERNO,ROW_NUMBER () OVER (PARTITION BY hhseq,PERNO ORDER BY BEGTOURNUM) AS tournum, COUNT(*) AS freq
				FROM templace  WHERE BEGTOURNUM > 0 GROUP BY hhseq,PERNO,BEGTOURNUM) AS ff
						WHERE ff.tournum = templace.ENDTOURNUM  AND ff.freq <= 2 
							AND templace.hhseq = ff.hhseq AND templace.PERNO = ff.PERNO	
		UPDATE templace SET BEGTOURNUM = -1 FROM templace,
			(SELECT hhseq,PERNO,ROW_NUMBER () OVER (PARTITION BY hhseq,PERNO ORDER BY BEGTOURNUM) AS tournum, COUNT(*) AS freq
				FROM templace  WHERE BEGTOURNUM > 0 GROUP BY hhseq,PERNO,BEGTOURNUM) AS ff
						WHERE ff.tournum = templace.BEGTOURNUM  AND ff.freq <= 2
							AND templace.hhseq = ff.hhseq AND templace.PERNO = ff.PERNO	

		--if last activity is not HOME then set to -1
		UPDATE templace SET BEGTOURNUM = -1
			FROM templace,(SELECT templace.hhseq,templace.PERNO,templace.ENDTOURNUM 
								FROM templace,(SELECT hhseq,PERNO,MAX(TourRecord) AS TourRecord FROM templace GROUP BY hhseq,PERNO) AS ff
								WHERE templace.hhseq = ff.hhseq AND templace.PERNO = ff.PERNO AND templace.TourRecord = ff.TourRecord AND templace.AGGACT != 'Home') AS tt
			WHERE templace.hhseq = tt.hhseq AND templace.PERNO = tt.PERNO  AND templace.BEGTOURNUM = tt.ENDTOURNUM
		UPDATE templace SET ENDTOURNUM = -1
			FROM templace,(SELECT templace.hhseq,templace.PERNO,templace.ENDTOURNUM 
								FROM templace,(SELECT hhseq,PERNO,MAX(TourRecord) AS TourRecord FROM templace GROUP BY hhseq,PERNO) AS ff
								WHERE templace.hhseq = ff.hhseq AND templace.PERNO = ff.PERNO AND templace.TourRecord = ff.TourRecord AND templace.AGGACT != 'Home') AS tt
			WHERE templace.hhseq = tt.hhseq AND templace.PERNO = tt.PERNO  AND templace.ENDTOURNUM = tt.ENDTOURNUM		
	END
	GO

-------------------------------------------------------------------------------------------------------------------------------------
--3)Identify Work SubTour Procedure
-------------------------------------------------------------------------------------------------------------------------------------
--Identifies at-work subtours for each individual --Used while coding trips AND tours in Part-4	
IF OBJECT_ID('dbo.identifyWorkSubTour') IS NOT NULL 
	DROP PROCEDURE identifyWorkSubTour;
GO
CREATE PROCEDURE identifyWorkSubTour
	AS 
	BEGIN
		--Creating a unique record number for Begtours
		UPDATE templace SET BegTourIndex =  templace.TourRecord - tt.TourRecord + 1
			FROM (SELECT hhseq,PERNO,BEGTOURNUM,MIN(TourRecord) AS TourRecord FROM templace WHERE BEGTOURNUM > 0 GROUP BY hhseq,PERNO,BEGTOURNUM) AS tt
			WHERE templace.hhseq = tt.hhseq AND templace.PERNO = tt.PERNO AND templace.BEGTOURNUM = tt.BEGTOURNUM
		UPDATE templace SET BegTourIndex = -1 WHERE BegTourIndex = NULL
		UPDATE templace SET BEGWORKSUBTOURNUM = -1,ENDWORKSUBTOURNUM = -1
													   
		DECLARE @WrkCnt TABLE
		(
		hhseq int,
		PERNO int,
		BEGTOURNUM int,
		Freq int
		)
		INSERT INTO @WrkCnt (hhseq,PERNO,BEGTOURNUM,Freq) SELECT hhseq,PERNO,BEGTOURNUM,COUNT(*) AS Freq FROM templace WHERE AGGACT = 'Work' 
			AND BEGTOURNUM > 0 GROUP BY hhseq,PERNO,BEGTOURNUM														   
		
		--only if more than 2 Work codes
		UPDATE templace SET WorkIdentifier = (CASE 
												  WHEN AGGACT='Work' THEN 1
												  ELSE 0
											  END) FROM @WrkCnt w, templace t
			WHERE w.hhseq = t.hhseq AND w.PERNO = t.PERNO AND w.BEGTOURNUM = t.BEGTOURNUM AND w.Freq > 1
			
		UPDATE templace SET BEGWORKSUBTOURNUM = tt.begwork 
			FROM (SELECT t1.hhseq,t1.PERNO,t1.BEGTOURNUM,t1.BegTourIndex, t1.WorkIdentifier, SUM(t2.WorkIdentifier) AS begwork FROM templace t1
				INNER JOIN  templace t2 ON t1.BegTourIndex >= t2.BegTourIndex AND t1.hhseq = t2.hhseq AND t1.PERNO = t2.PERNO 
				AND t1.BEGTOURNUM = t2.BEGTOURNUM WHERE t1.WorkIdentifier IS NOT NULL GROUP BY t1.hhseq,t1.PERNO,t1.BEGTOURNUM,t1.BegTourIndex,t1.WorkIdentifier) 
				AS tt,templace
			WHERE templace.hhseq = tt.hhseq AND templace.PERNO = tt.PERNO AND templace.BEGTOURNUM = tt.BEGTOURNUM AND templace.BegTourIndex = tt.BegTourIndex
		
		--NA before Work activity
		UPDATE templace SET  BEGWORKSUBTOURNUM = -1 WHERE BEGWORKSUBTOURNUM = 0 
		--NA the last work start codes if there isn't a work end code as well
		UPDATE templace SET BEGWORKSUBTOURNUM = 9999 FROM templace t, @WrkCnt w
			WHERE t.hhseq = w.hhseq AND t.PERNO = w.PERNO AND t.BEGTOURNUM = w.BEGTOURNUM AND t.BEGWORKSUBTOURNUM = w.Freq AND w.Freq > 1 
						
		UPDATE templace SET ENDWORKSUBTOURNUM = templace.BEGWORKSUBTOURNUM - tt.Diff 
			FROM templace,
				(SELECT Cur.hhseq,Cur.PERNO,Cur.BEGTOURNUM,Cur.WorkIdentifier,Cur.BegTourIndex,Cur.BEGWORKSUBTOURNUM-Prv.BEGWORKSUBTOURNUM AS Diff 
						FROM templace Cur 
							Left Outer Join templace Prv On Cur.hhseq = Prv.hhseq AND Cur.PERNO = Prv.PERNO 
																AND Cur.BEGTOURNUM = Prv.BEGTOURNUM AND Cur.BegTourIndex = Prv.BegTourIndex + 1
																WHERE Cur.WorkIdentifier IS NOT NULL) AS tt
			WHERE templace.hhseq = tt.hhseq AND templace.PERNO = tt.PERNO AND templace.BEGTOURNUM = tt.BEGTOURNUM AND templace.BegTourIndex = tt.BegTourIndex

		UPDATE templace SET ENDWORKSUBTOURNUM = BEGWORKSUBTOURNUM WHERE ENDWORKSUBTOURNUM IS NULL	
		--Adding extra records for work sub tour end AND tour start activities
		SELECT * INTO workextrarows FROM templace WHERE templace.BEGWORKSUBTOURNUM != templace.ENDWORKSUBTOURNUM
		UPDATE workextrarows SET BEGWORKSUBTOURNUM = ENDWORKSUBTOURNUM
		INSERT INTO templace SELECT * FROM workextrarows 
		--Deleting the workextrarows table
			IF OBJECT_ID('dbo.workextrarows') IS NOT NULL 
				DROP TABLE workextrarows;

		--Creating a unique record number for tours
		UPDATE templace SET WorkTourRecord = tt.WorkTour 
			FROM (SELECT hhseq,PERNO,BEGTOURNUM,BegTourindex,BEGWORKSUBTOURNUM,ROW_NUMBER () OVER (PARTITION BY hhseq,PERNO,BEGTOURNUM ORDER BY BegTourIndex,BEGWORKSUBTOURNUM) 
					AS WorkTour FROM templace) AS tt 
			WHERE templace.hhseq = tt.hhseq AND templace.PERNO = tt.PERNO AND templace.BEGTOURNUM = tt.BEGTOURNUM  
				AND templace.BegTourindex = tt.BegTourindex AND templace.BEGWORKSUBTOURNUM = tt.BEGWORKSUBTOURNUM AND templace.WorkIdentifier IS NOT NULL
		UPDATE templace SET BEGWORKSUBTOURNUM = -1 WHERE BEGWORKSUBTOURNUM = 9999
		UPDATE templace SET ENDWORKSUBTOURNUM = -1 WHERE ENDWORKSUBTOURNUM = 9999
		
		--NA (set to -2) out invalid start work subtours		
		UPDATE templace SET BEGWORKSUBTOURNUM = -2, ENDWORKSUBTOURNUM = -2 
			WHERE WorkTourRecord < (SELECT MIN(WorkTourRecord) 
										FROM templace WHERE BEGWORKSUBTOURNUM = 1)		
	
		--if last activity is the only with that code then set to -1		
		UPDATE templace SET ENDWORKSUBTOURNUM = -1 FROM templace,
			(SELECT hhseq,PERNO,BEGTOURNUM,ROW_NUMBER () OVER (PARTITION BY hhseq,PERNO,BEGTOURNUM ORDER BY BEGWORKSUBTOURNUM) AS tournum, COUNT(*) AS freq
			FROM templace  WHERE BEGWORKSUBTOURNUM > 0 GROUP BY hhseq,PERNO,BEGTOURNUM,BEGWORKSUBTOURNUM) AS ff
					WHERE ff.tournum = templace.ENDWORKSUBTOURNUM  AND ff.freq <= 2 
						AND templace.hhseq = ff.hhseq AND templace.PERNO = ff.PERNO	AND templace.BEGTOURNUM = ff.BEGTOURNUM
		UPDATE templace SET BEGWORKSUBTOURNUM = -1 FROM templace,
			(SELECT hhseq,PERNO,BEGTOURNUM,ROW_NUMBER () OVER (PARTITION BY hhseq,PERNO,BEGTOURNUM ORDER BY BEGWORKSUBTOURNUM) AS tournum, COUNT(*) AS freq
			FROM templace  WHERE BEGWORKSUBTOURNUM > 0 GROUP BY hhseq,PERNO,BEGTOURNUM,BEGWORKSUBTOURNUM) AS ff
					WHERE ff.tournum = templace.BEGWORKSUBTOURNUM  AND ff.freq <= 2 
						AND templace.hhseq = ff.hhseq AND templace.PERNO = ff.PERNO	AND templace.BEGTOURNUM = ff.BEGTOURNUM		
	END
	GO	
	
-------------------------------------------------------------------------------------------------------------------------------------
--4) AnchorPrimaryDestination Procedure
-------------------------------------------------------------------------------------------------------------------------------------
--Locates primary destination of the tour --Used while coding trips AND tours in Part-4	
IF OBJECT_ID('dbo.AnchorPrimaryDestination') IS NOT NULL 
	DROP PROCEDURE AnchorPrimaryDestination;
GO
CREATE PROCEDURE AnchorPrimaryDestination 
	AS
	BEGIN
		SELECT TourRecord,hhseq,PERNO,PLANO,XCORD,YCORD,ACTDUR,AGGACT INTO temp_y FROM ReqdTable 
			WHERE TourRecord > (SELECT MIN(TourRecord) FROM ReqdTable) 
			AND TourRecord < (SELECT MAX(TourRecord) FROM ReqdTable) ORDER BY hhseq,PERNO,PLANO,TourRecord
		ALTER TABLE temp_y ADD PrimTravTime real, LookupVal real, Score real, distcalc real, PrimDest int;
		DECLARE @AnchorLong AS real;
		DECLARE @AnchorLat AS real;
		DECLARE @EarthRadiusInMiles  AS real;
		DECLARE @PI  AS real;
		SET @EarthRadiusInMiles = 3963.1676
		SET @PI = PI();
		
		SET @AnchorLong = (SELECT TOP(1) XCORD * (@PI/180) FROM ReqdTable)
		SET @AnchorLat = (SELECT TOP(1) YCORD * (@PI/180) FROM ReqdTable)
		
		UPDATE temp_y SET distcalc = Cos(@AnchorLat) * Cos(@AnchorLong) * Cos(YCORD * (@PI/180)) * Cos(XCORD * (@PI/180)) + 
										Cos(@AnchorLat) * Sin(@AnchorLong) * Cos(YCORD * (@PI/180)) * Sin(XCORD * (@PI/180)) + 
										Sin(@AnchorLat) * Sin(YCORD * (@PI/180))
		UPDATE temp_y SET distcalc = (CASE
										WHEN distcalc > 1.0 THEN 1
										WHEN distcalc < -1.0 THEN -1
										ELSE distcalc
									  END)
								 --Converting the obtained distance into free flow time using speed = 1/2 miles per min
		UPDATE temp_y SET PrimTravTime =  ROUND((Acos(distcalc) * @EarthRadiusInMiles)/(0.5),2) FROM temp_y				
		
		UPDATE temp_y SET LookupVal = 2 * PrimTravTime + ACTDUR FROM temp_y
		UPDATE temp_y SET Score =
			(CASE
				WHEN LookupVal BETWEEN 0 AND 60
					THEN d0 + ((d60-d0) * (LookupVal - 0)/60)
				WHEN LookupVal BETWEEN 60 AND 120
					THEN d60 + ((d120-d60) * (LookupVal - 60)/60)		
				WHEN LookupVal BETWEEN 120 AND 180
					THEN d120 + ((d180-d120) * (LookupVal - 120)/60)
				WHEN LookupVal BETWEEN 180 AND 240
					THEN d180 + ((d240-d180) * (LookupVal - 180)/60)
				WHEN LookupVal BETWEEN 240 AND 300
					THEN d240 + ((d300-d240) * (LookupVal - 240)/60)
				WHEN LookupVal BETWEEN 300 AND 360
					THEN d300 + ((d360-d300) * (LookupVal - 300)/60)
				WHEN LookupVal BETWEEN 360 AND 420
					THEN d360 + ((d420-d360) * (LookupVal - 360)/60)
				WHEN LookupVal BETWEEN 420 AND 480
					THEN d420 + ((d480-d420) * (LookupVal - 420)/60)
				WHEN lookupVal > 480
					THEN d480
				END)		
		FROM dbo.actpriority a, temp_y 
		WHERE a.purp_label = dbo.temp_y.AGGACT
		
		UPDATE temp_y SET Score = 99 WHERE Score IS NULL
		
		UPDATE temp_y SET PrimDest = 1 WHERE Score = (SELECT MIN(Score) FROM temp_y) 
		UPDATE temp_y SET PrimDest = 0 WHERE PrimDest IS NULL
		UPDATE ReqdTable SET PrimDest = temp_y.PrimDest FROM temp_y WHERE temp_y.TourRecord = ReqdTable.TourRecord 
		
		IF (SELECT COUNT(primdest) FROM ReqdTable WHERE AGGACT != 'Work' AND primdest = 1) > 1
			BEGIN
				UPDATE ReqdTable SET primdest = 0 
					WHERE TourRecord ! = (SELECT TOP(1) TourRecord FROM ReqdTable 
												WHERE primdest = 1 ORDER BY WorkTourRecord,TourRecord)
			END
		IF ((SELECT TOP(1) PrimDest FROM ReqdTable ORDER BY TourRecord) IS NOT NULL 
				OR (SELECT TOP(1) PrimDest FROM ReqdTable ORDER BY TourRecord desc) IS NOT NULL) 
			BEGIN
				UPDATE ReqdTable SET PrimDest = NULL 
					WHERE TourRecord = (SELECT TOP(1) TourRecord FROM ReqdTable ORDER BY TourRecord)
						AND TourRecord = (SELECT TOP(1) TourRecord FROM ReqdTable ORDER BY TourRecord desc)
			END
			
		IF OBJECT_ID('dbo.temp_y') IS NOT NULL 
			DROP TABLE temp_y;	
			
		IF EXISTS(SELECT * FROM ReqdTable WHERE PrimDest = 1 AND AGGACT= 'Work')
			BEGIN
				UPDATE ReqdTable SET PrimDest = 1 
					WHERE AGGACT = 'Work' AND PTAZ = (SELECT TOP(1) PTAZ FROM ReqdTable WHERE PrimDest = 1) 
						AND XCORD = (SELECT TOP(1) XCORD FROM ReqdTable WHERE PrimDest = 1 ORDER BY TourRecord) 
						AND YCORD = (SELECT TOP(1) YCORD FROM ReqdTable WHERE PrimDest = 1 ORDER BY TourRecord) 
			END	
	END
GO
-------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------
--5)Total Trip File Creation Procedure 
-------------------------------------------------------------------------------------------------------------------------------------
--Calculate several fields for trip file creation --Used while coding trips AND tours in Part-4	
IF OBJECT_ID('dbo.TotalTripFileCreation') IS NOT NULL 
	DROP PROCEDURE TotalTripFileCreation
GO
CREATE PROCEDURE TotalTripFileCreation 
	AS
	BEGIN
		DECLARE @tempconstant AS int;
			IF EXISTS(SELECT * FROM ReqdTable WHERE BEGWORKSUBTOURNUM > 0)	
				SET @tempconstant = 0			--it will take care of case when at work sub tour exists
			ELSE
				SET @tempconstant = -2			--it will take care of case when at work sub tour does not exist
		
		IF (SELECT TOP(1) AGGACT FROM ReqdTable ORDER BY BegTourIndex) = 'Home'
			BEGIN
				IF EXISTS(SELECT * FROM ReqdTable WHERE BEGWORKSUBTOURNUM > 0)
					BEGIN
						UPDATE ReqdTable SET Trip_ID = ff.trip 
							FROM (SELECT ROW_NUMBER() OVER (ORDER BY BegTourIndex) AS trip,* FROM ReqdTable
									WHERE BEGWORKSUBTOURNUM < 0 AND BegTourIndex > 1 
										AND (BegTourIndex <= (SELECT TOP (1) BegTourIndex FROM ReqdTable 
																WHERE BEGWORKSUBTOURNUM > @tempconstant  AND BegTourIndex > 1 ORDER BY BegTourIndex)
												OR BegTourIndex > (SELECT TOP (1) BegTourIndex FROM ReqdTable 
																		WHERE BEGWORKSUBTOURNUM > @tempconstant 
																			AND BegTourIndex < (SELECT MAX(BegTourIndex) FROM ReqdTable) 
																			ORDER BY ReqdTable.BegTourIndex desc))) AS ff
							WHERE ReqdTable.BegTourIndex = ff.BegTourIndex AND ReqdTable.BEGWORKSUBTOURNUM < 0
					END			
				ELSE
					BEGIN
						UPDATE ReqdTable SET Trip_ID = ff.trip 
								FROM (SELECT ROW_NUMBER() OVER (ORDER BY BegTourIndex) AS trip,* FROM ReqdTable
										WHERE BEGWORKSUBTOURNUM < 0 AND BegTourIndex > 1) AS ff
								WHERE ReqdTable.BegTourIndex = ff.BegTourIndex AND ReqdTable.BEGWORKSUBTOURNUM < 0				
					END
			END
		ELSE
			BEGIN
				UPDATE ReqdTable SET Trip_ID = ff.trip
					FROM (SELECT ROW_NUMBER() OVER (ORDER BY WorkTourRecord) AS trip,* FROM ReqdTable
							WHERE ReqdTable.WorkTourRecord > (SELECT TOP(1) WorkTourRecord FROM ReqdTable ORDER BY WorkTourRecord)) AS ff
					WHERE Reqdtable.WorkTourRecord = ff.WorkTourRecord
			END			
				
		UPDATE ReqdTable SET ORIG_ARR_HR = ff.ARR_HR, ORIG_ARR_MIN = ff.ARR_MIN, ORIG_DEP_HR = ff.DEP_HR, ORIG_DEP_MIN = ff.DEP_MIN, ORIG_PURP = ff.AGGACT
			FROM (SELECT * FROM ReqdTable) AS ff
			WHERE ReqdTable.hhseq = ff.hhseq AND ReqdTable.PERNO = ff.PERNO AND ReqdTable.BegTourIndex = ff.BegTourIndex + 1
		
		UPDATE ReqdTable SET DEST_ARR_HR = ff.ARR_HR, DEST_ARR_MIN = ff.ARR_MIN, DEST_DEP_HR = ff.DEP_HR, DEST_DEP_MIN = ff.DEP_MIN, DEST_PURP = ff.AGGACT
			FROM (SELECT * FROM ReqdTable) AS ff
			WHERE ReqdTable.hhseq = ff.hhseq AND ReqdTable.PERNO = ff.PERNO AND ReqdTable.BegTourIndex = ff.BegTourIndex
		
		
		IF (SELECT TOP(1) AGGACT FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex) = 'Home' AND EXISTS(SELECT * FROM ReqdTable WHERE BEGWORKSUBTOURNUM > 0)
			BEGIN
				UPDATE ReqdTable SET DEST_DEP_HR = (SELECT TOP(1) DEP_HR FROM ReqdTable WHERE BEGWORKSUBTOURNUM > 0 ORDER BY WorkTourRecord desc,BegTourIndex desc) 
					WHERE AGGACT = 'Work' AND TRIP_ID IS NOT NULL
				UPDATE ReqdTable SET DEST_DEP_MIN = (SELECT TOP(1) DEP_MIN FROM ReqdTable WHERE BEGWORKSUBTOURNUM > 0 ORDER BY WorkTourRecord desc,BegTourIndex desc)
					WHERE AGGACT = 'Work' AND TRIP_ID IS NOT NULL
				UPDATE ReqdTable SET ORIG_ARR_HR = (SELECT TOP(1) ARR_HR FROM ReqdTable WHERE BEGWORKSUBTOURNUM > 0 ORDER BY WorkTourRecord,BegTourIndex)
					WHERE ORIG_PURP = 'Work'
				UPDATE ReqdTable SET ORIG_ARR_MIN = (SELECT TOP(1) ARR_MIN FROM ReqdTable WHERE BEGWORKSUBTOURNUM > 0 ORDER BY WorkTourRecord,BegTourIndex) 
					WHERE ORIG_PURP = 'Work' 
			END
		
		
		UPDATE ReqdTable SET TRIP_DUR_HR = (CASE
												WHEN ORIG_DEP_MIN > DEST_ARR_MIN THEN (DEST_ARR_HR - 1) - ORIG_DEP_HR
												ELSE DEST_ARR_HR - ORIG_DEP_HR
											END)
		UPDATE ReqdTable SET TRIP_DUR_MIN = (CASE
												WHEN ORIG_DEP_MIN > DEST_ARR_MIN THEN (DEST_ARR_MIN + 60) - ORIG_DEP_MIN
												ELSE DEST_ARR_MIN - ORIG_DEP_MIN
											END)									
		
		UPDATE ReqdTable SET DRIVER = (CASE
											WHEN MODENAME = 'DRIVER' THEN 1
											ELSE 0
										END)
		UPDATE ReqdTable SET SUBTOUR = (CASE
											WHEN (SELECT TOP(1) AGGACT FROM Reqdtable ORDER BY WorkTourRecord desc,BegTourIndex) = 'Work' THEN 1
											ELSE 0
										END)	
		UPDATE ReqdTable SET IS_INBOUND = (CASE
											WHEN BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable WHERE PrimDest = 1 ORDER BY WorkTourRecord desc,BegTourIndex desc) THEN 1
											ELSE 0
										END)	
		UPDATE ReqdTable SET TRIPS_ON_JOURNEY = (CASE
											WHEN IS_INBOUND = 1 THEN INBOUND_STOPS + 1
											ELSE OUTBOUND_STOPS + 1
										END)								
		UPDATE ReqdTable SET TRIPS_ON_TOUR = (SELECT TOP(1) INBOUND_STOPS + OUTBOUND_STOPS + 2 FROM ReqdTable)
		
		UPDATE ReqdTable SET ORIG_IS_TOUR_ORIG = (CASE
													WHEN ORIGIN_LONG = ORIG_X AND ORIGIN_LAT = ORIG_Y THEN 1
													ELSE 0
												  END)
		UPDATE ReqdTable SET ORIG_IS_TOUR_DEST = (CASE
													WHEN ORIGIN_LONG = DEST_X AND ORIGIN_LAT = DEST_Y THEN 1
													ELSE 0
												  END)										  
		UPDATE ReqdTable SET DEST_IS_TOUR_DEST = (CASE
													WHEN DEST_LONG = DEST_X AND DEST_LAT = DEST_Y THEN 1
													ELSE 0
												  END)				
		UPDATE ReqdTable SET DEST_IS_TOUR_ORIG = (CASE
													WHEN DEST_LONG = ORIG_X AND DEST_LAT = ORIG_Y THEN 1
													ELSE 0
												  END)		
		UPDATE ReqdTable SET AUTO_OCC = (CASE
													WHEN MODECODE BETWEEN 1 AND 6 THEN TOTTR
													ELSE 0
												  END)										
	END
	GO	
-------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------
--6)Total Tour File Creation Procedure 
-------------------------------------------------------------------------------------------------------------------------------------
--Calculate several fields for tour file creation --Used while coding trips AND tours in Part-4		
IF OBJECT_ID('dbo.TotalTourFileCreation') IS NOT NULL 
	DROP PROCEDURE TotalTourFileCreation
GO
CREATE PROCEDURE TotalTourFileCreation
	AS
	BEGIN
		UPDATE ReqdTable SET ORIG_PLACENO = (SELECT TOP (1) PLANO FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET DEST_PLACENO = (SELECT TOP(1) PLANO FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET ORIG_X = (SELECT TOP (1) XCORD FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET ORIG_Y = (SELECT TOP (1) YCORD FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET ORIG_TAZ = (SELECT TOP (1) PTAZ FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET DEST_X = (SELECT TOP(1) XCORD FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET DEST_Y = (SELECT TOP(1) YCORD FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET DEST_TAZ = (SELECT TOP(1) PTAZ FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET ANCHOR_DEPART_HOUR = (SELECT TOP (1) DEP_HR FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET ANCHOR_DEPART_MIN = (SELECT TOP (1) DEP_MIN FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET PRIMDEST_ARRIVE_HOUR = (SELECT TOP(1) ARR_HR FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET PRIMDEST_ARRIVE_MIN = (SELECT TOP(1) ARR_MIN FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord,BegTourIndex)
		UPDATE ReqdTable SET PRIMDEST_DEPART_HOUR = (SELECT TOP(1) DEP_HR FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord desc,BegTourIndex desc)
		UPDATE ReqdTable SET PRIMDEST_DEPART_MIN = (SELECT TOP(1) DEP_MIN FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord desc,BegTourIndex desc)
		UPDATE ReqdTable SET ANCHOR_ARRIVE_HOUR = (SELECT TOP (1) ARR_HR FROM ReqdTable ORDER BY PLANO desc)
		UPDATE ReqdTable SET ANCHOR_ARRIVE_MIN = (SELECT TOP (1) ARR_MIN FROM ReqdTable ORDER BY PLANO desc)
		UPDATE ReqdTable SET TOURPURP = (SELECT TOP(1) AGGACT FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord,BegTourIndex)

		UPDATE ReqdTable SET IS_SUBTOUR =	(CASE
												WHEN (SELECT TOP(1) AGGACT FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) = 'Home' THEN 0
												WHEN (SELECT TOP(1) AGGACT FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) = 'Work' THEN 1
											END) 
		UPDATE ReqdTable SET PARENT_TOUR_ID =	(CASE
													WHEN (SELECT TOP(1) IS_SUBTOUR FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) = 0 THEN 0
													WHEN (SELECT TOP(1) IS_SUBTOUR FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) !=0 THEN BEGTOURNUM
												END) 
		 					
		UPDATE ReqdTable SET HAS_SUBTOUR =	(CASE
												WHEN (SELECT TOP(1) AGGACT FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) = 'Home'  
													AND EXISTS(SELECT * FROM Reqdtable WHERE BEGWORKSUBTOURNUM > 0) THEN 1
												ELSE 0
											END) 
		
		UPDATE ReqdTable SET CHILD_TOUR_ID = '0'
		IF (SELECT TOP(1) AGGACT FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) = 'Home' AND EXISTS(SELECT * FROM ReqdTable WHERE BEGWORKSUBTOURNUM > 0)
			BEGIN
				DECLARE @child AS VARCHAR(100); 
				SELECT @child =  COALESCE(@child + ',', '') + CAST(ff. ChildID AS VARCHAR(50)) 
					FROM (SELECT DISTINCT BEGWORKSUBTOURNUM + TOUR_ID AS ChildID FROM ReqdTable WHERE BEGWORKSUBTOURNUM > 0) AS ff
				UPDATE ReqdTable SET CHILD_TOUR_ID = @child 
				SET @child = NULL
			END

		UPDATE ReqdTable SET PARENT_TOUR_MODE =	(CASE
							WHEN (SELECT TOP(1) IS_SUBTOUR FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) = 0 THEN '0'
							WHEN (SELECT TOP(1) IS_SUBTOUR FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) !=0 THEN (SELECT TOP(1) TotalTourfile.TOURMODE FROM Reqdtable,TotalTourfile
																															WHERE Reqdtable.SAMPN = TotalTourfile.HH_ID 
																																AND Reqdtable.PERNO = TotalTourfile.PER_ID
																																AND Reqdtable.BEGTOURNUM = TotalTourfile.TOUR_ID)
												END) 												
		
		DECLARE @tempconstant AS int,	 @tempconstant2 AS int;
		IF (SELECT TOP(1) AGGACT FROM ReqdTable ORDER BY WorkTourRecord,BEGTOURINDEX) = 'Home'	
			BEGIN
				SET @tempconstant = 0			--it will take care of case when at work sub tour exists
				IF(SELECT COUNT(*) FROM ReqdTable WHERE AGGACT = 'Work') > 1
					SET @tempconstant2 = (SELECT TOP(1) BEGTOURINDEX FROM ReqdTable 
											WHERE BEGWORKSUBTOURNUM < 0 AND AGGACT = 'Work' ORDER BY WorkTourRecord desc,BEGTOURINDEX desc)
				ELSE 
					SET @tempconstant2 = 0
			END
		ELSE IF(SELECT TOP(1) AGGACT FROM ReqdTable ORDER BY WorkTourRecord,BEGTOURINDEX) = 'Work'
			BEGIN
				SET @tempconstant = (SELECT TOP(1) BEGWORKSUBTOURNUM FROM ReqdTable) + 1	--it will take care of case when at work sub tour does not exist
				SET @tempconstant2 = 0
			END
		
		UPDATE ReqdTable SET TOURMODE = (CASE
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 14 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'PNR-RAIL'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 13 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'PNR-EXPRESSBUS'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 12 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'PNR-LOCALBUS'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 17 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'KNR-RAIL'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 16 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'KNR-EXPRESSBUS'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 15 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'KNR-LOCALBUS'											
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 18 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'SCHOOLBUS'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 8 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'BIKE/MOPED'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 19 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'TAXI'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 6 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'SHARED-3+(PAY)'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 5 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'SHARED-3+(FREE)'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 4 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'SHARED-2(PAY)'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 3 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'SHARED-2(FREE)'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 2 AND BEGWORKSUBTOURNUM < @tempconstant
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'AUTO SOV(PAY)'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 1 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'AUTO SOV(FREE)'											
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 11 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'WALK-RAIL'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 10 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'WALK-EXPRESSBUS'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 9 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'WALK-LOCALBUS'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE = 7 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'WALK'
				WHEN EXISTS(SELECT * FROM ReqdTable WHERE MODECODE =20 AND BEGWORKSUBTOURNUM < @tempconstant 
					AND BegTourIndex > (SELECT TOP(1) BegTourIndex FROM Reqdtable ORDER BY WorkTourRecord,BegTourIndex) AND BegTourIndex != @tempconstant2) THEN 'OTHER'
										END)
						
		IF ((SELECT TOP(1) ANCHOR_ARRIVE_MIN FROM ReqdTable) < (SELECT TOP(1) ANCHOR_DEPART_MIN FROM ReqdTable))
			BEGIN
				UPDATE ReqdTable SET TOUR_DUR_HR = (SELECT TOP (1)(ANCHOR_ARRIVE_HOUR - 1) - ANCHOR_DEPART_HOUR FROM ReqdTable)
				UPDATE ReqdTable SET TOUR_DUR_MIN = (SELECT TOP(1) (60 + ANCHOR_ARRIVE_MIN) - ANCHOR_DEPART_MIN FROM ReqdTable)
			END
		ELSE
			BEGIN
				UPDATE ReqdTable SET TOUR_DUR_HR = (SELECT  TOP(1) ANCHOR_ARRIVE_HOUR - ANCHOR_DEPART_HOUR FROM ReqdTable)
				UPDATE ReqdTable SET TOUR_DUR_MIN = (SELECT  TOP(1) ANCHOR_ARRIVE_MIN - ANCHOR_DEPART_MIN FROM ReqdTable)
			END		
		
	END
	GO
-------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------
--7)Stop Activity Fields Procedure 
-------------------------------------------------------------------------------------------------------------------------------------
--Calculate several fields related to inbound AND outbound stops --Used while coding trips AND tours in Part-4			
IF OBJECT_ID('dbo.StopActivityFields') IS NOT NULL 
	DROP PROCEDURE StopActivityFields;
GO
CREATE PROCEDURE StopActivityFields 
	AS
	BEGIN	
		--Number of OutBound AND Inbound Stops
		UPDATE ReqdTable SET OUTBOUND_STOPS	= (CASE 
			WHEN (SELECT TOP(1) AGGACT FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex) = 'Home'
				THEN (SELECT COUNT(DISTINCT(BegTourIndex))*1.0 FROM ReqdTable 
					WHERE BEGWORKSUBTOURNUM < 0 AND BegTourIndex != (SELECT TOP(1) BegTourIndex FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex) AND primdest = 0 
						AND BegTourIndex < (SELECT TOP (1) BegTourIndex FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord,BegTourIndex))
			WHEN (SELECT TOP(1) AGGACT FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex) = 'Work'
				THEN (SELECT COUNT(DISTINCT(BegTourIndex))*1.0 FROM ReqdTable 
					WHERE BegTourIndex != (SELECT TOP(1) BegTourIndex FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex) AND primdest = 0 
						AND BegTourIndex < (SELECT TOP (1) BegTourIndex FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord,BegTourIndex))
											   END)
		UPDATE ReqdTable SET INBOUND_STOPS	= (CASE 
			WHEN (SELECT TOP(1) AGGACT FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex) = 'Home'
				THEN (SELECT COUNT(DISTINCT(BegTourIndex))*1.0 FROM ReqdTable 
					WHERE BEGWORKSUBTOURNUM < 0 AND primdest = 0 
						AND BegTourIndex > (SELECT TOP (1) BegTourIndex FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord desc,BegTourIndex desc))
			WHEN (SELECT TOP(1) AGGACT FROM ReqdTable ORDER BY WorkTourRecord,BegTourIndex) = 'Work'
				THEN (SELECT COUNT(DISTINCT(BegTourIndex))*1.0 FROM ReqdTable 
					WHERE primdest = 0 
						AND BegTourIndex > (SELECT TOP (1) BegTourIndex FROM ReqdTable WHERE PrimDest = 1 ORDER BY WorkTourRecord desc,BegTourIndex desc))
											  END)
	--OutBound Stop Fields										
		--First OutBound Stop fields
		UPDATE ReqdTable SET OSTOP_1_PLACENO = (SELECT STOP_PLACENO FROM  OutboundStopFields(1))
		UPDATE ReqdTable SET OSTOP_1_X = (SELECT STOP_XCORD FROM  OutboundStopFields(1))
		UPDATE ReqdTable SET OSTOP_1_Y = (SELECT STOP_YCORD FROM  OutboundStopFields(1))
		UPDATE ReqdTable SET OSTOP_1_ARR_HR = (SELECT STOP_ARR_HR FROM  OutboundStopFields(1))
		UPDATE ReqdTable SET OSTOP_1_ARR_MIN = (SELECT STOP_ARR_MIN FROM  OutboundStopFields(1))
		UPDATE ReqdTable SET OSTOP_1_DEP_HR = (SELECT STOP_DEP_HR FROM  OutboundStopFields(1))
		UPDATE ReqdTable SET OSTOP_1_DEP_MIN = (SELECT STOP_DEP_MIN FROM  OutboundStopFields(1))
		UPDATE ReqdTable SET OSTOP_1_DUR_HR = (SELECT STOP_DUR_HR FROM  OutboundStopFields(1))
		UPDATE ReqdTable SET OSTOP_1_DUR_MIN = (SELECT STOP_DUR_MIN FROM  OutboundStopFields(1))
		UPDATE ReqdTable SET OSTOP_1_PURP = (SELECT STOP_PURP FROM  OutboundStopFields(1))
		
		--Second OutBound Stop fields
		UPDATE ReqdTable SET OSTOP_2_PLACENO = (SELECT STOP_PLACENO FROM  OutboundStopFields(2))
		UPDATE ReqdTable SET OSTOP_2_X = (SELECT STOP_XCORD FROM  OutboundStopFields(2))
		UPDATE ReqdTable SET OSTOP_2_Y = (SELECT STOP_YCORD FROM  OutboundStopFields(2))
		UPDATE ReqdTable SET OSTOP_2_ARR_HR = (SELECT STOP_ARR_HR FROM  OutboundStopFields(2))
		UPDATE ReqdTable SET OSTOP_2_ARR_MIN = (SELECT STOP_ARR_MIN FROM  OutboundStopFields(2))
		UPDATE ReqdTable SET OSTOP_2_DEP_HR = (SELECT STOP_DEP_HR FROM  OutboundStopFields(2))
		UPDATE ReqdTable SET OSTOP_2_DEP_MIN = (SELECT STOP_DEP_MIN FROM  OutboundStopFields(2))
		UPDATE ReqdTable SET OSTOP_2_DUR_HR = (SELECT STOP_DUR_HR FROM  OutboundStopFields(2))
		UPDATE ReqdTable SET OSTOP_2_DUR_MIN = (SELECT STOP_DUR_MIN FROM  OutboundStopFields(2))
		UPDATE ReqdTable SET OSTOP_2_PURP = (SELECT STOP_PURP FROM  OutboundStopFields(2))
		
		--Third OutBound Stop fields
		UPDATE ReqdTable SET OSTOP_3_PLACENO = (SELECT STOP_PLACENO FROM  OutboundStopFields(3))
		UPDATE ReqdTable SET OSTOP_3_X = (SELECT STOP_XCORD FROM  OutboundStopFields(3))
		UPDATE ReqdTable SET OSTOP_3_Y = (SELECT STOP_YCORD FROM  OutboundStopFields(3))
		UPDATE ReqdTable SET OSTOP_3_ARR_HR = (SELECT STOP_ARR_HR FROM  OutboundStopFields(3))
		UPDATE ReqdTable SET OSTOP_3_ARR_MIN = (SELECT STOP_ARR_MIN FROM  OutboundStopFields(3))
		UPDATE ReqdTable SET OSTOP_3_DEP_HR = (SELECT STOP_DEP_HR FROM  OutboundStopFields(3))
		UPDATE ReqdTable SET OSTOP_3_DEP_MIN = (SELECT STOP_DEP_MIN FROM  OutboundStopFields(3))
		UPDATE ReqdTable SET OSTOP_3_DUR_HR = (SELECT STOP_DUR_HR FROM  OutboundStopFields(3))
		UPDATE ReqdTable SET OSTOP_3_DUR_MIN = (SELECT STOP_DUR_MIN FROM  OutboundStopFields(3))
		UPDATE ReqdTable SET OSTOP_3_PURP = (SELECT STOP_PURP FROM  OutboundStopFields(3))
		
		--Forth OutBound Stop fields
		UPDATE ReqdTable SET OSTOP_4_PLACENO = (SELECT STOP_PLACENO FROM  OutboundStopFields(4))
		UPDATE ReqdTable SET OSTOP_4_X = (SELECT STOP_XCORD FROM  OutboundStopFields(4))
		UPDATE ReqdTable SET OSTOP_4_Y = (SELECT STOP_YCORD FROM  OutboundStopFields(4))
		UPDATE ReqdTable SET OSTOP_4_ARR_HR = (SELECT STOP_ARR_HR FROM  OutboundStopFields(4))
		UPDATE ReqdTable SET OSTOP_4_ARR_MIN = (SELECT STOP_ARR_MIN FROM  OutboundStopFields(4))
		UPDATE ReqdTable SET OSTOP_4_DEP_HR = (SELECT STOP_DEP_HR FROM  OutboundStopFields(4))
		UPDATE ReqdTable SET OSTOP_4_DEP_MIN = (SELECT STOP_DEP_MIN FROM  OutboundStopFields(4))
		UPDATE ReqdTable SET OSTOP_4_DUR_HR = (SELECT STOP_DUR_HR FROM  OutboundStopFields(4))
		UPDATE ReqdTable SET OSTOP_4_DUR_MIN = (SELECT STOP_DUR_MIN FROM  OutboundStopFields(4))
		UPDATE ReqdTable SET OSTOP_4_PURP = (SELECT STOP_PURP FROM  OutboundStopFields(4))
		
	--InBound Stop Fields		
		--First InBound Stop fields
		UPDATE ReqdTable SET ISTOP_1_PLACENO = (SELECT STOP_PLACENO FROM  InboundStopFields(1))
		UPDATE ReqdTable SET ISTOP_1_X = (SELECT STOP_XCORD FROM  InboundStopFields(1))
		UPDATE ReqdTable SET ISTOP_1_Y = (SELECT STOP_YCORD FROM  InboundStopFields(1))
		UPDATE ReqdTable SET ISTOP_1_ARR_HR = (SELECT STOP_ARR_HR FROM  InboundStopFields(1))
		UPDATE ReqdTable SET ISTOP_1_ARR_MIN = (SELECT STOP_ARR_MIN FROM  InboundStopFields(1))
		UPDATE ReqdTable SET ISTOP_1_DEP_HR = (SELECT STOP_DEP_HR FROM  InboundStopFields(1))
		UPDATE ReqdTable SET ISTOP_1_DEP_MIN = (SELECT STOP_DEP_MIN FROM  InboundStopFields(1))
		UPDATE ReqdTable SET ISTOP_1_DUR_HR = (SELECT STOP_DUR_HR FROM  InboundStopFields(1))
		UPDATE ReqdTable SET ISTOP_1_DUR_MIN = (SELECT STOP_DUR_MIN FROM  InboundStopFields(1))
		UPDATE ReqdTable SET ISTOP_1_PURP = (SELECT STOP_PURP FROM  InboundStopFields(1))
		
		--Second InBound Stop fields
		UPDATE ReqdTable SET ISTOP_2_PLACENO = (SELECT STOP_PLACENO FROM  InboundStopFields(2))
		UPDATE ReqdTable SET ISTOP_2_X = (SELECT STOP_XCORD FROM  InboundStopFields(2))
		UPDATE ReqdTable SET ISTOP_2_Y = (SELECT STOP_YCORD FROM  InboundStopFields(2))
		UPDATE ReqdTable SET ISTOP_2_ARR_HR = (SELECT STOP_ARR_HR FROM  InboundStopFields(2))
		UPDATE ReqdTable SET ISTOP_2_ARR_MIN = (SELECT STOP_ARR_MIN FROM  InboundStopFields(2))
		UPDATE ReqdTable SET ISTOP_2_DEP_HR = (SELECT STOP_DEP_HR FROM  InboundStopFields(2))
		UPDATE ReqdTable SET ISTOP_2_DEP_MIN = (SELECT STOP_DEP_MIN FROM  InboundStopFields(2))
		UPDATE ReqdTable SET ISTOP_2_DUR_HR = (SELECT STOP_DUR_HR FROM  InboundStopFields(2))
		UPDATE ReqdTable SET ISTOP_2_DUR_MIN = (SELECT STOP_DUR_MIN FROM  InboundStopFields(2))
		UPDATE ReqdTable SET ISTOP_2_PURP = (SELECT STOP_PURP FROM  InboundStopFields(2))
		
		--Third InBound Stop fields
		UPDATE ReqdTable SET ISTOP_3_PLACENO = (SELECT STOP_PLACENO FROM  InboundStopFields(3))
		UPDATE ReqdTable SET ISTOP_3_X = (SELECT STOP_XCORD FROM  InboundStopFields(3))
		UPDATE ReqdTable SET ISTOP_3_Y = (SELECT STOP_YCORD FROM  InboundStopFields(3))
		UPDATE ReqdTable SET ISTOP_3_ARR_HR = (SELECT STOP_ARR_HR FROM  InboundStopFields(3))
		UPDATE ReqdTable SET ISTOP_3_ARR_MIN = (SELECT STOP_ARR_MIN FROM  InboundStopFields(3))
		UPDATE ReqdTable SET ISTOP_3_DEP_HR = (SELECT STOP_DEP_HR FROM  InboundStopFields(3))
		UPDATE ReqdTable SET ISTOP_3_DEP_MIN = (SELECT STOP_DEP_MIN FROM  InboundStopFields(3))
		UPDATE ReqdTable SET ISTOP_3_DUR_HR = (SELECT STOP_DUR_HR FROM  InboundStopFields(3))
		UPDATE ReqdTable SET ISTOP_3_DUR_MIN = (SELECT STOP_DUR_MIN FROM  InboundStopFields(3))
		UPDATE ReqdTable SET ISTOP_3_PURP = (SELECT STOP_PURP FROM  InboundStopFields(3))
		
		--Forth InBound Stop fields
		UPDATE ReqdTable SET ISTOP_4_PLACENO = (SELECT STOP_PLACENO FROM  InboundStopFields(4))
		UPDATE ReqdTable SET ISTOP_4_X = (SELECT STOP_XCORD FROM  InboundStopFields(4))
		UPDATE ReqdTable SET ISTOP_4_Y = (SELECT STOP_YCORD FROM  InboundStopFields(4))
		UPDATE ReqdTable SET ISTOP_4_ARR_HR = (SELECT STOP_ARR_HR FROM  InboundStopFields(4))
		UPDATE ReqdTable SET ISTOP_4_ARR_MIN = (SELECT STOP_ARR_MIN FROM  InboundStopFields(4))
		UPDATE ReqdTable SET ISTOP_4_DEP_HR = (SELECT STOP_DEP_HR FROM  InboundStopFields(4))
		UPDATE ReqdTable SET ISTOP_4_DEP_MIN = (SELECT STOP_DEP_MIN FROM  InboundStopFields(4))
		UPDATE ReqdTable SET ISTOP_4_DUR_HR = (SELECT STOP_DUR_HR FROM  InboundStopFields(4))
		UPDATE ReqdTable SET ISTOP_4_DUR_MIN = (SELECT STOP_DUR_MIN FROM  InboundStopFields(4))
		UPDATE ReqdTable SET ISTOP_4_PURP = (SELECT STOP_PURP FROM  InboundStopFields(4))	
	END
	GO	
-------------------------------------------------------------------------------------------------------------------------------------
--8)Write Total Trip File Procedure 
-------------------------------------------------------------------------------------------------------------------------------------
--Write outputs to total trip file --Used while coding trips AND tours in Part-4
IF OBJECT_ID('dbo.WriteTotalTripFile') IS NOT NULL 
	DROP PROCEDURE WriteTotalTripFile;
GO
CREATE PROCEDURE WriteTotalTripFile 
	AS
	BEGIN
	INSERT INTO TotalTripFile SELECT SAMPN,PERNO,TOUR_ID,TRIP_ID,ORIGIN_LONG,ORIGIN_LAT,OTAZ,DEST_LONG,
	DEST_LAT,DTAZ,ORIG_PURP,DEST_PURP,ORIG_ARR_HR,ORIG_ARR_MIN,ORIG_DEP_HR,ORIG_DEP_MIN,DEST_ARR_HR,DEST_ARR_MIN,
	DEST_DEP_HR,DEST_DEP_MIN,TRIP_DUR_HR,TRIP_DUR_MIN,MODECODE,DRIVER,AUTO_OCC,TOURMODE,TOURPURP,BOARDING_PLACENO,
	BOARDING_PNAME,BOARDING_X,BOARDING_Y,ALIGHTING_PLACENO,ALIGHTING_PNAME,ALIGHTING_X,ALIGHTING_Y,PARKING_PLACENO,
	PARKING_PNAME,PARKING_X,PARKING_Y,SUBTOUR,IS_INBOUND,TRIPS_ON_JOURNEY,TRIPS_ON_TOUR,ORIG_IS_TOUR_ORIG,
	ORIG_IS_TOUR_DEST,DEST_IS_TOUR_DEST,DEST_IS_TOUR_ORIG
	FROM Reqdtable WHERE TRIP_ID IS NOT NULL
	
	END
	GO
-------------------------------------------------------------------------------------------------------------------------------------
	
-------------------------------------------------------------------------------------------------------------------------------------
--9)Write Total Tour File Procedure 
-------------------------------------------------------------------------------------------------------------------------------------
--Write outputs to total tour file --Used while coding trips AND tours in Part-4
IF OBJECT_ID('dbo.WriteTotalTourFile') IS NOT NULL 
	DROP PROCEDURE WriteTotalTourFile;
GO
CREATE PROCEDURE WriteTotalTourFile 
	AS
	BEGIN
		INSERT INTO TotalTourFile SELECT TOP(1) SAMPN,PERNO,TOUR_ID,ORIG_PLACENO,DEST_PLACENO,
		ORIG_X,ORIG_Y,ORIG_TAZ,DEST_X,DEST_Y,DEST_TAZ,TOURPURP,TOURMODE,(CASE 
			WHEN EXISTS(SELECT * FROM Reqdtable WHERE DRIVER = 1) THEN 1 
			ELSE 0
																END),
		ANCHOR_DEPART_HOUR,ANCHOR_DEPART_MIN,PRIMDEST_ARRIVE_HOUR,PRIMDEST_ARRIVE_MIN,PRIMDEST_DEPART_HOUR,
		PRIMDEST_DEPART_MIN,ANCHOR_ARRIVE_HOUR,ANCHOR_ARRIVE_MIN,TOUR_DUR_HR,TOUR_DUR_MIN,IS_SUBTOUR, 
		PARENT_TOUR_ID,PARENT_TOUR_MODE,HAS_SUBTOUR,CHILD_TOUR_ID,OUTBOUND_STOPS,INBOUND_STOPS, 
		OSTOP_1_PLACENO,OSTOP_1_X,OSTOP_1_Y,OSTOP_1_ARR_HR,OSTOP_1_ARR_MIN,OSTOP_1_DEP_HR,OSTOP_1_DEP_MIN, 
		OSTOP_1_DUR_HR,OSTOP_1_DUR_MIN,OSTOP_1_PURP,OSTOP_2_PLACENO,OSTOP_2_X,OSTOP_2_Y,OSTOP_2_ARR_HR,
		OSTOP_2_ARR_MIN,OSTOP_2_DEP_HR,OSTOP_2_DEP_MIN,OSTOP_2_DUR_HR,OSTOP_2_DUR_MIN,OSTOP_2_PURP, 
		OSTOP_3_PLACENO,OSTOP_3_X,OSTOP_3_Y,OSTOP_3_ARR_HR,OSTOP_3_ARR_MIN,OSTOP_3_DEP_HR,OSTOP_3_DEP_MIN, 
		OSTOP_3_DUR_HR,OSTOP_3_DUR_MIN,OSTOP_3_PURP,OSTOP_4_PLACENO,OSTOP_4_X,OSTOP_4_Y,OSTOP_4_ARR_HR,
		OSTOP_4_ARR_MIN,OSTOP_4_DEP_HR,OSTOP_4_DEP_MIN,OSTOP_4_DUR_HR,OSTOP_4_DUR_MIN,OSTOP_4_PURP,ISTOP_1_PLACENO,
		ISTOP_1_X,ISTOP_1_Y,ISTOP_1_ARR_HR,ISTOP_1_ARR_MIN,ISTOP_1_DEP_HR,ISTOP_1_DEP_MIN,ISTOP_1_DUR_HR,
		ISTOP_1_DUR_MIN,ISTOP_1_PURP,ISTOP_2_PLACENO,ISTOP_2_X,ISTOP_2_Y,ISTOP_2_ARR_HR,ISTOP_2_ARR_MIN,
		ISTOP_2_DEP_HR,ISTOP_2_DEP_MIN,ISTOP_2_DUR_HR,ISTOP_2_DUR_MIN,ISTOP_2_PURP,ISTOP_3_PLACENO, 
		ISTOP_3_X,ISTOP_3_Y,ISTOP_3_ARR_HR,ISTOP_3_ARR_MIN,ISTOP_3_DEP_HR,ISTOP_3_DEP_MIN,ISTOP_3_DUR_HR,
		ISTOP_3_DUR_MIN,ISTOP_3_PURP,ISTOP_4_PLACENO,ISTOP_4_X,ISTOP_4_Y,ISTOP_4_ARR_HR,ISTOP_4_ARR_MIN,
		ISTOP_4_DEP_HR,ISTOP_4_DEP_MIN,ISTOP_4_DUR_HR, ISTOP_4_DUR_MIN, ISTOP_4_PURP 
		FROM ReqdTable WHERE PrimDest = 1	
	END
	GO	
-------------------------------------------------------------------------------------------------------------------------------------