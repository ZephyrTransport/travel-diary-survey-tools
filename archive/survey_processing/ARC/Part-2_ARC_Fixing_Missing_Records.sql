--ARC Project on coding tours and trips using Regional Household Travel Survey Data
--Part-2: Fix missing records, remove inconsistencies FROM Person and Place tables 
--		  and code new fields in both tables
--Palvinder Singh			10/24/2012			singhp@pbworld.com
-------------------------------------------------------------------------------------------

---------------------------------------------
--INPUTS - Person and Place table
---------------------------------------------
SET NOCOUNT ON;	
PRINT 'Part - 2 is being executed and start time of the process is : ' + CAST(SYSDATETIME() AS VARCHAR)

---------------------------------------------
--START - Recoding Person File Records
---------------------------------------------
--Recoding NULL values of WORKS (Now 1 and 2)
UPDATE dbo.Person SET WORKS = 2 WHERE AGE < 16 
UPDATE dbo.Person SET WORKS = 2 WHERE AGEB = 1 AND WORKS IS NULL	

--Recoding Jobs, Hours, Hours2 and Hours3 to zero for Null values where WORKS = 2
UPDATE dbo.Person SET JOBS = 0, HOURS = 0, HOURS2 = 0, HOURS3 = 0 WHERE JOBS IS NULL AND WORKS = 2
UPDATE dbo.Person SET HOURS2 = 0 WHERE JOBS = 1 AND WORKS = 1
UPDATE dbo.Person SET HOURS3 = 0 WHERE JOBS IN (1,2) AND WORKS = 1

--Recoding Null values of WLOC to 0 for non-workers
UPDATE dbo.Person SET WLOC = 0 WHERE WLOC IS NULL AND WORKS = 2

--Recoding the Null values of (WTAZ, WXCORD, WYCORD) to (HTAZ, HXCORD, HYCORD) for 
--Home Based workers and setting Null values to zero for non-workers
UPDATE dbo.Person SET WTAZ = hh.HTAZ FROM dbo.Person, hh 
	WHERE dbo.Person.WLOC = 2 AND  dbo.Person.SAMPN = hh.SAMPN AND WTAZ IS NULL
UPDATE dbo.Person SET WXCORD = hh.HXCORD, WYCORD = hh.HYCORD FROM dbo.Person, hh 
	WHERE dbo.Person.WLOC = 2 AND dbo.Person.SAMPN = hh.SAMPN AND (WXCORD IS NULL AND WYCORD IS NULL)
--WTAZ=9999 for non-workers
UPDATE dbo.Person SET WTAZ = 9999 WHERE WTAZ IS NULL 
--WXCORD and WYCORD are fixed to zero for non-workers
UPDATE dbo.Person SET WXCORD = 0, WYCORD = 0 WHERE WXCORD IS NULL AND WYCORD IS NULL   

------------------------------------------------------------------
--Identify whether a person is student or not (1 - Yes, 0 - No)
------------------------------------------------------------------
ALTER TABLE dbo.Person ADD STUDENT int;
GO
UPDATE dbo.Person SET STUDENT = -1
UPDATE dbo.Person SET STUDENT = 1 WHERE STUDE IN (1,2) AND STUDENT = -1
UPDATE dbo.Person SET STUDENT = 0 WHERE STUDE = 3  AND STUDENT = -1
UPDATE dbo.Person SET STUDENT = 1 FROM dbo.Person, dbo.Place 
	WHERE dbo.Person.SAMPN = dbo.Place.SAMPN AND dbo.Person.PERNO = dbo.Place.PERNO 
		AND (dbo.Place.TPURP IN (11,12) OR O_TPURP IN ('ADULT DAYCARE','AFTER SCHOOL PROGRAM','ATTEND CLASS','BEFORE SCHOOL PROGRAM',
				'DAYCARE','GRADUATION','ORIENTATION','SCHOOL FUNCTION','SCHOOL RELATED','STUDYING')) AND dbo.Person.STUDENT = -1
UPDATE dbo.Person SET STUDENT = 0 FROM dbo.Person,dbo.Place 
	WHERE dbo.Person.SAMPN = dbo.Place.SAMPN AND dbo.Person.PERNO = dbo.Place.PERNO 
		AND dbo.Person.STUDE IN (8,9) and dbo.Place.TPURP NOT IN (11,12)  AND dbo.Person.STUDENT = -1
		
--Recoding 'OTHER' to categories using O_SCHOL
UPDATE dbo.Person SET SCHOL = (CASE
									WHEN O_SCHOL IN ('CONTINUING EDUCATION','FRESHMAN IN COLLEGE','BIBLE COLLEGE','SPANISH CLASS','TAX PROFESSIONAL COURSE',
													 'REAL ESTATE CLASS','TRAINING PROGRAM','ENGLISH CLASSES','PHOTOGRAPHY CLASSES','INSTITUTE','UNION CLASS',
													 'ADULT EDUCATION','CNA TRAINING','ART SCHOOL') THEN 5
									WHEN O_SCHOL IN ('GED','FRESHMAN IN COLLEGE','CERTIFICATE PROGRAM','CERTIFICATION','MENTAL HEALTH SCHOOL',
													 'SPECIAL','DISABILITY SUPPORT GROUP','MINISTERIAL SCHOOL','MENTAL HEALTH PROGRAM','SPECIAL NEEDS PROGRAM')
														THEN 4
								END) WHERE SCHOL = 97 AND STUDENT = 1
UPDATE dbo.Person SET SCHOL = (CASE
									WHEN AGE >= 19 THEN 5
									ELSE 4
								END) WHERE (SCHOL IS NULL OR SCHOL IN (98,99)) AND STUDENT = 1						
-------------------------------------------------------------------------------------------------------
--Person Type Definitions
-------------------------------------------------------------------------------------------------------
--1-"University Student (US)", 2-"Age 16-19 student, Any Work Status and Precollege(DS)",
--3-"Full-Time worker nonstudent age 16+ and total hours >=35(FW)",
--4-"Part-Time worker nonstudent age 16+ and total hours < 35(PW)",
--5-"Nonworker Nonstudent age 16-64 (NW)",6-"Nonworker Nonstudent age 65+ (RE)",
--7-"Age 6-15,nonworker and precollege (ND)", 8-"under age 6 Presch (PS)"
-------------------------------------------------------------------------------------------------------
ALTER TABLE dbo.Person ADD PerType VARCHAR(10);
GO
UPDATE  dbo.Person SET PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'US' 
	WHERE ((AGE >= 17 AND AGE < 998) OR AGEB = 2) AND STUDENT = 1 AND (SCHOL BETWEEN 5 AND 8) AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'DS' 
	WHERE STUDENT = 1 AND (AGE BETWEEN 16 AND 19) AND (SCHOL BETWEEN 1 and 4) AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'FW'
	WHERE ((hours >= 35 AND JOBS = 1) OR (hours + hours2 >= 35 AND JOBS = 2) OR (hours + hours2 + hours3 >= 35 AND JOBS >= 3))
		AND WORKS = 1 AND ((AGE >= 16 AND AGE < 998) OR AGEB IN (1,2)) AND STUDENT = 0 AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'PW'
	WHERE ((hours < 35 AND JOBS = 1) OR (hours + hours2 < 35 AND JOBS = 2) OR (hours + hours2 + hours3 < 35 AND JOBS >= 3))
		AND WORKS = 1 AND ((AGE >= 16 AND AGE < 998) OR AGEB IN (1,2)) AND STUDENT = 0 AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'NW' 
	WHERE WORKS = 2 AND STUDENT = 0 AND ((AGE BETWEEN 16 AND 64) OR AGEB = 2) AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'RE' 
	WHERE WORKS = 2 AND STUDENT = 0 AND ((AGE >= 65 AND AGE < 998) OR AGEB = 1) AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'ND' 
	WHERE (AGE BETWEEN 6 AND 15) AND PerType = 'NA'
--	WHERE ((AGE BETWEEN 6 AND 15) AND (SCHOL BETWEEN 1 and 4)) AND STUDENT = 1 AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'PS' 
	WHERE AGE < 6 AND PerType = 'NA'

--Recoding NA values fro 186 records that did not lie in any of the defintions above due to 'DK' or 'RF'
UPDATE  dbo.Person SET PerType = 'FW' WHERE AGE >= 19 AND WORKS = 1 AND HOURS >= 35 AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'PW' WHERE AGE >= 19 AND WORKS = 1 AND HOURS < 35 AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'US' WHERE AGE <= 30 AND WORKS = 2 AND STUDENT = 1 AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'NW' WHERE (AGE >= 30 AND AGE <= 64) AND WORKS = 2 AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'RE' WHERE (AGE >=65 AND AGE < 998) AND WORKS = 2 AND PerType = 'NA'
UPDATE  dbo.Person SET PerType = 'ND' WHERE AGE IN (998,999) AND WORKS = 2 AND PerType = 'NA'

---------------------------------------------
--END - Recoding Person File Records
---------------------------------------------

---------------------------------------------
--START - Recoding Place File Records
---------------------------------------------
ALTER TABLE dbo.Place ADD AGGACT VARCHAR(25),MODENAME VARCHAR(25);
GO

--Coding TPURP to 3 for observations with HTAZ = PTAZ and HXCORD = XCORD and HYCORD = YCORD
UPDATE dbo.Place SET dbo.Place.TPURP = 3 FROM dbo.Place,HH,dbo.Person  
	WHERE dbo.Place.XCORD = HH.HXCORD AND dbo.Place.YCORD = HH.HYCORD AND dbo.Place.PTAZ = HH.HTAZ AND dbo.Place.TPURP NOT IN (1,2,3,96,97)
	AND HH.SAMPN = dbo.Person.SAMPN AND HH.SAMPN = dbo.Place.SAMPN AND dbo.Place.PERNO = dbo.Person.PERNO
UPDATE dbo.Place SET dbo.Place.TPURP = 3 FROM dbo.Place,HH,dbo.Person  
	WHERE dbo.Place.XCORD = HH.HXCORD AND dbo.Place.YCORD = HH.HYCORD AND dbo.Place.PTAZ = HH.HTAZ AND dbo.Place.TPURP = 96
	AND dbo.Place.PNAME = 'HOME'
	AND HH.SAMPN = dbo.Person.SAMPN AND HH.SAMPN = dbo.Place.SAMPN AND dbo.Place.PERNO = dbo.Person.PERNO		

--Coding TPURP to 4 for persons on the BUS STOP waiting for transit or to get picked up
UPDATE dbo.Place SET TPURP = 4 WHERE ((CHARINDEX('BUS STOP', PNAME) > 0 OR CHARINDEX('TRANSIT STOP', PNAME) > 0
											OR CHARINDEX('STATION', PNAME) > 0) AND TPURP = 96) 
									 OR (O_TPURP = 'WAITING' AND (CHARINDEX('BUS STOP', PNAME) > 0 
																		OR CHARINDEX('STATION', PNAME) > 0) AND TPURP = 97)
																		
--Creating an AGGACT field for further calculations	
UPDATE dbo.Place SET dbo.Place.AGGACT = (CASE
									WHEN TPURP IN (1,2,3) THEN 'Home'
									WHEN TPURP IN (4) THEN 'ChangeMode'
									WHEN TPURP IN (5,6) THEN 'Escorting'
									WHEN TPURP IN (7,14,18,19,20) THEN 'Maintenance'
									WHEN TPURP IN (8,9) THEN 'Work'
									WHEN TPURP IN (13) THEN 'WorkRelated'
									WHEN TPURP IN (11,12) THEN 'School/University'
									WHEN TPURP IN (15,16,17) THEN 'Shopping'
									WHEN TPURP IN (21) THEN 'EatOut'
									WHEN TPURP IN (10,22,23,24) THEN 'Discretionary'
									WHEN TPURP IN (25) THEN 'Social/Visit'
									WHEN TPURP IN (96,97) THEN 'NA'
								END)
--Recoding AGGACT  FROM 'Work' to 'WorkRelated' for some misleading records								
UPDATE dbo.Place SET AGGACT = 'WorkRelated' FROM dbo.Place,dbo.Person 
	WHERE dbo.Place.SAMPN = dbo.Person.SAMPN AND dbo.Place.PERNO = dbo.Person.PERNO 
		AND dbo.Place.XCORD != dbo.Person.WXCORD AND dbo.Place.YCORD != dbo.Person.WYCORD AND dbo.Place.AGGACT = 'Work'
--Recoding AGGACT for TPURP 10 and 13 who reported the corresponding locations are work locations in Person file
UPDATE dbo.Place SET AGGACT = 'Work' FROM dbo.Place,dbo.Person 
	WHERE dbo.Place.SAMPN = dbo.Person.SAMPN AND dbo.Place.PERNO = dbo.Person.PERNO 
		AND dbo.Place.XCORD = dbo.Person.WXCORD AND dbo.Place.YCORD = dbo.Person.WYCORD AND dbo.Place.TPURP IN (10,13)
--Recoding AGGACT FROM 'NA' to other categories based on O_TPURP 
UPDATE dbo.Place SET dbo.Place.AGGACT = (CASE
			WHEN O_TPURP IN ('ACCOMPANY','ACCOMPANY PARENT','AUTO ACCIDENT','HOUSE HUNTING',
				'LOCATING/FINDING THIS Place','PRACTICE') AND AGGACT = 'NA' THEN 'Maintenance'
			WHEN O_TPURP IN ('ADULT DAYCARE','AFTER SCHOOL PROGRAM','ATTEND CLASS','BEFORE SCHOOL PROGRAM',
				'DAYCARE','GRADUATION','ORIENTATION','SCHOOL FUNCTION','SCHOOL RELATED','STUDYING')
				AND AGGACT = 'NA' THEN 'School/University'
			WHEN O_TPURP IN ('CARPOOL') AND AGGACT = 'NA' THEN 'Shopping'
			WHEN O_TPURP IN ('DROP OFF CHILD','DROP OFF CHILDREN','DROP OFF GRANDCHILDREN',
				'DROP OFF HH MEMBER','PICK UP CHILD','PICK UP CHILDREN','PICK UP GRANDCHILD',
				'PICK UP GRANDCHILDREN','PICK UP HH MEMBER') AND AGGACT = 'NA' THEN 'Escorting'
			WHEN O_TPURP IN ('EAT') AND AGGACT = 'NA' THEN 'EatOut'
			WHEN O_TPURP IN ('HOMESCHOOLED') AND AGGACT = 'NA' THEN 'Home'
			WHEN O_TPURP IN ('CLARINET LESSON','CUB SCOUTS','BALLET LESSON','ENTERTAINMENT','GIRL SCOUTS',
				'GUITAR LESSON','HORSEBACK LESSONS','MUSIC LESSON','MUSIC REHEARSAL','PIANO LESSONS',
				'VACATION','VIOLIN LESSON','ZITHER LESSON','ROAD TRIP','SIGHTSEEING','TRAVELING','AIRPORT',
				'ASKING FOR DIRECTIONS','BABYSITTER','BABYSITTING','BALLET LESSON','BATHROOM STOP',
				'BEING BABYSAT','BEING DROPPED OFF','BEING PICKED UP','CELL PHONE CALL-STOPPED',
				'CO-OP PICK UP','CO-OP PICKUP','HOTEL','LEFT-CHANGED MIND','LEFT-CLOSED',
				'LEFT-FORGOT PHONE','LEFT-NO ONE HOME','LEFT-TOO CROWDED','LEFT-WRONG OFFICE','OUT OF AREA',
				'OUT OF TOWN','SECOND HOME','VOLUNTEERING','WAITING','SLEEP','FUNERAL','GRAVE','')
				AND AGGACT = 'NA' THEN 'Discretionary'
										END) 
WHERE TPURP IN (96,97)

--Recoding mode to 'Driver' FROM 'Passenger' who reported that they are Passenger but TOTTR (i.e. total people in Vehicle) = 1
UPDATE dbo.Place SET MODE = 3 WHERE TOTTR = 1 AND MODE = 4
--Creating a fields MODENAME for further calculations	
UPDATE dbo.Place SET MODENAME = (CASE
				WHEN MODE = 1 THEN 'WALK'
				WHEN MODE IN (2,11) THEN 'BIKE/MOPED'
				WHEN MODE = 3 THEN 'DRIVER'
				WHEN MODE = 4 THEN 'PASSENGER'
				WHEN MODE = 5 THEN 'LOCALBUS'
				WHEN MODE = 6 THEN 'EXPRESSBUS'
				WHEN MODE = 7 THEN 'RAIL'
				WHEN MODE IN (8,9) THEN 'TAXI'			
				WHEN MODE = 10 THEN 'SCHOOLBUS'					
				WHEN MODE = 97 THEN 'OTHER'
							 END)	

ALTER TABLE dbo.Place DROP COLUMN TPURP,O_TPURP,TPUR2,O_TPURP2,O_MODE,HHMEM,PER1,PER2,PER3,PER4,PER5,NONHH,
	VEHNO,HOVL,DYGOV,PLOC,PRKTY,O_PRKTY,PAYPK,PKAMT,PKUNT,ROUTE,SERVC,O_SERVC,FARE,O_FARE,FAREC,TRIPNO,PTRIPS,
	ACCESSMODE,O_ACCESSMODE,EGRESSMODE,O_EGRESSMODE,PWGT,EXPPWGT;
---------------------------------------------
--END - Recoding Place File Records
---------------------------------------------	

---------------------------------------------
--OUTPUT - Modified Person and Place tables
---------------------------------------------

PRINT 'Part - 2 execution is over and end time of the process is : ' + CAST(SYSDATETIME() AS VARCHAR)