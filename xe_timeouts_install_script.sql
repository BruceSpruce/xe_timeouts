---- 1. CREATE FOLDER FOR EVENTS ----
-- C:\XE -- repalce in whole script if you change it

---- 2. CHOOSE DATABASE FOR STRUCTURE ----
USE [_SQL_] --repalce [_SQL_] in whole script if you change it
GO

---- 3. CREATE SESSION ---

CREATE EVENT SESSION [Timeouts] ON SERVER 
ADD EVENT sqlserver.rpc_completed(SET collect_statement=(1)
    ACTION(sqlserver.client_hostname,sqlserver.database_name,sqlserver.plan_handle,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.username,query_hash)
    WHERE ([result]=(2))),
ADD EVENT sqlserver.sql_batch_completed(SET collect_batch_text=(1)
    ACTION(sqlserver.client_hostname,sqlserver.database_name,sqlserver.plan_handle,sqlserver.session_id,sqlserver.sql_text,sqlserver.transaction_id,sqlserver.username,query_hash)
	WHERE ([result]=(2)))
ADD TARGET package0.event_file(SET filename=N'C:\XE\timeouts_queries.xel',metadatafile = N'C:\XE\timeouts_queries.xem',max_file_size=(20),max_rollover_files=(10))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=5 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=ON)
GO

ALTER EVENT SESSION [Timeouts] ON SERVER 
STATE = START;
GO

---- 4. CREATE STRUCTURE ----

USE [_SQL_]
GO
CREATE SCHEMA XE
GO

CREATE TABLE [_SQL_].[XE].[timeouts](
	[ID] [int] IDENTITY(1,1) CONSTRAINT PK_TIMEOUTS_ID PRIMARY KEY CLUSTERED WITH FILLFACTOR = 100,
	[event_time] [datetime2](7) NULL,
	[cpu_time] [bigint] NULL,
	[duration] [bigint] NULL,
	[physical_reads] [bigint] NULL,
	[logical_reads] [bigint] NULL,
	[writes] [bigint] NULL,
	[row_count] [bigint] NULL,
	[result] [nvarchar](max) NULL,
	[username] [nvarchar](max) NULL,
	[transaction_id] [bigint] NULL,
	[sql_text] [nvarchar](max) NULL,
	[session_id] [int] NULL,
	[database_name] [nvarchar](max) NULL,
	[client_hostname] [nvarchar](max) NULL,
	[query_type] [nvarchar](5) NULL,
	[object_name] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

---- 5. CREATE PROCEDURE ----

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- EXEC usp_XEGetTimeouts @email_rec = 'MSSQLAdmins@domain.com', @Only_report = 1, @XE_Path = 'C:\XE', @StartDate = '1900-01-01 00:00:00.000', @CurrentDate = '1901-01-01 00:00:00.000'
-- EXEC usp_XEGetTimeouts @email_rec = 'MSSQLAdmins@domain.com', @Only_report = 0, @XE_Path = 'C:\XE'

USE [_SQL_]
GO
----
IF NOT EXISTS (
  SELECT 1 
    FROM INFORMATION_SCHEMA.ROUTINES 
   WHERE SPECIFIC_SCHEMA = N'dbo'
     AND SPECIFIC_NAME = N'usp_XEGetTimeouts' 
)
   EXEC ('CREATE PROCEDURE [dbo].[usp_XEGetTimeouts] AS SELECT 1');
GO
---

ALTER PROCEDURE [dbo].[usp_XEGetTimeouts] @profile_name NVARCHAR(128) = 'mail_profile',
										  @email_rec NVARCHAR(MAX) = 'MSSQLAdmins@domain.com', 
										  @Only_report BIT = 0,
										  @XE_Path NVARCHAR(MAX) = 'C:\XE', 
										  @StartDate datetime2 = '1900-01-01 00:00:00.000', 
										  @CurrentDate datetime2 = '1901-01-01 00:00:00.000',
                                          @MaxTimeoutsForNotification INT = 0
AS
BEGIN
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
    -- XE Patch
    DECLARE @XE_Path_XEL NVARCHAR(MAX) = @XE_Path + '\timeouts_queries*.xel'
    DECLARE @XE_Path_XEM NVARCHAR(MAX) = @XE_Path + '\timeouts_queries*.xem'

    IF (@StartDate = '1900-01-01 00:00:00.000')
	    SELECT @StartDate = DATEADD(dd, -1, GETDATE());

    IF (@CurrentDate = '1901-01-01 00:00:00.000')
	    SELECT @CurrentDate = GETDATE();

    IF (@Only_report = 0)
    BEGIN
	    --- GET DATA FROM XML --
	    SELECT @CurrentDate = GETDATE();
	    SELECT @StartDate = ISNULL(MAX(event_time), CAST('2001-01-01 00:00:00.000' AS datetime2)) FROM [_SQL_].[XE].[timeouts]
	    --- INSERT NEW DATA
	    INSERT INTO [_SQL_].[XE].[timeouts]
	    SELECT	DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), CURRENT_TIMESTAMP), x.event_data.value('(event/@timestamp)[1]', 'datetime2')) AS event_time,
			    x.event_data.value('(event/data[@name="cpu_time"])[1]', 'bigint') AS cpu_time,
			    x.event_data.value('(event/data[@name="duration"])[1]', 'bigint') AS duration,
			    x.event_data.value('(event/data[@name="physical_reads"])[1]', 'bigint') AS physical_reads,
			    x.event_data.value('(event/data[@name="logical_reads"])[1]', 'bigint') AS logical_reads,
			    x.event_data.value('(event/data[@name="writes"])[1]', 'bigint') AS writes,
			    x.event_data.value('(event/data[@name="row_count"])[1]', 'bigint') AS row_count,
			    x.event_data.value('(event/data[@name="result"]/text)[1]', 'nvarchar(max)') AS result,
			    x.event_data.value('(event/action[@name="username"])[1]', 'nvarchar(max)') AS username,
			    x.event_data.value('(event/action[@name="transaction_id"])[1]', 'bigint') AS transaction_id,
			    CASE object_name
				    WHEN 'sql_batch_completed' THEN x.event_data.value('(event/data[@name="batch_text"])[1]', 'nvarchar(max)')
				    WHEN 'rpc_completed' THEN x.event_data.value('(event/data[@name="statement"])[1]', 'nvarchar(max)')
			    END AS sql_text
			    ,
			    x.event_data.value('(event/action[@name="session_id"])[1]', 'int') AS session_id,
			    x.event_data.value('(event/action[@name="database_name"])[1]', 'nvarchar(max)') AS database_name,
			    x.event_data.value('(event/action[@name="client_hostname"])[1]', 'nvarchar(max)') AS client_hostname,
			    CASE object_name
				    WHEN 'sql_batch_completed' THEN 'batch'
				    WHEN 'rpc_completed' THEN 'rpc'
			    END AS query_type,
			    CASE object_name
				    WHEN 'sql_batch_completed' THEN SUBSTRING(x.event_data.value('(event/data[@name="batch_text"])[1]', 'nvarchar(max)'), 0, 128)
				    WHEN 'rpc_completed' THEN x.event_data.value('(event/data[@name="object_name"])[1]', 'nvarchar(max)')
			    END AS object_name
	    FROM    sys.fn_xe_file_target_read_file (@XE_Path_XEL, @XE_Path_XEM, null, null)
			       CROSS APPLY (SELECT CAST(event_data AS XML) AS event_data) as x
	    WHERE DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), CURRENT_TIMESTAMP), x.event_data.value('(event/@timestamp)[1]', 'datetime2')) > @StartDate
	    ORDER BY event_time DESC
    END
    ---
    ---- REPORT ----
    Declare @body varchar(max), @bodyW varchar(max),
		    @TableHeadW varchar(max),
		    @TableTailW varchar(max),
		    @Subject varchar(100),
		    @SendEmail BIT = 0,
            @NumberOfTimeouts INT = 0;
 
    Set NoCount On;
    /* -------------------------------------------------------------------------------------------------------------- */
    -- REPORT - NUMBER OF ERROST AND LONGEST DURATION
    IF OBJECT_ID('tempdb.dbo.#TempRap', 'U') IS NOT NULL
      DROP TABLE #TempRap;
    IF OBJECT_ID('tempdb.dbo.#TempRap2', 'U') IS NOT NULL
      DROP TABLE #TempRap2;
    ---- TIME LINE TABLES ----
    IF OBJECT_ID('tempdb.dbo.#temp1', 'U') IS NOT NULL
      DROP TABLE #temp1;
    IF OBJECT_ID('tempdb.dbo.#Hours', 'U') IS NOT NULL
      DROP TABLE #Hours;
    
    DECLARE @RowCount INT = 0;

    SELECT	TOP(10)
		    ROW_NUMBER() OVER (ORDER BY COUNT(object_name) DESC) AS Pos,
		    object_name,
		    query_type,
		    result,
		    SUM(CAST(cpu_time AS BIGINT)) AS SUM_cpu, 
		    AVG(CAST(duration AS BIGINT)) AS AVG_duration,
		    SUM(CAST(physical_reads AS BIGINT)) SUM_physical_reads,
		    SUM(CAST(logical_reads AS BIGINT)) SUM_logical_reads,
		    SUM(CAST(writes AS BIGINT)) AS SUM_writes,
		    AVG(CAST(row_count AS BIGINT)) AS AVG_row_count,
		    COUNT(object_name) AS NbOfErr
    INTO #TempRap
    FROM [_SQL_].[XE].[timeouts]
    WHERE event_time > @StartDate AND event_time <= @CurrentDate
    GROUP BY object_name,
		    query_type,
		    result
    ORDER BY NbOfErr DESC

    SET @RowCount = @@ROWCOUNT;

    -- GET NUMBER OF ALL TIMEOUTS --
    SELECT @NumberOfTimeouts = COUNT(*) 
    FROM [_SQL_].[XE].[timeouts]
    WHERE event_time > @StartDate AND event_time <= @CurrentDate
    --
    IF (@RowCount <> 0 AND @NumberOfTimeouts > @MaxTimeoutsForNotification)
    BEGIN
	    Set @TableTailW = '</table>';
	    Set @TableHeadW = '<table cellpadding=0 cellspacing=0 border=0><caption>TOP 10 TIMEOUTS. DATE FROM ' + CONVERT(CHAR(19), @StartDate, 121) + ' TO ' + CONVERT(CHAR(19), @CurrentDate, 121) + '</caption>' +
					      '<tr bgcolor=#c0f4c3>' +
					      '<td align=center><b>Pos</b></td>' +
					      '<td align=center><b>Object name</b></td>' +
					      '<td align=center><b>Query type</b></td>' +
					      '<td align=center><b>Result</b></td>' +
					      '<td align=center><b>SUM CPU</b></td>' +
					      '<td align=center><b>AVG Duration (s)</b></td>' + 
					      '<td align=center><b>SUM Physical reads</b></td>' + 
					      '<td align=center><b>SUM Logical reads</b></td>' + 
					      '<td align=center><b>SUM Writes</b></td>' + 
					      '<td align=center><b>AVG Row count</b></td>' + 
					      '<td align=center><b>Errors count</b></td></tr>';

	    Select @bodyW = (SELECT ISNULL(Pos, '') AS [TD align=right]
						      ,ISNULL(SUBSTRING(object_name, 1, 255), 'n/a') AS [TD align=left]
						      ,ISNULL(query_type, 'n/a') AS [TD align=center nowrap]
						      ,ISNULL(result, 'n/a') AS [TD align=center nowrap]
						      ,ISNULL(FORMAT(SUM_cpu/1000, '### ### ### ###'), 0) AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(AVG_duration/1000000.0, '### ### ### ##0.00'), 0) AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(SUM_physical_reads, '### ### ### ### ###'), 0) AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(SUM_logical_reads, '### ### ### ### ###'), 0) AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(SUM_writes, '### ### ### ### ###'), 0) AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(AVG_row_count, '### ### ### ### ###'), 0) AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(NbOfErr, '### ### ### ### ###'), 0) AS [TD align=right nowrap]
					    FROM #TempRap
					    ORDER BY Pos ASC
					    For XML raw('tr'), Elements)

	    -- Replace the entity codes and row numbers
	    Set @bodyW = Replace(@bodyW, '_x0020_', space(1))
	    Set @bodyW = Replace(@bodyW, '_x003D_', '=')

	    -- CREATE HTML BODY 
	    Select @body = @TableHeadW + @bodyW + @TableTailW + '</br>'

	    SET @SendEmail = 1;

    END -- IF ROWCOUNT <> 0

    -- 24H TIMELINE RAPORT ---

    ;WITH top10(pos, object_name, NbOfErr) AS
    (
	    SELECT	TOP(10)
			    ROW_NUMBER() OVER (ORDER BY COUNT(object_name) DESC) AS pos,
			    object_name,		
			    COUNT(object_name) AS NbOfErr
	    FROM [_SQL_].[XE].[timeouts]
	    WHERE event_time > @StartDate AND event_time <= @CurrentDate
	    GROUP BY object_name
	    ORDER BY NbOfErr DESC
    )
    SELECT	SUBSTRING(CAST(TmO.event_time AS VARCHAR),12,2) AS Hour,
		    T10.pos,
		    Count(TmO.object_name) AS NmbrOf
    INTO #temp1
    FROM [_SQL_].[XE].[timeouts] AS TmO
    JOIN top10 T10 ON TmO.object_name = T10.object_name
    WHERE event_time > @StartDate AND event_time <= @CurrentDate
    GROUP BY TmO.object_name, SUBSTRING(CAST(TmO.event_time AS VARCHAR),12,2), T10.pos
    ORDER BY Hour;
    -- HOURS ---
    ;WITH n(n) AS
    (
        SELECT 0
        UNION ALL
        SELECT n+1 FROM n WHERE n < 23
    )
    SELECT FORMAT(n, '00') Hour
    INTO #Hours
    FROM n ORDER BY n
    OPTION (MAXRECURSION 23);
    -- PIVOT ---
    SELECT H.Hour, P.Pos1, P.Pos2, P.Pos3, P.Pos4, P.Pos5, P.Pos6, P.Pos7, P.Pos8, P.Pos9, P.Pos10 
    INTO #TempRap2
    FROM #Hours AS H
    LEFT JOIN (
			    SELECT	Hour,[1] AS Pos1, [2] AS Pos2, [3] AS Pos3, [4] AS Pos4, [5] AS Pos5, 
						     [6] AS Pos6, [7] AS Pos7, [8] AS Pos8, [9] AS Pos9, [10] AS Pos10
			    FROM (SELECT Hour, pos, NmbrOf FROM #temp1) AS ST
			    PIVOT
			    (
				    SUM(NmbrOf)
				    FOR pos IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10])
			    ) AS pvt) AS P ON H.Hour = P.Hour;
	
    --- RAPORT TIMELINE ---
    IF (@@ROWCOUNT <> 0)
    BEGIN
	    Set @TableTailW = '</table>';
	    Set @TableHeadW = '<table cellpadding=0 cellspacing=0 border=0><caption>24H TIMELINE TIMEOUTS</caption>' +
					      '<tr bgcolor=#ffff99>' +
					      '<td align=center><b>Hour</b></td>' +
					      '<td align=center><b>Pos 1</b></td>' +
					      '<td align=center><b>Pos 2</b></td>' +
					      '<td align=center><b>Pos 3</b></td>' +
					      '<td align=center><b>Pos 4</b></td>' +
					      '<td align=center><b>Pos 5</b></td>' +
					      '<td align=center><b>Pos 6</b></td>' +
					      '<td align=center><b>Pos 7</b></td>' +
					      '<td align=center><b>Pos 8</b></td>' +
					      '<td align=center><b>Pos 9</b></td>' +
					      '<td align=center><b>Pos 10</b></td></tr>';
					  
	    Select @bodyW = (SELECT ISNULL(Hour, '') AS [TD align=center]
						      ,ISNULL(FORMAT(Pos1, '### ###'), '') AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(Pos2, '### ###'), '') AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(Pos3, '### ###'), '') AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(Pos4, '### ###'), '') AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(Pos5, '### ###'), '') AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(Pos6, '### ###'), '') AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(Pos7, '### ###'), '') AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(Pos8, '### ###'), '') AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(Pos9, '### ###'), '') AS [TD align=right nowrap]
						      ,ISNULL(FORMAT(Pos10, '### ###'), '') AS [TD align=right nowrap]
					    FROM #TempRap2
					    ORDER BY Hour ASC
					    For XML raw('tr'), Elements)

	    -- Replace the entity codes and row numbers
	    Set @bodyW = Replace(@bodyW, '_x0020_', space(1))
	    Set @bodyW = Replace(@bodyW, '_x003D_', '=')

	    -- CREATE HTML BODY 
	    Select @body = @body + @TableHeadW + @bodyW + @TableTailW + '</br>';

    END -- IF ROWCOUNT <> 0	

    IF (@SendEmail = 1 AND 
        @email_rec IS NOT NULL)
    BEGIN
	    Select @body = '<html><head><style>' +
					      'td {border: solid black 1px;padding-left:5px;padding-right:5px;padding-top:1px;padding-bottom:1px;font-size:11pt;} ' +
					      '</style>' +
					      '</head>' +
					      '<body>' + @body + '</br>
                          All the timeouts collected: <b>' + CAST(@NumberOfTimeouts AS VARCHAR(10))  + '</b></br>
                          Timeout notification level: <b>' + CAST(@MaxTimeoutsForNotification AS VARCHAR(10))  + '</b></br>
                          </br>XE Timeouts 2019</body></html>'

	    --SELECT @body

	    SET @Subject = '[' + @@servername + '] XE TIMEOUTS. REPORT OF ' +  CONVERT(CHAR(10), GETDATE(), 121);

	    -- return output
	     EXEC msdb.dbo.sp_send_dbmail
				    @profile_name = @profile_name,
				    @recipients = @email_rec,
				    @body =  @body,
				    @subject = @Subject,
				    @body_format = 'HTML';
    END -- END IF SendEmail
    ---- CLEAN UP --
    IF OBJECT_ID('tempdb.dbo.#TempRap', 'U') IS NOT NULL
      DROP TABLE #TempRap;
    IF OBJECT_ID('tempdb.dbo.#TempRap2', 'U') IS NOT NULL
      DROP TABLE #TempRap2;
    IF OBJECT_ID('tempdb.dbo.#temp1', 'U') IS NOT NULL
      DROP TABLE #temp1;
    IF OBJECT_ID('tempdb.dbo.#Hours', 'U') IS NOT NULL
      DROP TABLE #Hours;
END
GO

---- 6. JOB ----

USE [msdb]
GO

DECLARE @Date datetime2 = GETDATE();
DECLARE @Name NVARCHAR(100) = ORIGINAL_LOGIN()
DECLARE @Description NVARCHAR(2000) = N'Presents report from timeouts captured by Extended Events - ' + CONVERT(CHAR(10), @Date, 121) + ' - ' + @Name;

DECLARE @jobId BINARY(16)
EXEC  msdb.dbo.sp_add_job @job_name=N'__XE_TIMEOUTS__', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@description=@Description, 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
select @jobId
GO
EXEC msdb.dbo.sp_add_jobserver @job_name=N'__XE_TIMEOUTS__', @server_name = @@SERVERNAME
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_add_jobstep @job_name=N'__XE_TIMEOUTS__', @step_name=N'_report_', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_fail_action=2, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXEC [_SQL_].[XE].usp_XEGetTimeouts @profile_name = ''mail_profile'', @email_rec = ''MSSQLAdmins@domain.com'', @XE_Path=''C:\XE'', @Only_report = 0, @MaxErrorsForNotification = 0;', 
		@database_name=N'master', 
		@flags=0
GO
USE [msdb]
GO
EXEC msdb.dbo.sp_update_job @job_name=N'__XE_TIMEOUTS__', 
		@enabled=1, 
		@start_step_id=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=2, 
		@notify_level_page=2, 
		@delete_level=0, 
		@category_name=N'Database Maintenance', 
		@owner_login_name=N'sa', 
		@notify_email_operator_name=N'', 
		@notify_page_operator_name=N''
GO
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule @job_name=N'__XE_TIMEOUTS__', @name=N'workday at 6 AM', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=62, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20230721, 
		@active_end_date=99991231, 
		@active_start_time=60000, 
		@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
select @schedule_id
GO
DECLARE @schedule_id int
EXEC msdb.dbo.sp_add_jobschedule @job_name=N'__XE_TIMEOUTS__', @name=N'workday at 4 PM', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=62, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20230721, 
		@active_end_date=99991231, 
		@active_start_time=160000, 
		@active_end_time=235959, @schedule_id = @schedule_id OUTPUT
select @schedule_id
GO
