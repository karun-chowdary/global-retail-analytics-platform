CREATE TABLE [audit].[pipeline_runs] (
    [run_id]          VARCHAR (64)  NOT NULL,
    [pipeline_name]   VARCHAR (100) NOT NULL,
    [activity_name]   VARCHAR (100) NOT NULL,
    [source_system]   VARCHAR (30)  NOT NULL,
    [target_table]    VARCHAR (100) NOT NULL,
    [rows_read]       BIGINT        NULL,
    [rows_copied]     BIGINT        NULL,
    [copy_duration_s] INT           NULL,
    [status]          VARCHAR (20)  NOT NULL,
    [logged_at_utc]   DATETIME2 (6) NOT NULL
);


GO