


    WITH incremental_raw AS (
        SELECT * FROM parquet.`abfss://landing@mfgdasynapsedev01sa.dfs.core.windows.net/Agcore/firstload/Scheduling_Load/`
    )select * from incremental_raw;


