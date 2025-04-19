
  
    
        create or replace table `mfl_bronze_dev`.`agcore`.`Scheduling_Load`
      
      using delta
      
      
      
      
      
      
      
      as
      




    WITH incremental_raw AS (
        SELECT * FROM parquet.`abfss://landing@mfgdasynapsedev01sa.dfs.core.windows.net/Agcore/firstload/Scheduling_Load/`
    )select * from incremental_raw;



  