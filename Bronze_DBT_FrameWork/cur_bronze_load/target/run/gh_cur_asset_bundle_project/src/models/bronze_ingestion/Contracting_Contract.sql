
  
    
        create or replace table `mfl_bronze_dev`.`agcore`.`Contracting_Contract`
      
      using delta
      
      
      
      
      
      
      
      as
      




    WITH incremental_raw AS (
        SELECT * FROM parquet.`abfss://landing@mfgdasynapsedev01sa.dfs.core.windows.net/Agcore/firstload/Contracting_Contract/`
    )select * from incremental_raw;



  