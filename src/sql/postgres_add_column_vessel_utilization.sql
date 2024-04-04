ALTER TABLE public.tbl_precleaned_all
ADD COLUMN vessel_utilization float GENERATED ALWAYS AS 
(CASE WHEN (containers_count IS NULL) OR (containers_count = 0) THEN NULL 
WHEN (vessel_teu IS NULL) OR (vessel_teu = 0) THEN NULL 
WHEN (containers_count > vessel_teu) THEN 1
ELSE cast(containers_count / vessel_teu AS double precision) END) STORED;
        