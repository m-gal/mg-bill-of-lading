-- PostgreSQL Create Table or Materialized Views -------------------------------
DROP TABLE IF EXISTS public.spst_cnee_shippers CASCADE;
CREATE TABLE public.spst_cnee_shippers AS
SELECT
    consignee_name,
    consignee_address,
    carrier_code,
    shipper_name,
    port_of_foreign_lading_country,
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp without time zone AS arrival_quarter,
    CONCAT(DATE_PART('YEAR', date_actual_arrival),'-',DATE_PART('QUARTER', date_actual_arrival), 'Q') AS arrival_quarter_txt,
    COUNT(_identifier) AS shipments_count,
    COUNT(DISTINCT voyage_number) AS voyages_count,
    COUNT(DISTINCT container_number) AS containers_count,
    SUM(harmonized_value) AS value_sum,
    AVG(containers_count) AS containers_count_avg
FROM public.tbl_precleaned_all
WHERE vessel_type = 'container_ship'
GROUP BY
    consignee_name,
    consignee_address,
    carrier_code,
    shipper_name,
    port_of_foreign_lading_country,
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp without time zone,
    CONCAT(DATE_PART('YEAR', date_actual_arrival),'-',DATE_PART('QUARTER', date_actual_arrival), 'Q')
    ;

CREATE INDEX ON public.spst_cnee_shippers(consignee_name, carrier_code, shipper_name, shipments_count);
CREATE INDEX ON public.spst_cnee_shippers(consignee_name, carrier_code, shipper_name, voyages_count);
CREATE INDEX ON public.spst_cnee_shippers(consignee_name, carrier_code, shipper_name, containers_count);
CREATE INDEX ON public.spst_cnee_shippers(consignee_name, carrier_code, shipper_name, value_sum);
CREATE INDEX ON public.spst_cnee_shippers(consignee_name, carrier_code, shipper_name, containers_count_avg);

CREATE INDEX ON public.spst_cnee_shippers(arrival_quarter_txt, consignee_name, carrier_code, shipper_name);
