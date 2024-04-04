-- PostgreSQL Create Table or Materialized Views -------------------------------
DROP TABLE IF EXISTS public.spst_cnee_ports CASCADE;
CREATE TABLE public.spst_cnee_ports AS
SELECT
    consignee_name,
    consignee_address,
    port_of_foreign_lading_continent,
    port_of_foreign_lading_country,
    port_of_foreign_lading_name,
    SPLIT_PART(SUBSTRING("port_of_fl_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 2) AS port_of_fl_lat,
    SPLIT_PART(SUBSTRING("port_of_fl_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 1) AS port_of_fl_lon,
    port_of_unlading_country,
    port_of_unlading_name,
    SPLIT_PART(SUBSTRING("port_of_ul_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 2) AS port_of_ul_lat,
    SPLIT_PART(SUBSTRING("port_of_ul_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 1) AS port_of_ul_lon,
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp without time zone AS arrival_quarter,
    CONCAT(DATE_PART('YEAR', date_actual_arrival),'-',DATE_PART('QUARTER', date_actual_arrival), 'Q') AS arrival_quarter_txt,
    COUNT(_identifier) AS shipments_count,
    SUM(harmonized_value) AS value_sum,
    SUM(harmonized_weight) as weight_sum
FROM public.tbl_precleaned_all
WHERE vessel_type = 'container_ship'
GROUP BY
    consignee_name,
    consignee_address,
    port_of_foreign_lading_continent,
    port_of_foreign_lading_country,
    port_of_foreign_lading_name,
    SPLIT_PART(SUBSTRING("port_of_fl_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 2),
    SPLIT_PART(SUBSTRING("port_of_fl_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 1),
    port_of_unlading_country,
    port_of_unlading_name,
    SPLIT_PART(SUBSTRING("port_of_ul_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 2),
    SPLIT_PART(SUBSTRING("port_of_ul_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 1),
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp without time zone,
    CONCAT(DATE_PART('YEAR', date_actual_arrival),'-',DATE_PART('QUARTER', date_actual_arrival), 'Q')
    ;

CREATE INDEX ON public.spst_cnee_ports(consignee_name, port_of_foreign_lading_continent, port_of_foreign_lading_country, port_of_foreign_lading_name, shipments_count);
CREATE INDEX ON public.spst_cnee_ports(consignee_name, port_of_foreign_lading_continent, port_of_foreign_lading_country, port_of_foreign_lading_name, value_sum);
CREATE INDEX ON public.spst_cnee_ports(consignee_name, port_of_foreign_lading_continent, port_of_foreign_lading_country, port_of_foreign_lading_name, weight_sum);
CREATE INDEX ON public.spst_cnee_ports(consignee_name, port_of_unlading_country, port_of_unlading_name, shipments_count);
CREATE INDEX ON public.spst_cnee_ports(consignee_name, port_of_unlading_country, port_of_unlading_name, value_sum);
CREATE INDEX ON public.spst_cnee_ports(consignee_name, port_of_unlading_country, port_of_unlading_name, weight_sum);

CREATE INDEX ON public.spst_cnee_ports(arrival_quarter_txt, consignee_name, port_of_foreign_lading_country, port_of_foreign_lading_name, shipments_count);
CREATE INDEX ON public.spst_cnee_ports(arrival_quarter_txt, consignee_name, port_of_unlading_country, port_of_unlading_name, shipments_count);
