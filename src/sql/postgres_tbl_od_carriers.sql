-- PostgreSQL Create Table or Materialized Views -------------------------------
DROP TABLE IF EXISTS public.spst_od_carriers CASCADE;
CREATE TABLE public.spst_od_carriers AS
SELECT
    carrier_code,
--	hschapter,
	port_of_foreign_lading_continent,
	port_of_foreign_lading_country,
	port_of_foreign_lading_locode,
	port_of_foreign_lading_name,
	port_of_unlading_continent,
	port_of_unlading_country,
	port_of_unlading_locode,
	port_of_unlading_name,
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp without time zone as arrival_quarter,
    CONCAT(DATE_PART('YEAR', date_actual_arrival),'-',DATE_PART('QUARTER', date_actual_arrival), 'Q') AS arrival_quarter_txt,
    AVG(vessel_utilization) AS vessel_utilization_avg
FROM public.tbl_precleaned_all
WHERE vessel_type = 'container_ship'
GROUP by
    carrier_code,
--	hschapter,
	port_of_foreign_lading_continent,
	port_of_foreign_lading_country,
	port_of_foreign_lading_locode,
	port_of_foreign_lading_name,
	port_of_unlading_continent,
	port_of_unlading_country,
	port_of_unlading_locode,
	port_of_unlading_name,
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp without time zone,
    CONCAT(DATE_PART('YEAR', date_actual_arrival),'-',DATE_PART('QUARTER', date_actual_arrival), 'Q')
	;

CREATE INDEX ON public.spst_od_carriers(port_of_foreign_lading_country, carrier_code);
CREATE INDEX ON public.spst_od_carriers(port_of_foreign_lading_locode, carrier_code);
CREATE INDEX ON public.spst_od_carriers(port_of_foreign_lading_name, carrier_code);

CREATE INDEX ON public.spst_od_carriers(port_of_unlading_locode, carrier_code);
CREATE INDEX ON public.spst_od_carriers(port_of_unlading_name, carrier_code);

CREATE INDEX ON public.spst_od_carriers(port_of_foreign_lading_name, port_of_unlading_name);
CREATE INDEX ON public.spst_od_carriers(port_of_unlading_name, port_of_foreign_lading_name);

CREATE INDEX ON public.spst_od_carriers(arrival_quarter_txt, port_of_foreign_lading_name, port_of_unlading_name);
CREATE INDEX ON public.spst_od_carriers(arrival_quarter_txt, port_of_unlading_name, port_of_foreign_lading_name);
