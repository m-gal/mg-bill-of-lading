-- PostgreSQL Create Table or Materialized Views -------------------------------
DROP TABLE IF EXISTS public.spst_od_vessels CASCADE;
CREATE TABLE public.spst_od_vessels AS
SELECT
    hschapter,
    vessel_name_vt AS vessel_name,
	port_of_foreign_lading_continent,
	port_of_foreign_lading_country,
	port_of_foreign_lading_locode,
	port_of_foreign_lading_name,
	port_of_unlading_continent,
	port_of_unlading_country,
	port_of_unlading_locode,
	port_of_unlading_name,
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp WITHOUT time ZONE AS arrival_quarter,
    CONCAT(DATE_PART('YEAR', date_actual_arrival),'-',DATE_PART('QUARTER', date_actual_arrival), 'Q') AS arrival_quarter_txt,
    COUNT(DISTINCT voyage_number) AS voyages_count,
	AVG(containers_count) AS containers_count_avg,
	AVG(vessel_utilization) AS vessel_utilization_avg
FROM public.tbl_precleaned_all
WHERE vessel_type = 'container_ship'
GROUP BY
    hschapter,
    vessel_name_vt,
	port_of_foreign_lading_continent,
	port_of_foreign_lading_country,
	port_of_foreign_lading_locode,
	port_of_foreign_lading_name,
	port_of_unlading_continent,
	port_of_unlading_country,
	port_of_unlading_locode,
	port_of_unlading_name,
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp WITHOUT time ZONE,
    CONCAT(DATE_PART('YEAR', date_actual_arrival),'-',DATE_PART('QUARTER', date_actual_arrival), 'Q')
	;

CREATE INDEX ON public.spst_od_vessels(port_of_foreign_lading_country, vessel_name, hschapter);
CREATE INDEX ON public.spst_od_vessels(port_of_foreign_lading_locode, vessel_name, hschapter);
CREATE INDEX ON public.spst_od_vessels(port_of_foreign_lading_name, vessel_name, hschapter);

CREATE INDEX ON public.spst_od_vessels(port_of_unlading_locode, vessel_name, hschapter);
CREATE INDEX ON public.spst_od_vessels(port_of_unlading_name, vessel_name, hschapter);

CREATE INDEX ON public.spst_od_vessels(port_of_foreign_lading_name, port_of_unlading_name, vessel_name, hschapter);
CREATE INDEX ON public.spst_od_vessels(port_of_unlading_name, port_of_foreign_lading_name, vessel_name, hschapter);

CREATE INDEX ON public.spst_od_vessels(arrival_quarter_txt, port_of_foreign_lading_name, port_of_unlading_name, vessel_name);
CREATE INDEX ON public.spst_od_vessels(arrival_quarter_txt, port_of_unlading_name, port_of_foreign_lading_name, vessel_name);
CREATE INDEX ON public.spst_od_vessels(arrival_quarter, port_of_foreign_lading_name, port_of_unlading_name, vessel_name);
CREATE INDEX ON public.spst_od_vessels(arrival_quarter, port_of_unlading_name, port_of_foreign_lading_name, vessel_name);

CREATE INDEX ON public.spst_od_vessels(port_of_foreign_lading_country, vessel_name, vessel_utilization_avg);
CREATE INDEX ON public.spst_od_vessels(port_of_foreign_lading_locode, vessel_name, vessel_utilization_avg);
CREATE INDEX ON public.spst_od_vessels(port_of_foreign_lading_name, vessel_name, vessel_utilization_avg);
CREATE INDEX ON public.spst_od_vessels(port_of_foreign_lading_country, vessel_name, containers_count_avg);
CREATE INDEX ON public.spst_od_vessels(port_of_foreign_lading_locode, vessel_name, containers_count_avg);
CREATE INDEX ON public.spst_od_vessels(port_of_foreign_lading_name, vessel_name, containers_count_avg);

CREATE INDEX ON public.spst_od_vessels(port_of_unlading_locode, vessel_name, vessel_utilization_avg);
CREATE INDEX ON public.spst_od_vessels(port_of_unlading_name, vessel_name, vessel_utilization_avg);
CREATE INDEX ON public.spst_od_vessels(port_of_unlading_locode, vessel_name, containers_count_avg);
CREATE INDEX ON public.spst_od_vessels(port_of_unlading_name, vessel_name, containers_count_avg);

CREATE INDEX ON public.spst_od_vessels(vessel_name, vessel_utilization_avg, containers_count_avg);
CREATE INDEX ON public.spst_od_vessels(vessel_name, hschapter, vessel_utilization_avg, containers_count_avg);
