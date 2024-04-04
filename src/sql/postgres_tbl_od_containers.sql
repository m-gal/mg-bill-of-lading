-- PostgreSQL Create Table or Materialized Views -------------------------------
DROP TABLE IF EXISTS public.spst_od_containers CASCADE;
CREATE TABLE public.spst_od_containers AS
SELECT
	hschapter,
	port_of_foreign_lading_continent,
	port_of_foreign_lading_country,
	port_of_foreign_lading_locode,
	port_of_foreign_lading_name,
	port_of_unlading_continent,
	port_of_unlading_country,
	port_of_unlading_locode,
	port_of_unlading_name,
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp without time zone as arrival_quarter,
    CONCAT(DATE_PART('YEAR', date_actual_arrival) ,'-', DATE_PART('QUARTER', date_actual_arrival), 'Q') AS arrival_quarter_txt,
	AVG(containers_count) AS containers_count_avg,
	COUNT(_identifier) AS shipments_count,
	SUM(harmonized_value) AS value_sum,
	CASE
		when (AVG(containers_count) is null or AVG(containers_count)=0) then null
		when (SUM(harmonized_value) is null or SUM(harmonized_value)=0) then null
		else CAST(SUM(harmonized_value)/AVG(containers_count) as double precision) END
		AS container_value_avg,
	SUM(harmonized_weight) as weight_sum,
	CASE
		when (AVG(containers_count) is null or AVG(containers_count)=0) then null
		when (SUM(harmonized_weight) is null or SUM(harmonized_weight)=0) then null
		else CAST(SUM(harmonized_weight)/AVG(containers_count) as double precision) END
		AS container_weight_avg
FROM public.tbl_precleaned_all
WHERE vessel_type = 'container_ship'
GROUP by
	hschapter,
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

CREATE INDEX ON public.spst_od_containers(port_of_foreign_lading_country, hschapter);
CREATE INDEX ON public.spst_od_containers(port_of_foreign_lading_locode, hschapter);
CREATE INDEX ON public.spst_od_containers(port_of_foreign_lading_name, hschapter);

CREATE INDEX ON public.spst_od_containers(port_of_unlading_locode, hschapter);
CREATE INDEX ON public.spst_od_containers(port_of_unlading_name, hschapter);

CREATE INDEX ON public.spst_od_containers(arrival_quarter_txt, port_of_foreign_lading_name, port_of_unlading_name);
CREATE INDEX ON public.spst_od_containers(arrival_quarter_txt, port_of_unlading_name, port_of_foreign_lading_name);
