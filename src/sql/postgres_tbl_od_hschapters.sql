-- PostgreSQL Create Table or Materialized Views -------------------------------
DROP TABLE IF EXISTS public.spst_od_hschapters CASCADE;
CREATE TABLE public.spst_od_hschapters AS
SELECT
	hschapter,
    hscode_02_desc_short,
	port_of_foreign_lading_continent,
	port_of_foreign_lading_country,
	port_of_foreign_lading_locode,
	port_of_foreign_lading_name,
	SPLIT_PART(SUBSTRING("port_of_fl_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 2) AS port_of_fl_lat,
	SPLIT_PART(SUBSTRING("port_of_fl_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 1) AS port_of_fl_lon,
	port_of_unlading_continent,
	port_of_unlading_country,
	port_of_unlading_locode,
	port_of_unlading_name,
	SPLIT_PART(SUBSTRING("port_of_ul_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 2) AS port_of_ul_lat,
    SPLIT_PART(SUBSTRING("port_of_ul_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 1) AS port_of_ul_lon,
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp without time zone AS arrival_quarter,
    CONCAT(DATE_PART('YEAR', date_actual_arrival),'-',DATE_PART('QUARTER', date_actual_arrival), 'Q') AS arrival_quarter_txt
FROM public.tbl_precleaned_all
WHERE vessel_type = 'container_ship'
GROUP by
	hschapter,
    hscode_02_desc_short,
	port_of_foreign_lading_continent,
	port_of_foreign_lading_country,
	port_of_foreign_lading_locode,
	port_of_foreign_lading_name,
	SPLIT_PART(SUBSTRING("port_of_fl_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 2),
	SPLIT_PART(SUBSTRING("port_of_fl_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 1),
	port_of_unlading_continent,
	port_of_unlading_country,
	port_of_unlading_locode,
	port_of_unlading_name,
	SPLIT_PART(SUBSTRING("port_of_ul_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 2),
    SPLIT_PART(SUBSTRING("port_of_ul_centroid", 8, LENGTH("port_of_fl_centroid")-8), ' ', 1),
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp without time zone,
    CONCAT(DATE_PART('YEAR', date_actual_arrival),'-',DATE_PART('QUARTER', date_actual_arrival), 'Q')
	;

CREATE INDEX ON public.spst_od_hschapters(port_of_foreign_lading_country, hschapter);
CREATE INDEX ON public.spst_od_hschapters(port_of_foreign_lading_locode, hschapter);
CREATE INDEX ON public.spst_od_hschapters(port_of_foreign_lading_name, hschapter);

CREATE INDEX ON public.spst_od_hschapters(port_of_unlading_locode, hschapter);
CREATE INDEX ON public.spst_od_hschapters(port_of_unlading_name, hschapter);

CREATE INDEX ON public.spst_od_hschapters(port_of_foreign_lading_name, port_of_unlading_name);
CREATE INDEX ON public.spst_od_hschapters(port_of_unlading_name, port_of_foreign_lading_name);

CREATE INDEX ON public.spst_od_hschapters(arrival_quarter_txt, port_of_foreign_lading_name, port_of_unlading_name);
CREATE INDEX ON public.spst_od_hschapters(arrival_quarter_txt, port_of_unlading_name, port_of_foreign_lading_name);
