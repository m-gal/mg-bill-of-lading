-- PostgreSQL Create Table or Materialized Views -------------------------------
DROP TABLE IF EXISTS public.spst_cnee_hschapters CASCADE;
CREATE TABLE public.spst_cnee_hschapters AS
SELECT
    consignee_name,
    consignee_address,
    port_of_foreign_lading_country,
    hschapter,
    hscode_02_desc_short,
    hscode_04_desc_short,
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
    port_of_foreign_lading_country,
    hschapter,
    hscode_02_desc_short,
    hscode_04_desc_short,
    CAST(DATE_TRUNC('QUARTER', date_actual_arrival) AS DATE)::timestamp without time zone,
    CONCAT(DATE_PART('YEAR', date_actual_arrival),'-',DATE_PART('QUARTER', date_actual_arrival), 'Q')
    ;

CREATE INDEX ON public.spst_cnee_hschapters(consignee_name, consignee_address, hschapter, hscode_02_desc_short, hscode_04_desc_short);
CREATE INDEX ON public.spst_cnee_hschapters(consignee_name, hschapter, hscode_02_desc_short, shipments_count);
CREATE INDEX ON public.spst_cnee_hschapters(consignee_name, hschapter, hscode_02_desc_short, value_sum);
CREATE INDEX ON public.spst_cnee_hschapters(consignee_name, hschapter, hscode_02_desc_short, weight_sum);
CREATE INDEX ON public.spst_cnee_hschapters(arrival_quarter_txt, consignee_name, consignee_address, hschapter, hscode_02_desc_short);
