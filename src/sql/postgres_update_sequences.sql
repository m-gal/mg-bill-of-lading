-- For 'mg-bol-superset'

--SELECT * FROM information_schema.sequences;
--SELECT table_name FROM information_schema.columns WHERE table_catalog='mg-bol-superset' AND COLUMN_NAME='id' AND table_schema='public';

--CREATE SEQUENCE "ab_permission_id_seq";
--SELECT SETVAL('ab_permission_id_seq', (SELECT MAX(id) FROM ab_permission));


--SELECT  'CREATE SEQUENCE "'|| 'TEST' ||'_id_seq";';
--SELECT  'SELECT SETVAL( '''|| 'TEST' ||'_id_seq'', (SELECT MAX(id) FROM '||'ssdfs'||'));';


DO $$
DECLARE
i TEXT;
BEGIN
  FOR i IN (SELECT table_name FROM information_schema.columns WHERE table_catalog='mg-bol-superset' AND COLUMN_NAME='id' AND table_schema='public') LOOP
    EXECUTE 'CREATE SEQUENCE "'|| i ||'_id_seq";';
    EXECUTE 'SELECT SETVAL( '''|| i ||'_id_seq'', (SELECT MAX(id) FROM '|| i ||'));';
  END LOOP;
END $$;
