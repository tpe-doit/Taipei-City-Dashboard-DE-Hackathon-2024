DROP TABLE IF EXISTS public.test;
DROP TRIGGER IF EXISTS test_mtime ON public.test;
DROP SEQUENCE IF EXISTS public.test_ogc_fid_seq;

    -- create table
    CREATE TABLE IF NOT EXISTS public.test
    (
                data_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
        name character varying(50) COLLATE pg_catalog."default",
        addr text COLLATE pg_catalog."default",
        lng double precision,
        lat double precision,
        wkb_geometry geometry(Point,4326)
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

    -- grant table
    ALTER TABLE IF EXISTS public.test OWNER to postgres;
    GRANT ALL ON TABLE public.test TO postgres WITH GRANT OPTION;

DROP TABLE IF EXISTS public.test_history;
DROP TRIGGER IF EXISTS test_history_mtime ON public.test_history;
DROP SEQUENCE IF EXISTS public.test_history_ogc_fid_seq;

    -- create table
    CREATE TABLE IF NOT EXISTS public.test_history
    (
                data_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
        name character varying(50) COLLATE pg_catalog."default",
        addr text COLLATE pg_catalog."default",
        lng double precision,
        lat double precision,
        wkb_geometry geometry(Point,4326)
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

    -- grant table
    ALTER TABLE IF EXISTS public.test_history OWNER to postgres;
    GRANT ALL ON TABLE public.test_history TO postgres WITH GRANT OPTION;