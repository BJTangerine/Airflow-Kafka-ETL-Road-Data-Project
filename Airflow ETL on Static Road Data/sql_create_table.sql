BEGIN;

CREATE TABLE public.toll_data
(
    row_id integer NOT NULL,
    vehicle_num character varying(5),
    vehicle_type character varying(10),
    vehicle_axles smallint,
    vehicle_code character varying(5),
    payment_code_type character varying(3),
    payment_amount numeric,
    tollplaza_id smallint NOT NULL,
    tollplaza_code character varying(11),
    day_name_of_week character varying(3),
    month_name character varying(3),
    day_of_month smallint,
    "time" time without time zone,
    year smallint,
    PRIMARY KEY (row_id)
);

END;