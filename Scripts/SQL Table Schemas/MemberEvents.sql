CREATE TABLE IF NOT EXISTS public.member_events
(
event_id integer NOT NULL DEFAULT nextval('member_events_event_id_seq'::regclass),
discord_id bigint,
event_type integer,
event_datetime timestamp without time zone,
CONSTRAINT member_events_pkey PRIMARY KEY (event_id),
CONSTRAINT member_events_event_type_check CHECK (event_type = ANY (ARRAY[1, '-1'::integer]))
);
