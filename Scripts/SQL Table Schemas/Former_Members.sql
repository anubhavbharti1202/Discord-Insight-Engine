CREATE TABLE IF NOT EXISTS public.former_members
(
discord_id bigint NOT NULL,
username character varying(255) COLLATE pg_catalog."default",
display_name character varying(255) COLLATE pg_catalog."default",
top_role character varying(255) COLLATE pg_catalog."default",
joined_at timestamp without time zone,
roles text COLLATE pg_catalog."default",
num_roles integer,
boosting_since timestamp without time zone,
joined_discord timestamp without time zone,
permissions text COLLATE pg_catalog."default",
screening_pending boolean,
left_before timestamp without time zone,
CONSTRAINT former_members_pkey PRIMARY KEY (discord_id)
);
