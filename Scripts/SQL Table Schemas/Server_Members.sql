CREATE TABLE IF NOT EXISTS public.server_members
(
discord_id bigint NOT NULL,
username character varying(255) COLLATE pg_catalog."default",
display_name character varying(255) COLLATE pg_catalog."default",
top_role character varying(255) COLLATE pg_catalog."default",
current_status character varying(50) COLLATE pg_catalog."default",
member_since timestamp without time zone,
roles text COLLATE pg_catalog."default",
num_roles integer,
boosting_since timestamp without time zone,
joined_discord timestamp without time zone,
permissions text COLLATE pg_catalog."default",
screening_pending boolean,
CONSTRAINT server_members_pkey PRIMARY KEY (discord_id)
);
