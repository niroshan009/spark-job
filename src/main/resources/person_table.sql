CREATE TABLE demo.person.information
(
  id integer,
  first_name string,
  last_name string,
  email string,
  gender string,
  ip_address string
)
PARTITIONED BY (id);