-- Run ONCE as SYSTEM, connected to the pluggable database (service FREEPDB1 on Oracle Database Free).
-- Replace the password with your own strong password and keep it OUT of GitHub (put it in .env).
CREATE USER retail_src IDENTIFIED BY "CHANGE_ME_Strong#2026"
  DEFAULT TABLESPACE users
  QUOTA UNLIMITED ON users;

GRANT CREATE SESSION, CREATE TABLE TO retail_src;
