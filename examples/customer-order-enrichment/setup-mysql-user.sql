-- Local demo account: CDC reads plus interactive source-row edits.
CREATE USER 'dogpaddle'@'%' IDENTIFIED WITH mysql_native_password BY 'dogpaddle';
-- PROCESS permits DogPaddle to discover the InnoDB table identity.
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES, PROCESS ON *.* TO 'dogpaddle'@'%';
GRANT INSERT, UPDATE, DELETE ON crm.* TO 'dogpaddle'@'%';
