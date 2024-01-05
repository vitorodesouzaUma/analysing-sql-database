/*-----------------------------------------------------------------------------*/
/*--------------------------- CREATE TABLES -----------------------------------*/

CREATE TABLE IF NOT EXISTS datatable (
    id SERIAL,
    name VARCHAR(50),
    height NUMERIC,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS log_dml_commands (
    id SERIAL,
    command VARCHAR(10),
    interaction_timestamp TIMESTAMP,
    interaction_user VARCHAR(50),
    PRIMARY KEY(id)
);

/*-----------------------------------------------------------------------------*/
/*-------------------------- CREATE FUNCTION -----------------------------------*/

CREATE or REPLACE FUNCTION f_log_dml_commands() RETURNS Trigger
AS
$$ 
BEGIN
    
    INSERT INTO "log_dml_commands" 
        (command,interaction_timestamp,interaction_user) 
    VALUES 
        (TG_OP,NOW(),CURRENT_USER);

RETURN NEW;
END
$$ LANGUAGE plpgsql;

/*-----------------------------------------------------------------------------*/
/*------------------------------ TRIGGER --------------------------------------*/


CREATE or REPLACE TRIGGER tr_dml_command_track AFTER INSERT or UPDATE or DELETE ON datatable
FOR EACH ROW 
EXECUTE PROCEDURE f_log_dml_commands();

/* TEST INSERT */ 
INSERT INTO datatable (name,height) VALUES ('João',1.89);

SELECT * FROM log_dml_commands;

/* TEST UPDATE */ 
UPDATE datatable 
SET 
    height = 1.90
WHERE
    name = 'João';

SELECT * FROM log_dml_commands;

/* TEST DELETE */ 
DELETE FROM datatable WHERE name = 'João';

SELECT * FROM log_dml_commands;


SELECT * FROM datatable;