DROP TABLE LOG_PROC_EXECUCAO;
DROP TABLE SEMAFORO_PROC_EXECUCAO;
DROP TABLE LOG_LEVEL;
DROP TABLE CQN_ROWS_CHANGED;
DROP TABLE CQN_TABLES_CHANGED;
DROP TABLE CQN_QUERIES;
DROP TABLE CQN_EVENTS;
DROP TABLE CQN_QUERY_OPERATION_TYPE;
DROP TABLE CQN_QUERY_EVENT_TYPES;

CREATE TABLE SEMAFORO_PROC_EXECUCAO (
  CD_ID NUMBER NOT NULL PRIMARY KEY,
  PROC_NAME VARCHAR2(200),
  ARGS VARCHAR2(4000) CHECK (ARGS IS JSON),
  RUNNING NUMBER(1) DEFAULT 0 CHECK (RUNNING IN (0,1)),
  HAS_ERROS NUMBER(1) DEFAULT 0 CHECK (HAS_ERROS IN (0,1)),
  DT_CREATED TIMESTAMP DEFAULT SYSTIMESTAMP,
  DT_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP
);
/
CREATE INDEX IDX_SEMAFORO_PROC_EXECUCAO ON SEMAFORO_PROC_EXECUCAO(PROC_NAME, DT_CREATED, RUNNING, HAS_ERROS);
/
CREATE SEQUENCE SEQ_SEMAFORO_PROC_EXECUCAO START WITH 1 INCREMENT BY 1;
/
CREATE OR REPLACE TRIGGER TRG_BI_SEMAFORO_PROC_EXECUCAO
  BEFORE INSERT ON SEMAFORO_PROC_EXECUCAO
  FOR EACH ROW
BEGIN
  :NEW.CD_ID := SEQ_SEMAFORO_PROC_EXECUCAO.nextval;
END;
/
CREATE OR REPLACE TRIGGER TRG_BU_SEMAFORO_PROC_EXECUCAO
  BEFORE UPDATE ON SEMAFORO_PROC_EXECUCAO
  FOR EACH ROW
BEGIN
  :NEW.DT_UPDATED := SYSTIMESTAMP;
END;
/
CREATE TABLE LOG_LEVEL(
  CD_ID NUMBER NOT NULL PRIMARY KEY,
  NAME VARCHAR2(30)
);
INSERT ALL
  INTO LOG_LEVEL (CD_ID, NAME) VALUES (0, 'FATAL')
  INTO LOG_LEVEL (CD_ID, NAME) VALUES (1, 'ERROR')
  INTO LOG_LEVEL (CD_ID, NAME) VALUES (2, 'WARNING')
  INTO LOG_LEVEL (CD_ID, NAME) VALUES (3, 'INFO')
  INTO LOG_LEVEL (CD_ID, NAME) VALUES (4, 'DEBUG')
SELECT 1 FROM DUAL;
COMMIT;
/
CREATE TABLE LOG_PROC_EXECUCAO (
  CD_SEMAFORO NUMBER REFERENCES SEMAFORO_PROC_EXECUCAO(CD_ID) ON DELETE CASCADE,
  DT_CREATED TIMESTAMP DEFAULT SYSTIMESTAMP,
  LOG_LEVEL NUMBER REFERENCES LOG_LEVEL (CD_ID),
  SQLCODE NUMBER,
  SQLERRM VARCHAR2(500),
  DETAILS CLOB,
  CONSTRAINT LOG_PROC_EXECUCAO_PK PRIMARY KEY (CD_SEMAFORO, LOG_LEVEL, DT_CREATED)
);
/
CREATE INDEX IDX_LOG_PROC_SEMAFORO ON LOG_PROC_EXECUCAO(CD_SEMAFORO);
/
CREATE INDEX IDX_LOG_PROC_LEVEL ON LOG_PROC_EXECUCAO(LOG_LEVEL);
/
CREATE OR REPLACE TYPE LOGGER AS OBJECT (
  v_semaforo_id NUMBER,
  v_source_name VARCHAR2(128),
  v_source_type VARCHAR2(12),
  v_proc_name   VARCHAR2(128),
  v_proc_args   VARCHAR2(4000),
  member procedure log_with_stack_trace(p_sqlCode NUMBER, p_sqlErrM VARCHAR2, message CLOB DEFAULT '', log_level NUMBER DEFAULT 0),
  member procedure fatal(p_sqlCode NUMBER, p_sqlErrM VARCHAR2, message CLOB DEFAULT ''),
  member procedure error(p_sqlCode NUMBER, p_sqlErrM VARCHAR2, message CLOB DEFAULT ''),
  member procedure warning(message CLOB),
  member procedure info(message CLOB),
  member procedure debug(message CLOB),
  member procedure endRun
);
/
CREATE OR REPLACE TYPE BODY LOGGER AS
  member procedure log_with_stack_trace(p_sqlCode NUMBER, p_sqlErrM VARCHAR2, message CLOB DEFAULT '', log_level NUMBER DEFAULT 0) is
    the_message CLOB;
    v_line NUMBER;
  begin
    SELECT TO_NUMBER(REGEXP_SUBSTR(
      DBMS_UTILITY.FORMAT_ERROR_BACKTRACE()
      , 'line.(\d+)'
      , 1, 1, NULL, 1
    )) into v_line FROM DUAL
    FETCH FIRST 1 ROWS ONLY;

    SELECT LISTAGG(LINE_TEXT, '\n') into the_message FROM (
      SELECT CASE LINE
        WHEN v_line THEN '=======> ' || LINE ||  ' | ' || TEXT
        ELSE LINE ||  ' | ' || TEXT END LINE_TEXT
      FROM SYS.USER_SOURCE
      WHERE NAME = v_source_name
        AND SYS.USER_SOURCE.TYPE = v_source_type
        AND LINE BETWEEN (v_line - 10) AND (v_line + 10)
      ORDER BY LINE
    );
    the_message := message || (chr(10)) || the_message;
    INSERT INTO LOG_PROC_EXECUCAO(CD_SEMAFORO, LOG_LEVEL, SQLCODE, SQLERRM, DETAILS) VALUES (v_semaforo_id, log_level, p_sqlCode, p_sqlErrM, the_message);
  end;
  member procedure fatal(p_sqlCode NUMBER, p_sqlErrM VARCHAR2, message CLOB DEFAULT '') is
  begin
    log_with_stack_trace(p_sqlCode, p_sqlErrM, message, 0);
    UPDATE SEMAFORO_PROC_EXECUCAO SET HAS_ERROS = 1 WHERE CD_ID = v_semaforo_id;
  end;
  member procedure error(p_sqlCode NUMBER, p_sqlErrM VARCHAR2, message CLOB DEFAULT '') is
  begin
    log_with_stack_trace(p_sqlCode, p_sqlErrM, message, 1);
    UPDATE SEMAFORO_PROC_EXECUCAO SET HAS_ERROS = 1 WHERE CD_ID = v_semaforo_id;
  end;
  member procedure warning(message CLOB) is
  begin
    INSERT INTO LOG_PROC_EXECUCAO(CD_SEMAFORO, LOG_LEVEL, DETAILS) VALUES (v_semaforo_id, 2, message);
  end;
  member procedure info(message CLOB) is
  begin
    INSERT INTO LOG_PROC_EXECUCAO(CD_SEMAFORO, LOG_LEVEL, DETAILS) VALUES (v_semaforo_id, 3, message);
  end;
  member procedure debug(message CLOB) is
  begin
    INSERT INTO LOG_PROC_EXECUCAO(CD_SEMAFORO, LOG_LEVEL, DETAILS) VALUES (v_semaforo_id, 4, message);
  end;
  member procedure endRun is
  begin
    info('FINISHED');
    UPDATE SEMAFORO_PROC_EXECUCAO SET RUNNING = 0 WHERE CD_ID = v_semaforo_id;
  end;
END;
/
CREATE OR REPLACE FUNCTION GET_LOGGER(v_proc_name VARCHAR2, v_proc_args VARCHAR2, v_source_name VARCHAR2 DEFAULT NULL, v_source_type VARCHAR2 DEFAULT 'PROCEDURE')
  RETURN LOGGER
IS
  v_semaforo_id NUMBER;
BEGIN
  INSERT INTO SEMAFORO_PROC_EXECUCAO(PROC_NAME, ARGS, RUNNING, HAS_ERROS) VALUES (v_proc_name, v_proc_args, 1, 0) RETURNING CD_ID INTO v_semaforo_id;
  RETURN LOGGER(
    v_semaforo_id,
    NVL(v_source_name, v_proc_name),
    v_source_type,
    v_proc_name,
    v_proc_args
  );
END;
/
CREATE TABLE CQN_QUERY_EVENT_TYPES(
  ID NUMBER NOT NULL PRIMARY KEY,
  DESCRIPTION VARCHAR2(255)
);
/
INSERT ALL
  INTO CQN_QUERY_EVENT_TYPES (ID, DESCRIPTION) VALUES (0 , 'OCI_EVENT_NONE - No further information about the continuous query notification')
  INTO CQN_QUERY_EVENT_TYPES (ID, DESCRIPTION) VALUES (1 , 'OCI_EVENT_STARTUP - Instance startup')
  INTO CQN_QUERY_EVENT_TYPES (ID, DESCRIPTION) VALUES (2 , 'OCI_EVENT_SHUTDOWN - Instance shutdown')
  INTO CQN_QUERY_EVENT_TYPES (ID, DESCRIPTION) VALUES (3 , 'OCI_EVENT_SHUTDOWN_ANY - Any instance shutdown - Oracle Real Application Clusters (Oracle RAC)')
  INTO CQN_QUERY_EVENT_TYPES (ID, DESCRIPTION) VALUES (5 , 'OCI_EVENT_DEREG - Unregistered or timed out')
  INTO CQN_QUERY_EVENT_TYPES (ID, DESCRIPTION) VALUES (6 , 'OCI_EVENT_OBJCHANGE - Object change notification')
  INTO CQN_QUERY_EVENT_TYPES (ID, DESCRIPTION) VALUES (7 , 'OCI_EVENT_QUERYCHANGE - Query change notification')
SELECT 1 FROM DUAL;
/
CREATE TABLE CQN_QUERY_OPERATION_TYPE(
  ID NUMBER NOT NULL PRIMARY KEY,
  DESCRIPTION VARCHAR2(255)
);
/
INSERT ALL
INTO CQN_QUERY_OPERATION_TYPE (ID, DESCRIPTION) VALUES (1 , 'OCI_OPCODE_ALLROWS - The table is completely invalidated.')
INTO CQN_QUERY_OPERATION_TYPE (ID, DESCRIPTION) VALUES (2 , 'OCI_OPCODE_INSERT - Insert operations on the table.')
INTO CQN_QUERY_OPERATION_TYPE (ID, DESCRIPTION) VALUES (4 , 'OCI_OPCODE_UPDATE - Update operations on the table.')
INTO CQN_QUERY_OPERATION_TYPE (ID, DESCRIPTION) VALUES (8 , 'OCI_OPCODE_DELETE - Delete operations on the table.')
INTO CQN_QUERY_OPERATION_TYPE (ID, DESCRIPTION) VALUES (10, 'OCI_OPCODE_ALTER - Table altered (schema change). This includes DDL statements and internal operations that cause row migration.')
INTO CQN_QUERY_OPERATION_TYPE (ID, DESCRIPTION) VALUES (20, 'OCI_OPCODE_DROP - Table dropped.')
SELECT 1 FROM DUAL;
COMMIT;
/
-- Create table to record notification events.
CREATE TABLE CQN_EVENTS (
  REGISTRATION_ID NUMBER NOT NULL,
  DT_CREATED      TIMESTAMP DEFAULT SYSTIMESTAMP,
  CONSTRAINT PK_CQN_EVENTS PRIMARY KEY (REGISTRATION_ID, DT_CREATED)
);
-- Create table to record notification queries:
/
CREATE TABLE CQN_QUERIES (
  REGISTRATION_ID NUMBER    NOT NULL,
  DT_CREATED      TIMESTAMP NOT NULL,
  EVENT_TYPE      NUMBER    NOT NULL,
  QUERY_ID        NUMBER    NOT NULL,
  CONSTRAINT FK_CQN_QUERIES FOREIGN KEY (REGISTRATION_ID, DT_CREATED)
    REFERENCES CQN_EVENTS (REGISTRATION_ID, DT_CREATED)
    ON DELETE CASCADE,
  CONSTRAINT PK_CQN_QUERIES PRIMARY KEY (REGISTRATION_ID, DT_CREATED, EVENT_TYPE, QUERY_ID)
);
/
CREATE INDEX IDX_FK_QUERIES_EVENTS ON CQN_QUERIES (REGISTRATION_ID, DT_CREATED, EVENT_TYPE);
-- Create table to record changes to registered tables:
/
CREATE TABLE CQN_TABLES_CHANGED (
  REGISTRATION_ID NUMBER    NOT NULL,
  DT_CREATED      TIMESTAMP NOT NULL,
  EVENT_TYPE      NUMBER    NOT NULL,
  QUERY_ID        NUMBER    NOT NULL,
  TABLE_NAME      VARCHAR2(100),

  CONSTRAINT FK_CQN_TABLES_CHANGED FOREIGN KEY (REGISTRATION_ID, DT_CREATED, EVENT_TYPE, QUERY_ID)
  REFERENCES CQN_QUERIES (REGISTRATION_ID, DT_CREATED, EVENT_TYPE, QUERY_ID)
  ON DELETE CASCADE,
  CONSTRAINT PK_CQN_TABLES_CHANGED PRIMARY KEY (REGISTRATION_ID, DT_CREATED, EVENT_TYPE, QUERY_ID, TABLE_NAME)
);
/
CREATE INDEX IDX_FK_TABLES_QUERIES ON CQN_TABLES_CHANGED (REGISTRATION_ID, DT_CREATED, EVENT_TYPE, QUERY_ID);
-- Create table to record ROWIDs of changed rows:
/
CREATE TABLE CQN_ROWS_CHANGED (
  REGISTRATION_ID NUMBER    NOT NULL,
  DT_CREATED      TIMESTAMP NOT NULL,
  EVENT_TYPE      NUMBER    NOT NULL,
  QUERY_ID        NUMBER    NOT NULL,
  TABLE_NAME      VARCHAR2(100),

  TABLE_OPERATION NUMBER    NOT NULL,
  ROW_ID          ROWID,
  CONSTRAINT FK_CQN_ROWS_CHANGED FOREIGN KEY (REGISTRATION_ID, DT_CREATED, EVENT_TYPE, QUERY_ID, TABLE_NAME)
  REFERENCES CQN_TABLES_CHANGED (REGISTRATION_ID, DT_CREATED, EVENT_TYPE, QUERY_ID, TABLE_NAME)
  ON DELETE CASCADE,
  CONSTRAINT PK_CQN_ROWS_CHANGED PRIMARY KEY (REGISTRATION_ID, DT_CREATED, EVENT_TYPE, QUERY_ID, TABLE_NAME)
);
/
CREATE OR REPLACE PROCEDURE chnf_callback(ntfnds IN CQ_NOTIFICATION$_DESCRIPTOR)
IS

  v_number_of_queries NUMBER;
  v_number_of_tables  NUMBER;
  v_number_of_rows    NUMBER;

  v_registration_id   NUMBER;
  v_event_type        NUMBER;
  v_event_date        TIMESTAMP;
  v_query_id          NUMBER;
  v_query_operation   NUMBER;
  v_table_name        VARCHAR2(60);
  v_operation_type    NUMBER;
  v_row_id ROWID;
  v_logger LOGGER;
  BEGIN
    v_logger := GET_LOGGER('chnf_callback', null);
    BEGIN
      v_logger.INFO('Recebeu notificacao de mudanca');
      v_registration_id := ntfnds.registration_id;
      v_event_type := ntfnds.event_type;
      INSERT INTO CQN_EVENTS (REGISTRATION_ID)
      VALUES (v_registration_id)
      RETURNING DT_CREATED INTO v_event_date;

      v_number_of_queries := 0;
      IF (v_event_type = DBMS_CQ_NOTIFICATION.EVENT_QUERYCHANGE)
      THEN
        v_number_of_queries := ntfnds.query_desc_array.count;
        FOR i IN 1..v_number_of_queries LOOP -- loop over queries
          v_query_id := ntfnds.query_desc_array(i).queryid;
          v_query_operation := ntfnds.query_desc_array(i).queryop;
          INSERT INTO CQN_QUERIES (REGISTRATION_ID, DT_CREATED, EVENT_TYPE, QUERY_ID)
          VALUES (v_registration_id, v_event_date, v_query_operation, v_query_id);
          v_number_of_tables := 0;
          v_number_of_tables := ntfnds.query_desc_array(i).table_desc_array.count;
          FOR j IN 1..v_number_of_tables LOOP -- loop over tables
            v_table_name := ntfnds.query_desc_array(i).table_desc_array(j).table_name;
            v_operation_type := ntfnds.query_desc_array(i).table_desc_array(j).opflags;
            INSERT INTO CQN_TABLES_CHANGED (REGISTRATION_ID, DT_CREATED, EVENT_TYPE, QUERY_ID, TABLE_NAME)
            VALUES (v_registration_id, v_event_date, v_query_operation, v_query_id, v_table_name);
            --verifies if rowid information is available
            IF (bitand(v_operation_type, DBMS_CQ_NOTIFICATION.ALL_ROWS) = 0)
            THEN
              v_number_of_rows := ntfnds.query_desc_array(i).table_desc_array(j).numrows;
            ELSE
              v_number_of_rows := 0; -- ROWID info not available
            END IF;
            -- Body of loop does not run when numrows is zero.
            FOR k IN 1..v_number_of_rows LOOP -- loop over rows
              v_operation_type := ntfnds.query_desc_array(i).table_desc_array(j).row_desc_array(k).opflags;
              v_row_id := ntfnds.query_desc_array(i).table_desc_array(j).row_desc_array(k).row_id;
              INSERT INTO CQN_ROWS_CHANGED (REGISTRATION_ID, DT_CREATED, EVENT_TYPE, QUERY_ID, TABLE_NAME, TABLE_OPERATION, ROW_ID)
              VALUES (v_registration_id, v_event_date, v_query_operation, v_query_id, v_table_name, v_operation_type, v_row_id);
            END LOOP; -- loop over rows
          END LOOP; -- loop over tables
        END LOOP; -- loop over queries
      END IF;
    EXCEPTION WHEN OTHERS THEN
      v_logger.FATAL(SQLCODE, SQLERRM, 'FATAL ERROR');
    END;
    v_logger.ENDRUN();
    COMMIT;
  END;

--------------------------------------------------------------------------------------
-----------------------------EXEMPLOS DE UTILIZACAO-----------------------------------
--------------------------------------------------------------------------------------
/*
# BLOCO ANONIMO PARA SE REGISTRAR E COMECAR A OBSERVAR UMA DETERMINADA QUERY
# CADA CHAMADA DO BLOCO ANONIMO CRIA UM REGISTRO DE NOTIFICAO
# DENTRO DE UM REGISTRO DE NOTIFICAO PODEM ESTAR VARIAS QUERIES

DECLARE
  reginfo  CQ_NOTIFICATION$_REG_INFO;
  mgr_id   NUMBER;
  dept_id  NUMBER;
  v_cursor SYS_REFCURSOR;
  regid    NUMBER;
BEGIN
  -- Register two queries for QRNC:
  -- 1. Construct registration information.
  -- chnf_callback is name of notification handler.
  -- QOS_QUERY specifies result-set-change notifications.
  reginfo := cq_notification$_reg_info(
      'chnf_callback', -- NOME DA PROCEDURE DE CALLBACK QUE RECEBERA AS NOTIFICACOES MUDANCA DAS QUERIES REGISTRADAS
      DBMS_CQ_NOTIFICATION.QOS_QUERY + DBMS_CQ_NOTIFICATION.QOS_ROWIDS, -- QUAIS OPERACAOES DA TABELA DESEJA OBSERVAR
      0, -- TEMPO DE VIDA DO REGISTRO DE NOTIFICACAO (0 SIGNIFICA INFINITO )
      DBMS_CQ_NOTIFICATION.INSERTOP + DBMS_CQ_NOTIFICATION.UPDATEOP + DBMS_CQ_NOTIFICATION.DELETEOP, -- operations_filter
      0 -- DELAY DE NOTIFICAO ( 0 SIGNIFICA NOTIFICACOES IMEDIATAS )
  );
  -- Comeca a registrar as queries.
  regid := DBMS_CQ_NOTIFICATION.NEW_REG_START(reginfo);
  --Query 1
  OPEN v_cursor FOR
  SELECT x, y, z FROM table_alpha;
  CLOSE v_cursor;
  --Query 2
  OPEN v_cursor FOR
  SELECT a, b, c FROM table_beta;
  CLOSE v_cursor;
  --Query n ...
  --Fim do registro de queries e confirmacao do registro de notificao
  DBMS_CQ_NOTIFICATION.REG_END();
END;

--Onde verificar os registros existentes
SELECT * FROM USER_CHANGE_NOTIFICATION_REGS;
--Como deletar um registro de notificao
CALL DBMS_CQ_NOTIFICATION.DEREGISTER(603);
*/