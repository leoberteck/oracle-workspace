CREATE OR REPLACE PACKAGE SGPA_INTEGRACAO_UTILS AS
  /*
  Procedures que tratam de permitir integracao de dados
  com uma tabela oficial do SGPA. Procedure funciona da seguinte
  maneira:
  1- Baseado no parametro @p_table_name, gera a DDL necessaria e
  verifica se a tabela prestes a ser criada ja existe realizando
  uma query na tabela USER_TABLES. Se a tabela ja existir, aborta a
  execucao sem disparar nenhuma exception.
  2 - Se a tabela a ser criada ainda nao existir, cria ela bem como
  a sequence e a trigger necessaria para o autoincrement da chave
  primaria.
  3 - Ao final atualiza a CFG_CONSUMER_API da seguinte maneira:
    a - Se for o primeiro overload sendo chamado, procura o registro
    de configuracao existente no consumer_api e o atualiza setando
    o nome da tabela de staging e setando a flag de push para 1
    b - Se for o segundo overload Insere um novo registro com os
    dados fornecidos.
  */
  PROCEDURE ENABLE_STAGING_ON_TABLE(p_table_name VARCHAR2);
  PROCEDURE ENABLE_STAGING_ON_TABLE(p_table_name VARCHAR2, p_campos VARCHAR2, p_condicional VARCHAR2);
  PROCEDURE PROC_CONVERT_CONSUMER_API;
END SGPA_INTEGRACAO_UTILS;

CREATE OR REPLACE PACKAGE BODY SGPA_INTEGRACAO_UTILS AS
  PROCEDURE ENABLE_STAGING_ON_TABLE(p_table_name VARCHAR2) IS

    m_ddp_metadata varchar2(10000);
    m_staging_table varchar2(30);
    m_cfg CFG_CONSUMER_API%ROWTYPE;
  BEGIN
    SELECT * INTO m_cfg from CFG_CONSUMER_API cca WHERE cca.TABELA = p_table_name;
    m_staging_table := p_table_name;
    m_ddp_metadata := SGPA_METADATA_UTILS.GENERATE_DDP_TABLE_DDL(m_staging_table);
    SGPA_METADATA_UTILS.EXEC_MULTI_STATEMENT_SCRIPT(m_ddp_metadata);
    UPDATE CFG_CONSUMER_API SET STAGING_TABLE = m_staging_table, CAN_PUSH_DATA = 1 WHERE CD_ID = m_cfg.CD_ID;
  EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Nao foi possivel configurar a tabela para integracao - '||SQLCODE||' -ERROR- '||SQLERRM);
  END ENABLE_STAGING_ON_TABLE;

  PROCEDURE ENABLE_STAGING_ON_TABLE(p_table_name VARCHAR2, p_campos VARCHAR2, p_condicional VARCHAR2) IS
      m_ddp_metadata varchar2(10000);
      m_staging_table varchar2(30);
      CURSOR cr_find_table (p_table_name VARCHAR2) IS SELECT TABLE_NAME FROM USER_TABLES ut WHERE ut.TABLE_NAME = p_table_name;
      m_found_table cr_find_table%ROWTYPE;
    BEGIN
      m_staging_table := p_table_name;
      m_ddp_metadata := SGPA_METADATA_UTILS.GENERATE_DDP_TABLE_DDL(m_staging_table);
      OPEN cr_find_table(m_staging_table);
      FETCH cr_find_table INTO m_found_table;
      IF cr_find_table%NOTFOUND THEN
        SGPA_METADATA_UTILS.EXEC_MULTI_STATEMENT_SCRIPT(m_ddp_metadata);
        INSERT INTO CFG_CONSUMER_API(TABELA, CAMPO, CONDICIONAL, STAGING_TABLE, CAN_PULL_DATA, CAN_PUSH_DATA) VALUES (p_table_name, p_campos, p_condicional, m_staging_table, 1, 1);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Tabela preparada para integracao com sucesso!');
      ELSE
        DBMS_OUTPUT.PUT_LINE('Table ja existe. Ignorando comandos de DDL...');
      END IF;
    EXCEPTION
        WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('Nao foi possivel configurar a tabela para integracao - '||SQLCODE||' -ERROR- '||SQLERRM);
          ROLLBACK;
    END ENABLE_STAGING_ON_TABLE;
  PROCEDURE PROC_CONVERT_CONSUMER_API IS
      CURSOR CR_CONSUMER_API IS
        SELECT * FROM CFG_CONSUMER_API;
      CONSUMER CR_CONSUMER_API%ROWTYPE;
      OWNER VARCHAR2(30);
    BEGIN
      SELECT USER INTO OWNER FROM DUAL;
      OPEN CR_CONSUMER_API;
      LOOP
        FETCH CR_CONSUMER_API INTO CONSUMER;
        EXIT WHEN CR_CONSUMER_API%NOTFOUND;
        SELECT REPLACE(CONSUMER.CONDICIONAL, 'WHERE', '') INTO CONSUMER.CONDICIONAL FROM DUAL;
        INSERT INTO CFG_PULL_INTEGRATION (IDENTIFICACAO, QUERY)
          VALUES (CONSUMER.TABELA
            , 'SELECT ' || CONSUMER.CAMPO || ' FROM ' || OWNER || '.' || CONSUMER.TABELA || ' WHERE ' || CONSUMER.CONDICIONAL);
      END LOOP;
      COMMIT;
      CLOSE CR_CONSUMER_API;
    END PROC_CONVERT_CONSUMER_API;
END SGPA_INTEGRACAO_UTILS;