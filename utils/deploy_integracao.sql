--owner sgpa
/*
  Glossario de logs de integracao
  NIVEIS DE LOG
  0 - Erro critico nao conhecido
  1 - Erro no dado.
  2 - Erro causado pelo banco
  3 - Erro causado por falta de configuracao do sistema SGPA
  4 - Alerta : nao prejudica a execucao da funcao, mas nao e comportamento ideal do sistema
  5 - Info : Informativo apenas.
  6 - Debug : Informacoes que facilitam o debug da aplicacao
*/

CREATE TABLE CDT_LOG(
  CD_ID NUMBER(11, 0) NOT NULL PRIMARY KEY
  , TITULO VARCHAR2(500) NOT NULL
  , MENSAGEM VARCHAR2(4000) DEFAULT NULL
  , NIVEL NUMBER(1, 0) DEFAULT 0 NOT NULL CHECK (NIVEL IN (0,1,2,3,4,5,6))
);
/
INSERT ALL
  INTO CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (1, 'SGPA_INTEGRATION', 'O dado nao possui mapeamento', 3)
  INTO CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (2, 'SGPA_INTEGRATION', 'O dado nao pode ser inserido na tabela de destino', 2)
  INTO CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (3, 'SGPA_INTEGRATION', 'A tabela de entrada nao pode ser atualizada', 2)
  INTO CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (4, 'SGPA_INTEGRATION', 'A integracao foi concluida com sucesso para um grupo de tuplas', 5)
  INTO CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (5, 'SGPA_INTEGRATION', 'Foram deletadas tuplas processadas da tabela de entrada', 5)
  INTO CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (6, 'COMMON', 'Nao foi possivel concluir o procedimento devido a um erro interno no banco de dados', 1)
SELECT * FROM DUAL;
/
COMMIT;
/
--Instancia de integraçao
CREATE TABLE DDP_DADOSINTEGRACAO(
  CD_ID NUMBER(11,0) NOT NULL PRIMARY KEY
  , VERSAO_DADOS VARCHAR2(50) NOT NULL
  , TABELA_DESTINO VARCHAR2(100) NOT NULL
  , DT_INSERIDO TIMESTAMP DEFAULT SYSDATE
  , VL_VALORES_ENTRADA CLOB NOT NULL
  , VL_COLUNAS_ENTRADA VARCHAR2(4000) NOT NULL
  , CONSTRAINT UN_DADOSINTEGRACAO UNIQUE (CD_ID, VERSAO_DADOS, TABELA_DESTINO, DT_INSERIDO)
    USING INDEX (CREATE UNIQUE INDEX IDX_DADOSINTEGRACAO ON DDP_DADOSINTEGRACAO(CD_ID, VERSAO_DADOS, TABELA_DESTINO, DT_INSERIDO))
);
/
CREATE INDEX IDX_DADOSINTEGRACAO_SEARCH ON DDP_DADOSINTEGRACAO (VERSAO_DADOS, TABELA_DESTINO, DT_INSERIDO);
/
CREATE TABLE DDP_DADOSINTEGRACAO_BK(
  CD_ID NUMBER(11,0) NOT NULL PRIMARY KEY
  , VERSAO_DADOS VARCHAR2(50) NOT NULL
  , TABELA_DESTINO VARCHAR2(100) NOT NULL
  , DT_INSERIDO TIMESTAMP DEFAULT SYSDATE
  , VL_VALORES_ENTRADA CLOB NOT NULL
  , VL_COLUNAS_ENTRADA VARCHAR2(4000) NOT NULL
  , CONSTRAINT UN_DADOSINTEGRACAO_BK UNIQUE (CD_ID, VERSAO_DADOS, TABELA_DESTINO, DT_INSERIDO)
    USING INDEX (CREATE UNIQUE INDEX IDX_DADOSINTEGRACAO_BK ON DDP_DADOSINTEGRACAO_BK(CD_ID, VERSAO_DADOS, TABELA_DESTINO, DT_INSERIDO))
);
/
CREATE INDEX IDX_DADOSINTEGRACAO_BK_SEARCH ON DDP_DADOSINTEGRACAO_BK (VERSAO_DADOS, TABELA_DESTINO, DT_INSERIDO);
/
CREATE SEQUENCE SEQ_PK_DADOSINTEGRACAO START WITH 1 INCREMENT BY 1;
/
CREATE OR REPLACE TRIGGER TRG_BI_DADOSINTEGRACAO_PK
  BEFORE INSERT ON DDP_DADOSINTEGRACAO FOR EACH ROW
DECLARE
BEGIN
  :NEW.CD_ID := SEQ_PK_DADOSINTEGRACAO.nextval;
END;
/
CREATE OR REPLACE TRIGGER TRG_AI_DADOSINTEGRACAO
  AFTER INSERT ON DDP_DADOSINTEGRACAO FOR EACH ROW
DECLARE
BEGIN
  INSERT INTO DDP_DADOSINTEGRACAO_BK (CD_ID, VERSAO_DADOS, TABELA_DESTINO, VL_COLUNAS_ENTRADA, VL_VALORES_ENTRADA)
    VALUES (:NEW.CD_ID, :NEW.VERSAO_DADOS, :NEW.TABELA_DESTINO, :NEW.VL_COLUNAS_ENTRADA, :NEW.VL_VALORES_ENTRADA);
END;
/
--Owner do cliente do cliente
/
CREATE TABLE CFG_VERSAO_INTEGRACAO(
  VERSAO_DADOS VARCHAR2(50) NOT NULL
  , TABELA_DESTINO VARCHAR2(30) NOT NULL
  , CONSTRAINT PK_VERSAO_INTEGRACAO PRIMARY KEY (VERSAO_DADOS, TABELA_DESTINO)
);
/
CREATE TABLE CFG_DEPARA_INTEGRACAO(
  VERSAO_DADOS VARCHAR2(50) NOT NULL
  , TABELA_DESTINO VARCHAR2(30) NOT NULL
  , COLUNA_ENTRADA VARCHAR2(255) NOT NULL
  , COLUNA_SAIDA VARCHAR2(30) NOT NULL
  , CONSTRAINT PK_DEPARA_INTEGRACAO PRIMARY KEY (VERSAO_DADOS, TABELA_DESTINO, COLUNA_ENTRADA, COLUNA_SAIDA)
  , CONSTRAINT FK_DEPARA_INTEGRACAO
    FOREIGN KEY(VERSAO_DADOS, TABELA_DESTINO)
    REFERENCES CFG_VERSAO_INTEGRACAO(VERSAO_DADOS, TABELA_DESTINO)
);
/
CREATE TABLE LOG_INTEGRACAO_HIST(
  CD_ID NUMBER(11, 0) NOT NULL PRIMARY KEY
  , CD_LOG NUMBER(11, 0) NOT NULL
  , CD_DADOSINTEGRACAO NUMBER(11, 0) NULL
  , DT_LOG TIMESTAMP DEFAULT SYSDATE NOT NULL
  , DETALHES CLOB NULL
);
/
CREATE INDEX IDX_LOG_INTEGRACAO_SEARCH ON LOG_INTEGRACAO_HIST (CD_LOG, DT_LOG);
/
CREATE SEQUENCE SEQ_PK_LOG_INTEGRACAO START WITH 1 INCREMENT BY 1;
/
CREATE OR REPLACE TRIGGER TRG_BI_LOG_INTEGRACAO_PK
  BEFORE INSERT ON LOG_INTEGRACAO_HIST FOR EACH ROW
DECLARE
BEGIN
  :NEW.CD_ID := SEQ_PK_LOG_INTEGRACAO.nextval;
END;
/
CREATE TABLE CFG_PULL_INTEGRATION(
  CD_ID NUMBER(11, 0) NOT NULL PRIMARY KEY
  , IDENTIFICACAO VARCHAR2(100)
  , ENABLED NUMBER(1,0) DEFAULT 1
  , QUERY CLOB
  , DT_CREATED TIMESTAMP DEFAULT SYSDATE
  , DT_LAST_UPDATE TIMESTAMP DEFAULT SYSDATE
);
/
CREATE SEQUENCE SEQ_PULL_INTEGRATION_PK START WITH 1 INCREMENT BY 1;
/
CREATE OR REPLACE TRIGGER TRG_BU_PULL_INTEGRATION
  BEFORE UPDATE ON CFG_PULL_INTEGRATION FOR EACH ROW
BEGIN
  :NEW.DT_LAST_UPDATE := SYSDATE;
END;
/
CREATE OR REPLACE TRIGGER TRG_BI_PULL_INTEGRATION
  BEFORE INSERT ON CFG_PULL_INTEGRATION FOR EACH ROW
BEGIN
  :NEW.CD_ID := SEQ_PULL_INTEGRATION_PK.NEXTVAL;
END;
/
CREATE TABLE CFG_PUSH_INTEGRATION(
  CD_ID NUMBER(11, 0) NOT NULL PRIMARY KEY
  , IDENTIFICACAO VARCHAR2(100)
  , STAGING_TABLE VARCHAR2(30)
  , ENABLED NUMBER(1,0) DEFAULT 1
  , DT_CREATED TIMESTAMP DEFAULT SYSDATE
  , DT_LAST_UPDATE TIMESTAMP DEFAULT SYSDATE
);
/
CREATE SEQUENCE SEQ_PUSH_INTEGRATION_PK START WITH 1 INCREMENT BY 1;
/
CREATE OR REPLACE TRIGGER TRG_BU_PUSH_INTEGRATION
  BEFORE UPDATE ON CFG_PUSH_INTEGRATION FOR EACH ROW
BEGIN
  :NEW.DT_LAST_UPDATE := SYSDATE;
END;
/
CREATE OR REPLACE TRIGGER TRG_BI_PUSH_INTEGRATION
  BEFORE INSERT ON CFG_PUSH_INTEGRATION FOR EACH ROW
BEGIN
  :NEW.CD_ID := SEQ_PUSH_INTEGRATION_PK.NEXTVAL;
END;
/
CREATE TABLE CFG_DEPARA_VALOR_INTEGRACAO (
  ID NUMBER NOT NULL PRIMARY KEY
  , TABELA_DESTINO VARCHAR2(30) NOT NULL
  , COLUNA_ENTRADA VARCHAR2(255) NOT NULL
  , VALOR_ENTRADA VARCHAR2(4000) NOT NULL
  , VALOR_SAIDA VARCHAR2(4000) NOT NULL
  , CONSTRAINT UN_DEPARA_VALOR_INTEGRACAO UNIQUE (TABELA_DESTINO, COLUNA_ENTRADA, VALOR_ENTRADA)
    USING INDEX (CREATE INDEX IDX_UN_DEPARA_VALOR_INT ON CFG_DEPARA_VALOR_INTEGRACAO(TABELA_DESTINO, COLUNA_ENTRADA, VALOR_ENTRADA))
);
/
CREATE SEQUENCE SEQ_DEPARA_VALOR_INTEGRACAO START WITH 1 INCREMENT BY 1;
/
CREATE OR REPLACE TRIGGER TRG_BI_DEPARA_VALOR_INT
  BEFORE INSERT ON CFG_DEPARA_VALOR_INTEGRACAO
  FOR EACH ROW
BEGIN
  :NEW.ID := SEQ_DEPARA_VALOR_INTEGRACAO.nextval;
END;
/
CREATE TABLE CFG_SEMAFORO_PROCESSAMENTO
(
  CD_ID               NUMBER(20) not null
    constraint PK_CFG_SEMAFORO_PROCESAMENTO
    primary key,
  DESC_NOME_PRC       VARCHAR2(50),
  FG_PRC_ATIVO        VARCHAR2(1),
  DESC_COMENTARIO     VARCHAR2(100),
  DT_HR_EXECUCAO_PROC DATE,
  VL_TEMPO_EXECUCAO   NUMBER(5),
  CD_TP_PRC           NUMBER(20)
);
/
CREATE OR REPLACE PACKAGE SGPA_INTEGRACAO_PROCESSADOR AS
    /*
      Quando ativada, esta procedure percorre toda a tabela DDP_DADOSINTEGRACAO
      e faz um fetch de 1000 em 1000 no cursor. Para cada dado processado do cursor
      sao realizadas as seguintes tarefas:
        1 - Sao buscados os dados de mapeamento baseado na versao e tabela destino
        informados no dado quanado inserido.
        2 - As colunas de entrada sao mapeadas para as colunas de saida.
        3 - E montado e executado um insert da tabela de destino.
        4 - E montado e executado um update para a tupla processada.
      Nao possui parametros
      Fluxos alternativos:
        Durante a execucao desta procedure podem ocorrer os seguintes erros
        que serao classificados e inseridos na tabela de LOG_INTEGRACAO:
        [SGPA_INTEGRATION 1]SELECT  FROM DUAL
        [SGPA_INTEGRATION 2]
        [SGPA_INTEGRATION 3]
        [SGPA_INTEGRATION 4]
    */
  PROCEDURE PROC_INTEGRAR_DADOS;
  /*
    Procedure que deleta dados da tabela DPP_DADOSINTEGRACAO_BK
    em batch e mantem apenas dados inseridos dentro do periodo
    marcado pelo ultimos n dias informados no parametro 'dias'
    da procedure.
    @Param dias: ultimos n dias a manter na base. Dados mais antigos
    do que n dias serao apagados.
  */
  PROCEDURE PROC_DELETAR_ANTIGOS(dias NUMBER);
  PROCEDURE PROC_DELETAR_LOG_ANTIGO(dias NUMBER);
END SGPA_INTEGRACAO_PROCESSADOR;
/
CREATE OR REPLACE PACKAGE BODY SGPA_INTEGRACAO_PROCESSADOR AS

  PROCEDURE PROC_INTEGRAR_DADOS IS
    CURSOR cr_estrutura_nao_processado IS
      select a.VERSAO_DADOS
        , a.TABELA_DESTINO
        , MAX(a.VL_COLUNAS_ENTRADA) AS VL_COLUNAS_ENTRADA
      from DDP_DADOSINTEGRACAO a GROUP BY VERSAO_DADOS, TABELA_DESTINO;

    CURSOR cr_tuplas_nao_processadas(versaoDados VARCHAR2, tabelaDestino VARCHAR2) IS
      select CD_ID, TABELA_DESTINO, VL_VALORES_ENTRADA from DDP_DADOSINTEGRACAO WHERE TABELA_DESTINO = tabelaDestino AND VERSAO_DADOS = versaoDados;

    CURSOR cr_mapeamento (versaoDados VARCHAR2, tabelaDestino VARCHAR2) IS
      SELECT cdi.COLUNA_ENTRADA, cdi.COLUNA_SAIDA FROM CFG_VERSAO_INTEGRACAO cvi
        JOIN CFG_DEPARA_INTEGRACAO cdi on cvi.VERSAO_DADOS = cdi.VERSAO_DADOS and cvi.TABELA_DESTINO = cdi.TABELA_DESTINO
        WHERE cvi.VERSAO_DADOS = versaoDados AND cvi.TABELA_DESTINO = tabelaDestino;

    TYPE T_TUPLA_NAO_PROCESSADA IS TABLE OF cr_tuplas_nao_processadas%ROWTYPE INDEX BY BINARY_INTEGER;
    TYPE T_TYPE_MAPEAMENTO IS TABLE OF cr_mapeamento%ROWTYPE INDEX BY BINARY_INTEGER;
    v_tipo_dado        cr_estrutura_nao_processado%ROWTYPE;
    v_nao_processada   T_TUPLA_NAO_PROCESSADA;
    v_table_mapeamento T_TYPE_MAPEAMENTO;
    v_insert           CLOB := '';
    v_colunas_insert   CLOB := '';
    v_ativo            VARCHAR2(1);
  BEGIN
    DBMS_SESSION.SET_NLS('NLS_DATE_FORMAT', '''YYYY-MM-DD HH24:MI:SS''');

    BEGIN
      SELECT FG_PRC_ATIVO INTO v_ativo
      FROM CFG_SEMAFORO_PROCESSAMENTO
      WHERE CD_ID = 303;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      INSERT INTO CFG_SEMAFORO_PROCESSAMENTO (CD_ID, DESC_NOME_PRC, FG_PRC_ATIVO, DESC_COMENTARIO)
      VALUES (303, 'SGPA_INTEGRACAO_PROCESSADOR.PROC_INTEGRAR_DADOS', 'F', 'Procedure que le dados da DDP_DADOSINTEGRACAO e distribui para as outras tabelas');
      COMMIT;
      v_ativo := 'F';
    END;

    IF v_ativo = 'F' THEN
      UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'T', DT_HR_EXECUCAO_PROC = SYSDATE WHERE CD_ID = 303;
      COMMIT;

      OPEN cr_estrutura_nao_processado;
        LOOP
          FETCH cr_estrutura_nao_processado INTO v_tipo_dado;
          EXIT WHEN cr_estrutura_nao_processado%NOTFOUND;
          v_colunas_insert := v_tipo_dado.VL_COLUNAS_ENTRADA;
          OPEN cr_mapeamento(v_tipo_dado.VERSAO_DADOS, v_tipo_dado.TABELA_DESTINO);
          FETCH cr_mapeamento BULK COLLECT INTO v_table_mapeamento;
          CLOSE cr_mapeamento;
          IF v_table_mapeamento.COUNT > 0 THEN
            FOR I IN 1.. v_table_mapeamento.COUNT LOOP
              SELECT REPLACE(v_colunas_insert, v_table_mapeamento(I).COLUNA_ENTRADA, v_table_mapeamento(I).COLUNA_SAIDA)
                INTO v_colunas_insert FROM DUAL;
            END LOOP;
          END IF;
          OPEN cr_tuplas_nao_processadas(v_tipo_dado.VERSAO_DADOS, v_tipo_dado.TABELA_DESTINO);
          LOOP
            FETCH cr_tuplas_nao_processadas
            BULK COLLECT INTO v_nao_processada
            LIMIT 10000;
            EXIT WHEN v_nao_processada.COUNT = 0;

            FOR T IN 1 .. v_nao_processada.COUNT LOOP
              BEGIN
                v_insert := 'INSERT INTO ' || v_nao_processada(T).TABELA_DESTINO ||' (' || v_colunas_insert  || ') VALUES (' || v_nao_processada(T).VL_VALORES_ENTRADA || ')';
                EXECUTE IMMEDIATE v_insert;
                --Somente deleta inserido com sucesso.
                BEGIN
                  DELETE FROM DDP_DADOSINTEGRACAO WHERE CD_ID = v_nao_processada(T).CD_ID;
                EXCEPTION WHEN OTHERS THEN
                  --Erro de update.
                  INSERT INTO LOG_INTEGRACAO_HIST(CD_LOG, CD_DADOSINTEGRACAO, DETALHES) VALUES (3, v_nao_processada(T).CD_ID, SYS.STANDARD.SQLERRM);
                END;
              EXCEPTION WHEN OTHERS THEN
                --Erro de inserçao.
                INSERT INTO LOG_INTEGRACAO_HIST(CD_LOG, CD_DADOSINTEGRACAO, DETALHES) VALUES (2, v_nao_processada(T).CD_ID, SYS.STANDARD.SQLERRM || ' | ' || v_insert );
              END;
            END LOOP;

            COMMIT;
          END LOOP;
          CLOSE cr_tuplas_nao_processadas;
          v_colunas_insert := null;
        END LOOP;
        CLOSE cr_estrutura_nao_processado;

      UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'F', VL_TEMPO_EXECUCAO = ( SYSDATE - DT_HR_EXECUCAO_PROC ) * 86400 WHERE CD_ID = 303;
      COMMIT;
    END IF;
  END PROC_INTEGRAR_DADOS;

  PROCEDURE PROC_DELETAR_ANTIGOS(dias NUMBER) IS
    CURSOR C_CURSOR IS
      SELECT CD_ID FROM DDP_DADOSINTEGRACAO_BK WHERE DT_INSERIDO <= (SYSDATE - dias);
    TYPE TYPE_CURSOR IS TABLE OF C_CURSOR%ROWTYPE INDEX BY BINARY_INTEGER;
    R_CURSOR TYPE_CURSOR;
    v_ativo VARCHAR2(1);
  BEGIN

    BEGIN
      SELECT FG_PRC_ATIVO INTO v_ativo
      FROM CFG_SEMAFORO_PROCESSAMENTO
      WHERE CD_ID = 304;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      INSERT INTO CFG_SEMAFORO_PROCESSAMENTO (CD_ID, DESC_NOME_PRC, FG_PRC_ATIVO, DESC_COMENTARIO)
      VALUES (304, 'SGPA_INTEGRACAO_PROCESSADOR.PROC_DELETAR_ANTIGOS', 'F', 'Procedure que deleta dados antigos da tabela de backup DDP_DADOSINTEGRACAO_BK');
      COMMIT;
      v_ativo := 'F';
    END;

    IF v_ativo = 'F' THEN
      UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'T', DT_HR_EXECUCAO_PROC = SYSDATE WHERE CD_ID = 304;
      COMMIT;

      OPEN C_CURSOR;
        LOOP
          FETCH C_CURSOR
            BULK COLLECT INTO R_CURSOR
            LIMIT 10000;
            EXIT WHEN R_CURSOR.COUNT = 0;
          FORALL I IN 1 .. R_CURSOR.COUNT
              DELETE FROM LOG_INTEGRACAO_HIST WHERE CD_DADOSINTEGRACAO = R_CURSOR(I).CD_ID;
          FORALL I IN 1 .. R_CURSOR.COUNT
              DELETE FROM DDP_DADOSINTEGRACAO_BK WHERE CD_ID = R_CURSOR(I).CD_ID;
          INSERT INTO LOG_INTEGRACAO_HIST(CD_LOG, DETALHES) VALUES (5, '10000');
          COMMIT;
        END LOOP;
      CLOSE C_CURSOR;

      UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'F', VL_TEMPO_EXECUCAO = ( SYSDATE - DT_HR_EXECUCAO_PROC ) * 86400 WHERE CD_ID = 304;
      COMMIT;
    END IF;
  END PROC_DELETAR_ANTIGOS;
  PROCEDURE PROC_DELETAR_LOG_ANTIGO(dias NUMBER) IS
    CURSOR C_CURSOR IS
      SELECT CD_ID FROM LOG_INTEGRACAO_HIST WHERE DT_LOG <= (SYSDATE - dias);
    TYPE TYPE_CURSOR IS TABLE OF C_CURSOR%ROWTYPE INDEX BY BINARY_INTEGER;
    R_CURSOR TYPE_CURSOR;
    v_ativo VARCHAR2(1);
  BEGIN

    BEGIN
      SELECT FG_PRC_ATIVO INTO v_ativo
      FROM CFG_SEMAFORO_PROCESSAMENTO
      WHERE CD_ID = 305;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      INSERT INTO CFG_SEMAFORO_PROCESSAMENTO (CD_ID, DESC_NOME_PRC, FG_PRC_ATIVO, DESC_COMENTARIO)
      VALUES (305, 'SGPA_INTEGRACAO_PROCESSADOR.PROC_DELETAR_LOG_ANTIG', 'F', 'Procedure que deleta dados antigos da tabela de log de integracao LOG_INTEGRACAO_HIST');
      COMMIT;
      v_ativo := 'F';
    END;

    IF v_ativo = 'F' THEN
      UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'T', DT_HR_EXECUCAO_PROC = SYSDATE WHERE CD_ID = 305;
      COMMIT;

      OPEN C_CURSOR;
        LOOP
          FETCH C_CURSOR
            BULK COLLECT INTO R_CURSOR
            LIMIT 10000;
            EXIT WHEN R_CURSOR.COUNT = 0;
          FORALL I IN 1 .. R_CURSOR.COUNT
              DELETE FROM LOG_INTEGRACAO_HIST WHERE CD_ID = R_CURSOR(I).CD_ID;
          INSERT INTO LOG_INTEGRACAO_HIST(CD_LOG, DETALHES) VALUES (5, '10000');
          COMMIT;
        END LOOP;
      CLOSE C_CURSOR;

      UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'F', VL_TEMPO_EXECUCAO = ( SYSDATE - DT_HR_EXECUCAO_PROC ) * 86400 WHERE CD_ID = 305;
      COMMIT;
    END IF;
  END PROC_DELETAR_LOG_ANTIGO;
END SGPA_INTEGRACAO_PROCESSADOR;
/
CREATE OR REPLACE PACKAGE SGPA_METADATA_UTILS AS
  /*
  Recebe o nome de uma tabela e gera o codigo necessario para criar uma tabela
  DDP equivalente, bem como uma sequence e uma procedure que farao o autoincrement
  da chave primaria da tabela DDP gerada.

  recebe
    @p_table_name nome da tabela a ser criado, ao fim da funcao e substituido pelo
    nome da tabela gerada
  retorna
    @ddl ddl contendo create table, trigger e sequence
  */
  FUNCTION generate_ddp_table_ddl(p_table_name IN VARCHAR2) RETURN CLOB;
  /*
  Recebe um script que pode conter um ou mais comandos SQL, quebra os statemnts por
  `/` e remove ultimo caractere para retirar o `;`. Executa cada statement dentro
  de um loop, caso algum deles falhe o loop para e cabe ao usuario tratar a exception
  recebe
    @statement comandos a serem executados
  */
  PROCEDURE exec_multi_statement_script(statement varchar2);
END SGPA_METADATA_UTILS;
/
CREATE OR REPLACE PACKAGE BODY SGPA_METADATA_UTILS AS
  FUNCTION generate_ddp_table_ddl(p_table_name IN VARCHAR2)
  	RETURN CLOB IS
      CURSOR columns_info_cr (p_table_name IN VARCHAR2) IS
        SELECT * FROM USER_TAB_COLUMNS atc WHERE atc.TABLE_NAME = p_table_name ;
      column_info columns_info_cr%ROWTYPE;
      ddl CLOB := '';
      m_ddp_name VARCHAR2(30);
      m_name_hash VARCHAR2(10);
      m_seq_name VARCHAR2(30);
      m_trg_name VARCHAR2(30);
  	BEGIN
      m_ddp_name := 'DDP_' || SUBSTR(p_table_name, 5, 26);
      SELECT ORA_HASH(m_ddp_name)INTO m_name_hash FROM DUAL;
      ddl := 'CREATE TABLE '|| m_ddp_name || ' ( ';
      OPEN columns_info_cr(p_table_name);
  		LOOP
  			FETCH columns_info_cr INTO column_info;
  			EXIT WHEN columns_info_cr%NOTFOUND;
        if column_info.DATA_TYPE = 'VARCHAR2' then
          ddl := ddl || column_info.COLUMN_NAME || ' VARCHAR2(' || nvl(column_info.DATA_LENGTH, 255) || '), ';
        elsif column_info.DATA_TYPE like 'TIMESTAMP%' or column_info.DATA_TYPE = 'DATE' then
          ddl := ddl || column_info.COLUMN_NAME || ' VARCHAR2(255), ';
        elsif column_info.DATA_TYPE = 'NUMBER' then
          ddl := ddl || column_info.COLUMN_NAME || ' NUMBER(' || nvl(column_info.DATA_PRECISION, 20) || ', ' || nvl(column_info.DATA_SCALE, 0)  || '), ';
        else
          ddl := ddl || column_info.COLUMN_NAME || ' ' || column_info.DATA_TYPE || ', ';
        end if;
  		END LOOP;
      CLOSE columns_info_cr;
      ddl := ddl || 'VERSAO_SCRIPT VARCHAR2(10) DEFAULT ''V1'' NOT NULL, ';
      ddl := ddl || 'DDP_ID NUMBER NOT NULL, ';
      ddl := ddl || 'VL_PROCESSED NUMBER(1,0) DEFAULT 0 NOT NULL,  ';
      ddl := ddl || 'DT_CREATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, ';
      ddl := ddl || 'CONSTRAINT DDP_'|| m_name_hash ||'_PK PRIMARY KEY (DT_CREATED, VL_PROCESSED, DDP_ID), ';
      ddl := ddl || 'CONSTRAINT DPP_' || m_name_hash || '_CHECK_FLAG CHECK (VL_PROCESSED IN(1, 0))';
      ddl := ddl || ');';

      ddl := ddl || '/';
      ddl := ddl || 'CREATE INDEX IDX_' || m_name_hash || '_ID ON ' || m_ddp_name || '(DDP_ID);';

      ddl := ddl || '/';
      m_seq_name := 'SEQ_' || m_name_hash;
      ddl := ddl || 'CREATE SEQUENCE ' || m_seq_name || ' START WITH 1 INCREMENT BY 1 NOCYCLE;';

      ddl := ddl || '/';

      m_trg_name := 'BI_' || m_name_hash;
      ddl := ddl || 'CREATE OR REPLACE TRIGGER ' || m_trg_name || ' BEFORE INSERT ON ' || m_ddp_name || ' FOR EACH ROW BEGIN SELECT ' || m_seq_name || '.NEXTVAL INTO :NEW.DDP_ID FROM DUAL; END ' || m_trg_name || '; ';

      ddl := ddl || '/';

      ddl := ddl || 'COMMENT ON TABLE '|| m_ddp_name ||' IS ''Tabela gerada automaticamente como staging da integracao com a tabela ' || p_table_name || '. Sequence: ' || m_seq_name ||  '. BI_TRIGGER: ' || m_trg_name || ''';';
  		RETURN ddl;
  	END generate_ddp_table_ddl;

  PROCEDURE exec_multi_statement_script(statement varchar2) IS
      CURSOR split(input IN VARCHAR2, delimiter IN CHAR) IS
        select regexp_substr (input, '[^' || delimiter || ']+',1, rownum) str
        from dual
        connect by level <= regexp_count (input, '[^' || delimiter || ']+');
      m_splited split%ROWTYPE;
      m_statement CLOB;
    BEGIN
      OPEN split(statement, '/');
      LOOP
        FETCH split INTO m_splited;
        EXIT WHEN split%NOTFOUND;
        SELECT SUBSTR(m_splited.str, 1, LENGTH(m_splited.str) - 1) INTO m_statement FROm DUAL;
        DBMS_UTILITY.EXEC_DDL_STATEMENT(m_statement);
        DBMS_OUTPUT.PUT_LINE('O seguinte SQL foi executado com sucesso : ' || m_statement);
      END LOOP;
      CLOSE split;
      EXCEPTION
        WHEN OTHERS THEN
          DBMS_OUTPUT.PUT_LINE('COULD NOT EXECUTE FOLLOWING STATEMENT : ' || m_statement);
          DBMS_OUTPUT.PUT_LINE(SQLCODE||' -ERROR- '||SQLERRM);
          ROLLBACK;
          CLOSE split;
  END exec_multi_statement_script;
END SGPA_METADATA_UTILS;
/