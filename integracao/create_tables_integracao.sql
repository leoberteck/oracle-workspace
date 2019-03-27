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

CREATE TABLE SGPA.CDT_LOG(
  CD_ID NUMBER(11, 0) NOT NULL PRIMARY KEY
  , TITULO VARCHAR2(500) NOT NULL
  , MENSAGEM VARCHAR2(4000) DEFAULT NULL
  , NIVEL NUMBER(1, 0) DEFAULT 0 NOT NULL CHECK (NIVEL IN (0,1,2,3,4,5,6))
);
/
INSERT ALL
  INTO SGPA.CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (1, 'SGPA_INTEGRATION', 'O dado nao possui mapeamento', 3)
  INTO SGPA.CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (2, 'SGPA_INTEGRATION', 'O dado nao pode ser inserido na tabela de destino', 2)
  INTO SGPA.CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (3, 'SGPA_INTEGRATION', 'A tabela de entrada nao pode ser atualizada', 2)
  INTO SGPA.CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (4, 'SGPA_INTEGRATION', 'A integracao foi concluida com sucesso para um grupo de tuplas', 5)
  INTO SGPA.CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (5, 'SGPA_INTEGRATION', 'Foram deletadas tuplas processadas da tabela de entrada', 5)
  INTO SGPA.CDT_LOG (CD_ID, TITULO, MENSAGEM, NIVEL) VALUES (6, 'COMMON', 'Nao foi possivel concluir o procedimento devido a um erro interno no banco de dados', 1)
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
  , DETALHES VARCHAR2(4000) NULL
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