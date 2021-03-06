alter table DDP_ORDEM_SERVICO ADD CD_INSUMO VARCHAR2(255);
/
alter table DDP_ORDEM_SERVICO ADD DESC_INSUMO VARCHAR2(1000);
/
create or replace trigger TRG_BI_DDP_ORDEM_SERVICO
  before insert
  on DDP_ORDEM_SERVICO
  for each row
  BEGIN
    :NEW.CD_ID := SEQ_DDP_ORDEM_SERVICO.nextval;
    :new.dt_hr_servidor:= SYSDATE;
END;
/
CREATE TABLE DDP_ENTRADA_ORDEM_SERVICO (
  ID_ERP_FAZENDA       NUMBER(38, 0)
  , ID_ERP_ZONA          NUMBER(38, 0)
  , ID_ERP_TALHAO        NUMBER(38, 0)
  , DT_ABERTURA          VARCHAR2(255)
  , DT_ENCERRA           VARCHAR2(255)
  , FG_SITUACAO          VARCHAR2(1)
  , DT_HR_SERVIDOR       VARCHAR2(255)
  , DT_UPDATE            VARCHAR2(255)
  , CD_INSUMO            VARCHAR2(255)
  , DESC_INSUMO          VARCHAR2(1000)
  , CD_ID                NUMBER(11, 0)
  , CD_ORDEM_SERVICO     NUMBER(38, 0)
  , CD_CENTRO_CUSTO      VARCHAR2(20)
  , CD_EQUIPE            NUMBER(38, 0)
  , CD_OPERACAO          NUMBER(38, 0)
  , CD_PERIODO_SAFRA     NUMBER(38, 0)
  , CD_SAFRA             NUMBER(38, 0)
  , CD_UNIDADE           VARCHAR2(15)
  , CD_FAZENDA           VARCHAR2(6)
  , CD_ZONA              VARCHAR2(6)
  , CD_TALHAO            VARCHAR2(6)
  , DESC_CENTRO_CUSTO    VARCHAR2(30)
  , DESC_EQUIPE          VARCHAR2(50)
  , DESC_OPERACAO        VARCHAR2(60)
  , DESC_PERIODO_SAFRA   VARCHAR2(30)
  , DESC_SAFRA           VARCHAR2(30)
  , DESC_UNIDADE         VARCHAR2(50)
  , DESC_FAZENDA         VARCHAR2(50)
  , DESC_ZONA            VARCHAR2(50)
  , DESC_TALHAO          VARCHAR2(50)
  , ID_ERP_ORDEM_SERVICO NUMBER(38, 0)
  , ID_ERP_CENTRO_CUSTO  NUMBER(38, 0)
  , ID_ERP_EQUIPE        NUMBER(38, 0)
  , ID_ERP_OPERACAO      NUMBER(38, 0)
  , ID_ERP_PERIODO_SAFRA NUMBER(38, 0)
  , ID_ERP_SAFRA         NUMBER(38, 0)
  , ID_ERP_UNIDADE       NUMBER(38, 0)
  , VERSAO_SCRIPT        VARCHAR2(10) DEFAULT 'V1'           NOT NULL
  , DDP_ID               NUMBER                              NOT NULL
  , VL_PROCESSED         NUMBER(1, 0) DEFAULT 0              NOT NULL
  , DT_CREATED           TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
  , CONSTRAINT DDP_2865471667_PK PRIMARY KEY (DT_CREATED, VL_PROCESSED, DDP_ID)
  , CONSTRAINT DPP_2865471667_CHECK_FLAG CHECK (VL_PROCESSED IN (1, 0))
);
/
CREATE SEQUENCE SEQ_2865471667
  START WITH 1
  INCREMENT BY 1
  NOCYCLE;
/
CREATE OR REPLACE TRIGGER BI_2865471667
  BEFORE INSERT
  ON DDP_ENTRADA_ORDEM_SERVICO
  FOR EACH ROW
  BEGIN SELECT SEQ_2865471667.NEXTVAL
        INTO :NEW.DDP_ID
        FROM DUAL;
  END BI_2865471667;
/
COMMENT ON TABLE DDP_ENTRADA_ORDEM_SERVICO IS 'Tabela gerada automaticamente como staging da integracao com a tabela DDP_ORDEM_SERVICO. Sequence: SEQ_2865471667. BI_TRIGGER: BI_2865471667';
/
CREATE TABLE LOG_INT_OS_HIST (
  CD_ID NUMBER NOT NULL PRIMARY KEY
  , CD_LOG NUMBER NOT NULL
  , DDP_ID NUMBER
  , DT_LOG TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
  , DETALHES CLOB
);
/
CREATE SEQUENCE SEQ_LOG_INT_OS_HIST
  START WITH 1
  INCREMENT BY 1
  NOCYCLE;
/
CREATE OR REPLACE TRIGGER TRG_LOG_INT_OS_HIST_PK
  BEFORE INSERT
  ON LOG_INT_OS_HIST
  FOR EACH ROW
  BEGIN SELECT SEQ_LOG_INT_OS_HIST.NEXTVAL
        INTO :NEW.CD_ID
        FROM DUAL;
  END TRG_LOG_INT_OS_HIST_PK;
/