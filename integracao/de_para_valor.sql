CREATE TABLE CFG_DEPARA_VALOR_INTEGRACAO (
  ID NUMBER NOT NULL PRIMARY KEY
  , TABELA_DESTINO VARCHAR2(30) NOT NULL
  , COLUNA_ENTRADA VARCHAR2(255) NOT NULL
  , VALOR_ENTRADA VARCHAR2(30) NOT NULL
  , VALOR_SAIDA VARCHAR2(30) NOT NULL
  , CONSTRAINT UN_DEPARA_VALOR_INTEGRACAO UNIQUE (TABELA_DESTINO, COLUNA_ENTRADA, VALOR_ENTRADA)
    USING INDEX (CREATE INDEX IDX_UN_DEPARA_VALOR_INT ON CFG_DEPARA_VALOR_INTEGRACAO(TABELA_DESTINO, COLUNA_ENTRADA, VALOR_ENTRADA))
);

CREATE SEQUENCE SEQ_DEPARA_VALOR_INTEGRACAO START WITH 1 INCREMENT BY 1;
CREATE OR REPLACE TRIGGER TRG_BI_DEPARA_VALOR_INT
  BEFORE INSERT ON CFG_DEPARA_VALOR_INTEGRACAO
  FOR EACH ROW
BEGIN
  :NEW.ID := SEQ_DEPARA_VALOR_INTEGRACAO.nextval;
END;

INSERT ALL
  --INTO CFG_VERSAO_INTEGRACAO(VERSAO_DADOS, TABELA_DESTINO) VALUES ('V1', 'DDP_ENTRADA_ORDEM_SERVICO')
  INTO CFG_DEPARA_VALOR_INTEGRACAO(TABELA_DESTINO, COLUNA_ENTRADA, VALOR_ENTRADA, VALOR_SAIDA) VALUES ('DDP_ENTRADA_ORDEM_SERVICO', 'ID_ERP_TALHAO', '8154961382274479803', '''153''')
  INTO CFG_DEPARA_VALOR_INTEGRACAO(TABELA_DESTINO, COLUNA_ENTRADA, VALOR_ENTRADA, VALOR_SAIDA) VALUES ('DDP_ENTRADA_ORDEM_SERVICO', 'ID_ERP_TALHAO', '1076568637056828911', '''154''')
  INTO CFG_DEPARA_VALOR_INTEGRACAO(TABELA_DESTINO, COLUNA_ENTRADA, VALOR_ENTRADA, VALOR_SAIDA) VALUES ('DDP_ENTRADA_ORDEM_SERVICO', 'ID_ERP_TALHAO', '860659240049189433', '''155''')
  INTO CFG_DEPARA_VALOR_INTEGRACAO(TABELA_DESTINO, COLUNA_ENTRADA, VALOR_ENTRADA, VALOR_SAIDA) VALUES ('DDP_ENTRADA_ORDEM_SERVICO', 'ID_ERP_TALHAO', '2880922369011192475', '''164''')
SELECT 1 FROM DUAL;
COMMIT;

SELECT CD_TALHAO, DECODE(ID_ERP_TALHAO, VALOR_ENTRADA, VALOR_SAIDA, ID_ERP_TALHAO)
FROM DDP_ENTRADA_ORDEM_SERVICO deos
LEFT JOIN CFG_DEPARA_VALOR_INTEGRACAO cdvi ON (deos.ID_ERP_TALHAO = cdvi.VALOR_ENTRADA AND cdvi.COLUNA_ENTRADA = 'ID_ERP_TALHAO');