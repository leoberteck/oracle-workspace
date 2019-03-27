CREATE TABLE CDT_INSUMO (
  CD_ID NUMBER NOT NULL PRIMARY KEY
  , DESCRICAO VARCHAR2(500)
  , UNIDADE VARCHAR2(5)
  , DT_INSERIDO TIMESTAMP DEFAULT SYSTIMESTAMP
  , DT_ATUALIZADO TIMESTAMP DEFAULT SYSTIMESTAMP
);
/
CREATE TABLE CDT_INSUMO_COMPONENTE (
  CD_INSUMO NUMBER CONSTRAINT FK_INSUMO_COMPOSTO REFERENCES CDT_INSUMO(CD_ID)
  , CD_COMPONENTE NUMBER CONSTRAINT FK_INSUMO_COMPONENTE REFERENCES CDT_INSUMO(CD_ID)
  , CONSTRAINT PK_INSUMO_COMPONENTE PRIMARY KEY (CD_INSUMO, CD_COMPONENTE)
);
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
CREATE TABLE DDP_INSUMO (
  CD_ID         NUMBER(20, 0),
  DESCRICAO     VARCHAR2(500),
  UNIDADE       VARCHAR2(5),
  DT_INSERIDO   VARCHAR2(255),
  DT_ATUALIZADO VARCHAR2(255),
  VERSAO_SCRIPT VARCHAR2(10) DEFAULT 'V1'           NOT NULL,
  DDP_ID        NUMBER                              NOT NULL,
  VL_PROCESSED  NUMBER(1, 0) DEFAULT 0              NOT NULL,
  DT_CREATED    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT DDP_1870345742_PK PRIMARY KEY (DT_CREATED, VL_PROCESSED, DDP_ID),
  CONSTRAINT DPP_1870345742_CHECK_FLAG CHECK (VL_PROCESSED IN (1, 0))
);
/ CREATE SEQUENCE SEQ_1870345742
  START WITH 1
  INCREMENT BY 1
  NOCYCLE;
/ CREATE OR REPLACE TRIGGER BI_1870345742
  BEFORE INSERT
  ON DDP_INSUMO
  FOR EACH ROW
  BEGIN SELECT SEQ_1870345742.NEXTVAL
        INTO :NEW.DDP_ID
        FROM DUAL;
  END BI_1870345742; / COMMENT ON TABLE DDP_INSUMO
IS 'Tabela gerada automaticamente como staging da integracao com a tabela CDT_INSUMO. Sequence: SEQ_1870345742. BI_TRIGGER: BI_1870345742';
/
CREATE TABLE DDP_INSUMO_COMPONENTE (
  CD_INSUMO     NUMBER(20, 0),
  CD_COMPONENTE NUMBER(20, 0),
  VERSAO_SCRIPT VARCHAR2(10) DEFAULT 'V1'           NOT NULL,
  DDP_ID        NUMBER                              NOT NULL,
  VL_PROCESSED  NUMBER(1, 0) DEFAULT 0              NOT NULL,
  DT_CREATED    TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT DDP_2369826196_PK PRIMARY KEY (DT_CREATED, VL_PROCESSED, DDP_ID),
  CONSTRAINT DPP_2369826196_CHECK_FLAG CHECK (VL_PROCESSED IN (1, 0))
);
/ CREATE SEQUENCE SEQ_2369826196
  START WITH 1
  INCREMENT BY 1
  NOCYCLE;
/ CREATE OR REPLACE TRIGGER BI_2369826196
  BEFORE INSERT
  ON DDP_INSUMO_COMPONENTE
  FOR EACH ROW
  BEGIN SELECT SEQ_2369826196.NEXTVAL
        INTO :NEW.DDP_ID
        FROM DUAL;
  END BI_2369826196; / COMMENT ON TABLE DDP_INSUMO_COMPONENTE
IS 'Tabela gerada automaticamente como staging da integracao com a tabela CDT_INSUMO_COMPONENTE. Sequence: SEQ_2369826196. BI_TRIGGER: BI_2369826196';
/
INSERT INTO CFG_PUSH_INTEGRATION(IDENTIFICACAO, STAGING_TABLE) VALUES ('INSUMO', 'DDP_INSUMO');
/
INSERT INTO CFG_PUSH_INTEGRATION(IDENTIFICACAO, STAGING_TABLE) VALUES ('INSUMO_COMPONENTE', 'DDP_INSUMO_COMPONENTE');
/
COMMIT;
/
CREATE OR REPLACE PACKAGE SGPA_PROCESSADOR_INSUMO AS
  PROCEDURE PRC_INTEGRAR_INSUMOS;
  PROCEDURE PRC_INTEGRAR_COMPONENTES;
END SGPA_PROCESSADOR_INSUMO;
/
CREATE OR REPLACE PACKAGE BODY SGPA_PROCESSADOR_INSUMO AS
  PROCEDURE PRC_INTEGRAR_INSUMOS IS
    CURSOR cr_all IS SELECT * FROM DDP_INSUMO;
    TYPE t_all IS TABLE OF cr_all%ROWTYPE INDEX BY BINARY_INTEGER;
    r_all t_all;
    v_error_code    NUMBER;
    v_error_message VARCHAR2(32000);
    v_inicio_prc DATE := SYSDATE;
    v_prc_id NUMBER := 313;
    v_desc_prc VARCHAR2(400) := 'SGPA_PROCESSADOR_INSUMO.PRC_INTEGRAR_INSUMOS';
    v_ativo VARCHAR2(1);

    PROCEDURE LOGGER(P_TIPO VARCHAR2, P_DESC VARCHAR2) IS
    BEGIN
      PRC_SALVAR_LOG_V5(v_prc_id, v_desc_prc, SYSDATE, SYSDATE, v_inicio_prc, P_DESC, P_TIPO, NULL , NULL , NULL, NULL);
    END;
  BEGIN
    BEGIN
      SELECT FG_PRC_ATIVO
      INTO v_ativo
      FROM CFG_SEMAFORO_PROCESSAMENTO
      WHERE CD_ID = v_prc_id;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      INSERT INTO CFG_SEMAFORO_PROCESSAMENTO (CD_ID, DESC_NOME_PRC, FG_PRC_ATIVO, DESC_COMENTARIO)
      VALUES (v_prc_id, v_desc_prc, 'F', 'Integra dados de cadastro de insumo');
      COMMIT;
      v_ativo := 'F';
    END;

    IF v_ativo = 'F' THEN
      UPDATE CFG_SEMAFORO_PROCESSAMENTO
      SET FG_PRC_ATIVO = 'T', DT_HR_EXECUCAO_PROC = v_inicio_prc
      WHERE CD_ID = v_prc_id;
      COMMIT;

      LOGGER('I', 'START - PRC_INTEGRAR_INSUMOS');
      OPEN cr_all;
      LOGGER('I', 'MERGE DE INSUMOS');
      LOOP FETCH cr_all
        BULK COLLECT INTO r_all
        LIMIT 1000;
        EXIT WHEN r_all.COUNT = 0;
        FOR I IN 1 .. r_all.COUNT LOOP
          BEGIN
            MERGE INTO CDT_INSUMO ci USING DUAL ON
              (ci.CD_ID = r_all(I).CD_ID)
            WHEN NOT MATCHED THEN
              INSERT (CD_ID, DESCRICAO, UNIDADE, DT_INSERIDO, DT_ATUALIZADO)
              VALUES (r_all(I).CD_ID, r_all(I).DESCRICAO, r_all(I).UNIDADE, TO_DATE(r_all(I).DT_INSERIDO, 'YYYY-MM-DD HH24:MI:SS'), TO_DATE(r_all(I).DT_ATUALIZADO, 'YYYY-MM-DD HH24:MI:SS'))
            WHEN MATCHED THEN
              UPDATE SET
                DESCRICAO = r_all(I).DESCRICAO
                , UNIDADE = r_all(I).UNIDADE
                , DT_INSERIDO = TO_DATE(r_all(I).DT_INSERIDO, 'YYYY-MM-DD HH24:MI:SS')
                , DT_ATUALIZADO = TO_DATE(r_all(I).DT_ATUALIZADO, 'YYYY-MM-DD HH24:MI:SS');
            DELETE FROM DDP_INSUMO WHERE DDP_ID = r_all(I).DDP_ID;
          EXCEPTION WHEN OTHERS THEN
            v_error_code := SQLCODE;
            v_error_message := SQLERRM;
            LOGGER('E', 'Nao foi possivel mergear o registro de insumo com id: ' || r_all(I).DDP_ID
                  || '. Error: ' || v_error_code || ': ' || v_error_message);
          END;
        END LOOP;
        COMMIT;
      END LOOP;
      CLOSE cr_all;
      LOGGER('S', 'FIM - PRC_INTEGRAR_INSUMOS');
      UPDATE CFG_SEMAFORO_PROCESSAMENTO
      SET FG_PRC_ATIVO = 'F', VL_TEMPO_EXECUCAO = (SYSDATE - DT_HR_EXECUCAO_PROC) * 86400
      WHERE CD_ID = v_prc_id;
      COMMIT;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_error_code := SQLCODE;
    v_error_message := SQLERRM;
    LOGGER('E', 'Nao foi possivel concluir MERGE DE INSUMOS. Error: ' || v_error_code || ': ' || v_error_message);
  END PRC_INTEGRAR_INSUMOS;

  PROCEDURE PRC_INTEGRAR_COMPONENTES IS
    CURSOR cr_all IS SELECT * FROM DDP_INSUMO_COMPONENTE;
    TYPE t_all IS TABLE OF cr_all%ROWTYPE INDEX BY BINARY_INTEGER;
    r_all t_all;
    v_error_code    NUMBER;
    v_error_message VARCHAR2(32000);
    v_inicio_prc DATE := SYSDATE;
    v_prc_id NUMBER := 314;
    v_desc_prc VARCHAR2(400) := 'SGPA_PROCESSADOR_INSUMO.PRC_INTEGRAR_COMPONENTES';
    v_ativo VARCHAR2(1);
    v_count NUMBER;
    PROCEDURE LOGGER(P_TIPO VARCHAR2, P_DESC VARCHAR2) IS
    BEGIN
      PRC_SALVAR_LOG_V5(v_prc_id, v_desc_prc, SYSDATE, SYSDATE, v_inicio_prc, P_DESC, P_TIPO, NULL , NULL , NULL, NULL);
    END;
  BEGIN
    SELECT COUNT(1) INTO v_count from DDP_INSUMO_COMPONENTE;
    BEGIN
      SELECT FG_PRC_ATIVO
      INTO v_ativo
      FROM CFG_SEMAFORO_PROCESSAMENTO
      WHERE CD_ID = v_prc_id;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      INSERT INTO CFG_SEMAFORO_PROCESSAMENTO (CD_ID, DESC_NOME_PRC, FG_PRC_ATIVO, DESC_COMENTARIO)
      VALUES (v_prc_id, v_desc_prc, 'F', 'Integra dados dos componentes de cada insumo');
      COMMIT;
      v_ativo := 'F';
    END;
    IF v_ativo = 'F' AND v_count > 0 THEN
      UPDATE CFG_SEMAFORO_PROCESSAMENTO
      SET FG_PRC_ATIVO = 'T', DT_HR_EXECUCAO_PROC = v_inicio_prc
      WHERE CD_ID = v_prc_id;
      COMMIT;
      LOGGER('I', 'START - PRC_INTEGRAR_COMPONENTES');
      LOGGER('I', 'DELETAR COMPONENTES QUE NAO VIERAM NA INTEGRACAO');
      DELETE FROM CDT_INSUMO_COMPONENTE WHERE CD_INSUMO IN (
        SELECT cic.CD_INSUMO FROM CDT_INSUMO_COMPONENTE cic
          LEFT JOIN DDP_INSUMO_COMPONENTE dic ON (cic.CD_INSUMO = dic.CD_INSUMO AND cic.CD_COMPONENTE = dic.CD_COMPONENTE)
          WHERE dic.DDP_ID IS NULL
        GROUP BY cic.CD_INSUMO
      );
      COMMIT;
      OPEN cr_all;
      LOGGER('I', 'MERGE DE COMPONENTES DE INSUMOS');
      LOOP FETCH cr_all
        BULK COLLECT INTO r_all
        LIMIT 1000;
        EXIT WHEN r_all.COUNT = 0;
        FOR I IN 1 .. r_all.COUNT LOOP
          BEGIN
            MERGE INTO CDT_INSUMO_COMPONENTE cic USING DUAL
              ON (cic.CD_INSUMO = r_all(I).CD_INSUMO AND cic.CD_COMPONENTE = r_all(I).CD_COMPONENTE)
            WHEN NOT MATCHED THEN
              INSERT (CD_INSUMO, CD_COMPONENTE) VALUES (r_all(I).CD_INSUMO, r_all(I).CD_COMPONENTE);
            DELETE FROM DDP_INSUMO WHERE DDP_ID = r_all(I).DDP_ID;
          EXCEPTION WHEN OTHERS THEN
            v_error_code := SQLCODE;
            v_error_message := SQLERRM;
            LOGGER('E', 'Nao foi possivel mergear o registro de componente de insumo com id: ' || r_all(I).DDP_ID
                  || '. Error: ' || v_error_code || ': ' || v_error_message);
          END;
        END LOOP;
        COMMIT;
      END LOOP;
      CLOSE cr_all;
      LOGGER('S', 'FIM - PRC_INTEGRAR_COMPONENTES');
      UPDATE CFG_SEMAFORO_PROCESSAMENTO
      SET FG_PRC_ATIVO = 'F', VL_TEMPO_EXECUCAO = (SYSDATE - DT_HR_EXECUCAO_PROC) * 86400
      WHERE CD_ID = v_prc_id;
      COMMIT;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_error_code := SQLCODE;
    v_error_message := SQLERRM;
    LOGGER('E', 'Nao foi possivel concluir MERGE DE COMPONENTES DE INSUMO. Error: ' || v_error_code || ': ' || v_error_message);
  END PRC_INTEGRAR_COMPONENTES;
END SGPA_PROCESSADOR_INSUMO;
/
DROP TABLE LOG_INT_OS_HIST;
/
CREATE OR REPLACE PACKAGE SGPA_PROCESSADOR_OS AS
  PROCEDURE PRC_INT_ORDEM_SERVICO;
  PROCEDURE GERA_COMANDO(p_equipamento VARCHAR2, fg_status VARCHAR2, cd_ordem_servico NUMBER, cd_operacao NUMBER);
END SGPA_PROCESSADOR_OS;
/
CREATE OR REPLACE PACKAGE BODY SGPA_PROCESSADOR_OS AS
  PROCEDURE PRC_INT_ORDEM_SERVICO IS
    CURSOR cr_new IS SELECT
      deos.CD_UNIDADE
      , deos.CD_OPERACAO
      , deos.CD_ORDEM_SERVICO
      , deos.FG_SITUACAO,
      cee.CD_EQUIPAMENTO
     FROM DDP_ENTRADA_ORDEM_SERVICO deos
       LEFT JOIN DDP_ORDEM_SERVICO dos
         ON (dos.CD_ORDEM_SERVICO = deos.CD_ORDEM_SERVICO AND dos.CD_UNIDADE = deos.CD_UNIDADE)
       LEFT JOIN CDT_EQUIPAMENTO_EQUIPE cee
         ON (deos.CD_EQUIPE = cee.CD_EQUIPE AND deos.CD_UNIDADE = cee.CD_UNIDADE)
     WHERE dos.CD_ORDEM_SERVICO IS NULL AND cee.CD_EQUIPAMENTO IS NOT NULL
     GROUP BY deos.CD_UNIDADE, deos.CD_OPERACAO, deos.CD_ORDEM_SERVICO, deos.FG_SITUACAO, cee.CD_EQUIPAMENTO;
    CURSOR cr_updated IS
      WITH os_with_equipe AS (
           SELECT
             deos.*,
             cee.CD_EQUIPAMENTO
           FROM DDP_ENTRADA_ORDEM_SERVICO deos
             LEFT JOIN CDT_EQUIPAMENTO_EQUIPE cee
               ON (deos.CD_EQUIPE = cee.CD_EQUIPE AND deos.CD_UNIDADE = cee.CD_UNIDADE)
       ), updated_os_with_reason AS (
       SELECT
         deos.CD_ORDEM_SERVICO AS ENTRADA_ORDEM_SERVICO,
         deos.CD_EQUIPAMENTO   AS ENTRADA_EQUIPAMENTO,
         deos.CD_OPERACAO      AS ENTRADA_OPERACAO,
         deos.FG_SITUACAO      AS ENTRADA_SITUACAO,
         dos.CD_ORDEM_SERVICO  AS OFICIAL_ORDEM_SERVICO,
         cee.CD_EQUIPAMENTO    AS OFICIAL_EQUIPAMENTO,
         dos.CD_OPERACAO       AS OFICIAL_OPERACAO,
         dos.FG_SITUACAO       AS OFICIAL_SITUACAO,
         CASE
         WHEN NVL(dos.CD_EQUIPE, 0) <> NVL(deos.CD_EQUIPE, 0)
           THEN 'MUDANCA_EQUIPE'
         WHEN NVL(dos.CD_OPERACAO, 0) <> NVL(deos.CD_OPERACAO, 0)
           THEN 'MUDANCA_OPERACAO'
         WHEN NVL(dos.FG_SITUACAO, '0') <> NVL(deos.FG_SITUACAO, '0')
           THEN 'MUDANCA_STATUS'
         END                   AS CAUSA
       FROM os_with_equipe deos
         JOIN DDP_ORDEM_SERVICO dos ON (
           dos.CD_ORDEM_SERVICO = deos.CD_ORDEM_SERVICO
           AND dos.CD_UNIDADE = deos.CD_UNIDADE
           AND (
             NVL(dos.CD_EQUIPE, 0) <> NVL(deos.CD_EQUIPE, 0)
             OR NVL(dos.CD_OPERACAO, 0) <> NVL(deos.CD_OPERACAO, 0)
             OR NVL(dos.FG_SITUACAO, '0') <> NVL(deos.FG_SITUACAO, '0')
           ))
         LEFT JOIN CDT_EQUIPAMENTO_EQUIPE cee
           ON (dos.CD_EQUIPE = cee.CD_EQUIPE AND dos.CD_UNIDADE = cee.CD_UNIDADE)
         WHERE cee.CD_EQUIPAMENTO IS NOT NULL
       ), updated_groupped as (SELECT
         t.ENTRADA_ORDEM_SERVICO,
         t.ENTRADA_EQUIPAMENTO,
         t.ENTRADA_OPERACAO,
         t.ENTRADA_SITUACAO,
         t.OFICIAL_ORDEM_SERVICO,
         t.OFICIAL_EQUIPAMENTO,
         t.OFICIAL_OPERACAO,
         t.OFICIAL_SITUACAO,
         t.CAUSA
       FROM updated_os_with_reason t
       GROUP BY t.ENTRADA_ORDEM_SERVICO, t.ENTRADA_EQUIPAMENTO, t.ENTRADA_OPERACAO, t.ENTRADA_SITUACAO,
         t.OFICIAL_ORDEM_SERVICO, t.OFICIAL_EQUIPAMENTO, t.OFICIAL_OPERACAO, t.OFICIAL_SITUACAO, t.CAUSA
      ) SELECT a.ENTRADA_EQUIPAMENTO
           , a.ENTRADA_SITUACAO
           , a.ENTRADA_ORDEM_SERVICO
           , a.ENTRADA_OPERACAO
           , a.CAUSA
        FROM updated_groupped a
      UNION
        SELECT b.OFICIAL_EQUIPAMENTO
          , CASE b.CAUSA
          WHEN 'MUDANCA_EQUIPE' THEN '0'
          WHEN 'MUDANCA_OPERACAO' THEN '0'
          ELSE b.ENTRADA_SITUACAO
          END AS ENTRADA_SITUACAO
          , b.OFICIAL_ORDEM_SERVICO
          , b.OFICIAL_OPERACAO
          , b.CAUSA
        FROM updated_groupped b;
    CURSOR cr_closed IS
      WITH os_deletadas AS (
          SELECT
            dos.CD_UNIDADE,
            dos.CD_ORDEM_SERVICO,
            dos.CD_OPERACAO,
            cee.CD_EQUIPAMENTO
          FROM DDP_ORDEM_SERVICO dos
            LEFT JOIN DDP_ENTRADA_ORDEM_SERVICO deos
              ON (deos.CD_ORDEM_SERVICO = dos.CD_ORDEM_SERVICO AND deos.CD_UNIDADE = dos.CD_UNIDADE)
            LEFT JOIN CDT_EQUIPAMENTO_EQUIPE cee ON (dos.CD_EQUIPE = cee.CD_EQUIPE AND dos.CD_UNIDADE = cee.CD_UNIDADE)
          WHERE deos.CD_ORDEM_SERVICO IS NULL AND dos.FG_SITUACAO = '1'
          GROUP BY dos.CD_UNIDADE, dos.CD_ORDEM_SERVICO, dos.CD_OPERACAO, cee.CD_EQUIPAMENTO
      )
      SELECT * FROM os_deletadas;
    CURSOR cr_all_rowid IS SELECT ROWID FROM DDP_ENTRADA_ORDEM_SERVICO;
    CURSOR cr_all_entrada IS SELECT * FROM DDP_ENTRADA_ORDEM_SERVICO;

    TYPE t_new IS TABLE OF cr_new%ROWTYPE INDEX BY BINARY_INTEGER;
    TYPE t_updated IS TABLE OF cr_updated%ROWTYPE INDEX BY BINARY_INTEGER;
    TYPE t_closed IS TABLE OF cr_closed%ROWTYPE INDEX BY BINARY_INTEGER;
    TYPE t_all_rowid IS TABLE OF cr_all_rowid%ROWTYPE INDEX BY BINARY_INTEGER;
    TYPE t_all_entrada IS TABLE OF cr_all_entrada%ROWTYPE INDEX BY BINARY_INTEGER;

    r_new           t_new;
    r_updated       t_updated;
    r_closed        t_closed;
    r_all_entrada   t_all_entrada;
    r_all_rowid     t_all_rowid;
    v_error_code    NUMBER;
    v_error_message VARCHAR2(32000);
    v_inicio_prc    DATE := SYSDATE;
    v_prc_id        NUMBER := 306;
    v_desc_prc      VARCHAR2(400) := 'SGPA_PROCESSADOR_OS.PRC_INT_ORDEM_SERVICO';
    v_ativo         VARCHAR2(1);
    v_count         NUMBER;

    PROCEDURE LOGGER(P_TIPO VARCHAR2, P_DESC VARCHAR2) IS
    BEGIN
      PRC_SALVAR_LOG_V5(v_prc_id, v_desc_prc, SYSDATE, SYSDATE, v_inicio_prc, P_DESC, P_TIPO, NULL , NULL , NULL, NULL);
    END;

    BEGIN
      BEGIN
        SELECT FG_PRC_ATIVO
        INTO v_ativo
        FROM CFG_SEMAFORO_PROCESSAMENTO
        WHERE CD_ID = v_prc_id;
        EXCEPTION WHEN NO_DATA_FOUND
        THEN
          INSERT INTO CFG_SEMAFORO_PROCESSAMENTO (CD_ID, DESC_NOME_PRC, FG_PRC_ATIVO, DESC_COMENTARIO)
          VALUES (v_prc_id, v_desc_prc, 'F', 'Esta procedure executa um processamento completo das ordens de servico na entrada');
          COMMIT;
          v_ativo := 'F';
      END;

      IF v_ativo = 'F'
      THEN
        UPDATE CFG_SEMAFORO_PROCESSAMENTO
        SET FG_PRC_ATIVO = 'T', DT_HR_EXECUCAO_PROC = SYSDATE
        WHERE CD_ID = v_prc_id;
        COMMIT;

        SELECT COUNT(1) INTO v_count FROM DDP_ENTRADA_ORDEM_SERVICO;
        IF v_count > 0 THEN

          BEGIN
            LOGGER('I', 'REALIZANDO DE_PARA');
            MERGE INTO DDP_ENTRADA_ORDEM_SERVICO deos USING (
            SELECT
              deos.ROWID
              , NVL(dp_operacao.VALOR_SAIDA, deos.CD_OPERACAO) CD_OPERACAO
              , NVL(dp_talhao.VALOR_SAIDA, deos.CD_TALHAO) CD_TALHAO
            FROM DDP_ENTRADA_ORDEM_SERVICO deos
              LEFT JOIN CFG_DEPARA_VALOR_INTEGRACAO dp_operacao ON (
                dp_operacao.TABELA_DESTINO = 'DDP_ORDEM_SERVICO'
                AND dp_operacao.COLUNA_ENTRADA = 'CD_OPERACAO'
                AND CAST( dp_operacao.VALOR_ENTRADA AS NUMBER ) = deos.CD_OPERACAO)
              LEFT JOIN CFG_DEPARA_VALOR_INTEGRACAO dp_talhao ON (
                dp_talhao.TABELA_DESTINO = 'DDP_ORDEM_SERVICO'
                AND dp_talhao.COLUNA_ENTRADA = 'CD_TALHAO'
                AND dp_talhao.VALOR_ENTRADA = deos.CD_TALHAO)) de_para
            ON (deos.ROWID = de_para.ROWID)
            WHEN MATCHED THEN
              UPDATE SET deos.CD_OPERACAO = de_para.CD_OPERACAO
              , deos.CD_TALHAO = de_para.CD_TALHAO;
            COMMIT;
            LOGGER('S', 'DE_PARA REALIZADO');
          EXCEPTION WHEN OTHERS THEN
            v_error_code := SQLCODE;
            v_error_message := SQLERRM;
            LOGGER('E', 'Nao foi possivel concluir PROCESSAR_OS_NOVAS. Error: ' || v_error_code || ': ' || v_error_message);
          END;

          BEGIN
            OPEN cr_new;
            LOGGER('I', 'PROCESSANDO OS NOVAS');
            LOOP FETCH cr_new
            BULK COLLECT INTO r_new LIMIT 1000;
              EXIT WHEN r_new.COUNT = 0;
              FOR T IN 1.. r_new.COUNT LOOP
                GERA_COMANDO(r_new(T).CD_EQUIPAMENTO, r_new(T).FG_SITUACAO, r_new(T).CD_ORDEM_SERVICO,
                             r_new(T).CD_OPERACAO);
              END LOOP;
              COMMIT;
            END LOOP;
            CLOSE cr_new;
            LOGGER('I', 'PROCESSAR OS NOVAS concluido com sucesso.');
            COMMIT;
            EXCEPTION WHEN OTHERS
            THEN
              v_error_code := SQLCODE;
              v_error_message := SQLERRM;
              LOGGER('E', 'Nao foi possivel concluir PROCESSAR_OS_NOVAS. Error: ' || v_error_code || ': ' || v_error_message);
          END;

          BEGIN
            OPEN cr_updated;
            LOGGER('I', 'PROCESSANDO OS ATUALIZADAS');
            LOOP FETCH cr_updated
            BULK COLLECT INTO r_updated LIMIT 1000;
              EXIT WHEN r_updated.COUNT = 0;
              FOR T IN 1.. r_updated.COUNT LOOP
                GERA_COMANDO(r_updated(T).ENTRADA_EQUIPAMENTO, r_updated(T).ENTRADA_SITUACAO, r_updated(T).ENTRADA_ORDEM_SERVICO, r_updated(T).ENTRADA_OPERACAO);
              END LOOP;
              COMMIT;
            END LOOP;
            CLOSE cr_updated;
            LOGGER('I', 'PROCESSAR OS ATUALIZADAS concluido com sucesso.');
            EXCEPTION WHEN OTHERS
            THEN
              v_error_code := SQLCODE;
              v_error_message := SQLERRM;
              LOGGER('E', 'Nao foi possivel concluir PROCESSAR OS ATUALIZADAS. Error: ' || v_error_code || ': ' || v_error_message);
          END;

          BEGIN
            OPEN cr_closed;
            LOGGER('I', 'PROCESSANDO OS FECHADAS');
            LOOP FETCH cr_closed
            BULK COLLECT INTO r_closed LIMIT 1000;
              EXIT WHEN r_closed.COUNT = 0;
              FOR T IN 1.. r_closed.COUNT LOOP
                UPDATE DDP_ORDEM_SERVICO SET FG_SITUACAO = '0' WHERE CD_UNIDADE = r_closed(T).CD_UNIDADE AND CD_ORDEM_SERVICO = r_closed(T).CD_ORDEM_SERVICO;
                GERA_COMANDO(r_closed(T).CD_EQUIPAMENTO, '0', r_closed(T).CD_ORDEM_SERVICO, r_closed(T).CD_OPERACAO);
              END LOOP;
              COMMIT;
            END LOOP;
            CLOSE cr_closed;
            LOGGER('I', 'PROCESSAR OS FECHADAS concluido com sucesso.');
            EXCEPTION WHEN OTHERS
            THEN
              v_error_code := SQLCODE;
              v_error_message := SQLERRM;
              LOGGER('E', 'Nao foi possivel concluir PROCESSAR OS FECHADAS. Error: ' || v_error_code || ': ' || v_error_message);
          END;

          BEGIN
            /*
              Quando a OS ainda existe, mas algum talhao foi retirado da OS. Nesse caso deleta-se a tupla
              dos talhoes que existem na tabela oficial mas nao estao presentes na entrada.
            */
            LOGGER('I', 'DELETANDO ITENS DE OS QUE NAO EXISTEM MAIS');
            DELETE FROM DDP_ORDEM_SERVICO WHERE CD_ID IN (
            SELECT dos.CD_ID FROM DDP_ORDEM_SERVICO dos
              LEFT JOIN DDP_ENTRADA_ORDEM_SERVICO deos ON (
                NVL(dos.CD_ORDEM_SERVICO, 0) = NVL(deos.CD_ORDEM_SERVICO, 0)
                AND NVL(dos.CD_UNIDADE, '0') = NVL(deos.CD_UNIDADE, '0')
                AND NVL(dos.CD_FAZENDA, '0') = NVL(deos.CD_FAZENDA, '0')
                AND NVL(dos.CD_ZONA, '0') = NVL(deos.CD_ZONA, '0')
                AND NVL(dos.CD_TALHAO, '0') = NVL(deos.CD_TALHAO, '0')
                AND NVL(dos.CD_INSUMO, 0) = NVL(deos.CD_INSUMO, 0)
            ) WHERE dos.FG_SITUACAO = '1'
            AND deos.DDP_ID IS NULL);
            COMMIT;
          EXCEPTION WHEN OTHERS THEN
            v_error_code := SQLCODE;
            v_error_message := SQLERRM;
            LOGGER('E', 'Nao foi possivel concluir DELETANDO ITENS DE OS QUE NAO EXISTEM MAIS. Error: ' || v_error_code || ': ' || v_error_message);
          END;

          BEGIN
            LOGGER('I', 'MERGEANDO OS');
            OPEN cr_all_entrada;
            LOOP FETCH cr_all_entrada
            BULK COLLECT INTO r_all_entrada
            LIMIT 1000;
              EXIT WHEN r_all_entrada.COUNT = 0;
              FORALL I IN 1..r_all_entrada.COUNT
              MERGE INTO DDP_ORDEM_SERVICO dos
              USING DUAL
              ON (
                NVL(dos.CD_ORDEM_SERVICO, 0) = NVL(r_all_entrada(I).CD_ORDEM_SERVICO, 0)
                AND NVL(dos.CD_UNIDADE, '0') = NVL(r_all_entrada(I).CD_UNIDADE, '0')
                AND NVL(dos.CD_FAZENDA, '0') = NVL(r_all_entrada(I).CD_FAZENDA, '0')
                AND NVL(dos.CD_ZONA, '0') = NVL(r_all_entrada(I).CD_ZONA, '0')
                AND NVL(dos.CD_TALHAO, '0') = NVL(r_all_entrada(I).CD_TALHAO, '0')
                AND NVL(dos.CD_INSUMO, 0) = NVL(r_all_entrada(I).CD_INSUMO, 0)
              )
              WHEN MATCHED THEN
                UPDATE SET dos.CD_CENTRO_CUSTO  = r_all_entrada(I).CD_CENTRO_CUSTO
                  , dos.CD_EQUIPE               = r_all_entrada(I).CD_EQUIPE
                  , dos.CD_OPERACAO             = r_all_entrada(I).CD_OPERACAO
                  , dos.CD_PERIODO_SAFRA        = r_all_entrada(I).CD_PERIODO_SAFRA
                  , dos.CD_SAFRA                = r_all_entrada(I).CD_SAFRA
                  , dos.DESC_CENTRO_CUSTO       = r_all_entrada(I).DESC_CENTRO_CUSTO
                  , dos.DESC_EQUIPE             = r_all_entrada(I).DESC_EQUIPE
                  , dos.DESC_OPERACAO           = r_all_entrada(I).DESC_OPERACAO
                  , dos.DESC_PERIODO_SAFRA      = r_all_entrada(I).DESC_PERIODO_SAFRA
                  , dos.DESC_SAFRA              = r_all_entrada(I).DESC_SAFRA
                  , dos.DESC_UNIDADE            = r_all_entrada(I).DESC_UNIDADE
                  , dos.DESC_FAZENDA            = r_all_entrada(I).DESC_FAZENDA
                  , dos.DESC_ZONA               = r_all_entrada(I).DESC_ZONA
                  , dos.DESC_TALHAO             = r_all_entrada(I).DESC_TALHAO
                  , dos.ID_ERP_ORDEM_SERVICO    = r_all_entrada(I).ID_ERP_ORDEM_SERVICO
                  , dos.ID_ERP_CENTRO_CUSTO     = r_all_entrada(I).ID_ERP_CENTRO_CUSTO
                  , dos.ID_ERP_EQUIPE           = r_all_entrada(I).ID_ERP_EQUIPE
                  , dos.ID_ERP_OPERACAO         = r_all_entrada(I).ID_ERP_OPERACAO
                  , dos.ID_ERP_PERIODO_SAFRA    = r_all_entrada(I).ID_ERP_PERIODO_SAFRA
                  , dos.ID_ERP_SAFRA            = r_all_entrada(I).ID_ERP_SAFRA
                  , dos.ID_ERP_UNIDADE          = r_all_entrada(I).ID_ERP_UNIDADE
                  , dos.ID_ERP_FAZENDA          = r_all_entrada(I).ID_ERP_FAZENDA
                  , dos.ID_ERP_ZONA             = r_all_entrada(I).ID_ERP_ZONA
                  , dos.ID_ERP_TALHAO           = r_all_entrada(I).ID_ERP_TALHAO
                  , dos.DT_ABERTURA             = TO_DATE(r_all_entrada(I).DT_ABERTURA, 'YYYY-MM-DD HH24:MI:SS')
                  , dos.DT_ENCERRA              = TO_DATE(r_all_entrada(I).DT_ENCERRA, 'YYYY-MM-DD HH24:MI:SS')
                  , dos.FG_SITUACAO             = r_all_entrada(I).FG_SITUACAO
                  , dos.DT_HR_SERVIDOR          = TO_DATE(r_all_entrada(I).DT_HR_SERVIDOR, 'YYYY-MM-DD HH24:MI:SS')
                  , dos.DT_UPDATE               = TO_DATE(r_all_entrada(I).DT_UPDATE, 'YYYY-MM-DD HH24:MI:SS')
                  , dos.DESC_INSUMO             = r_all_entrada(I).DESC_INSUMO
                  , dos.VL_DOSAGEM_TAXA1        = r_all_entrada(I).VL_DOSAGEM_TAXA1
                  , dos.VL_DOSAGEM_TAXA2        = r_all_entrada(I).VL_DOSAGEM_TAXA2
                  , dos.VL_DOSAGEM_TAXA3        = r_all_entrada(I).VL_DOSAGEM_TAXA3
                  , dos.VL_VAZAO                = r_all_entrada(I).VL_VAZAO
                  , dos.VL_RENDIMENTO           = r_all_entrada(I).VL_RENDIMENTO
                  , dos.VL_VELOCIDADE           = r_all_entrada(I).VL_VELOCIDADE
                  , dos.VL_POPULACAO            = r_all_entrada(I).VL_POPULACAO
                  , dos.VL_META_DIARIA          = r_all_entrada(I).VL_META_DIARIA
                  , dos.CD_JORNADA              = r_all_entrada(I).CD_JORNADA
                  , dos.CD_IMPLEMENTO           = r_all_entrada(I).CD_IMPLEMENTO
                  , dos.DT_PREVISAO_EXEC_INICIO = r_all_entrada(I).DT_PREVISAO_EXEC_INICIO
                  , dos.DT_PREVISAO_EXEC_FIM    = r_all_entrada(I).DT_PREVISAO_EXEC_FIM
                  , dos.CD_OPERADOR             = r_all_entrada(I).CD_OPERADOR
                  , dos.DESC_OPERADOR           = r_all_entrada(I).DESC_OPERADOR
                  , dos.DESC_JORNADA            = r_all_entrada(I).DESC_JORNADA
                  , dos.DESC_IMPLEMENTO         = r_all_entrada(I).DESC_IMPLEMENTO
                  , CD_TEMP_MIN                 = r_all_entrada(I).CD_TEMP_MIN
                  , CD_TEMP_MAX                 = r_all_entrada(I).CD_TEMP_MAX
                  , CD_UR_MIN                   = r_all_entrada(I).CD_UR_MIN
                  , CD_UR_MAX                   = r_all_entrada(I).CD_UR_MAX
                  , CD_VENTO_MIN                = r_all_entrada(I).CD_VENTO_MIN
                  , CD_VENTO_MAX                = r_all_entrada(I).CD_VENTO_MAX
              WHEN NOT MATCHED THEN
                INSERT (
                  CD_ORDEM_SERVICO
                  , CD_CENTRO_CUSTO
                  , CD_EQUIPE
                  , CD_OPERACAO
                  , CD_PERIODO_SAFRA
                  , CD_SAFRA
                  , CD_UNIDADE
                  , CD_FAZENDA
                  , CD_ZONA
                  , CD_TALHAO
                  , DESC_CENTRO_CUSTO
                  , DESC_EQUIPE
                  , DESC_OPERACAO
                  , DESC_PERIODO_SAFRA
                  , DESC_SAFRA
                  , DESC_UNIDADE
                  , DESC_FAZENDA
                  , DESC_ZONA
                  , DESC_TALHAO
                  , ID_ERP_ORDEM_SERVICO
                  , ID_ERP_CENTRO_CUSTO
                  , ID_ERP_EQUIPE
                  , ID_ERP_OPERACAO
                  , ID_ERP_PERIODO_SAFRA
                  , ID_ERP_SAFRA
                  , ID_ERP_UNIDADE
                  , ID_ERP_FAZENDA
                  , ID_ERP_ZONA
                  , ID_ERP_TALHAO
                  , DT_ABERTURA
                  , DT_ENCERRA
                  , FG_SITUACAO
                  , DT_HR_SERVIDOR
                  , DT_UPDATE
                  , CD_INSUMO
                  , DESC_INSUMO
                  , VL_DOSAGEM_TAXA1
                  , VL_DOSAGEM_TAXA2
                  , VL_DOSAGEM_TAXA3
                  , VL_VAZAO
                  , VL_RENDIMENTO
                  , VL_VELOCIDADE
                  , VL_POPULACAO
                  , VL_META_DIARIA
                  , CD_JORNADA
                  , CD_IMPLEMENTO
                  , DT_PREVISAO_EXEC_INICIO
                  , DT_PREVISAO_EXEC_FIM
                  , CD_OPERADOR
                  , DESC_OPERADOR
                  , DESC_JORNADA
                  , DESC_IMPLEMENTO
                  , CD_TEMP_MIN
                  , CD_TEMP_MAX
                  , CD_UR_MIN
                  , CD_UR_MAX
                  , CD_VENTO_MIN
                  , CD_VENTO_MAX)
                VALUES (r_all_entrada(I).CD_ORDEM_SERVICO
                  , r_all_entrada(I).CD_CENTRO_CUSTO
                  , r_all_entrada(I).CD_EQUIPE
                  , r_all_entrada(I).CD_OPERACAO
                  , r_all_entrada(I).CD_PERIODO_SAFRA
                  , r_all_entrada(I).CD_SAFRA
                  , r_all_entrada(I).CD_UNIDADE
                  , r_all_entrada(I).CD_FAZENDA
                  , r_all_entrada(I).CD_ZONA
                  , r_all_entrada(I).CD_TALHAO
                  , r_all_entrada(I).DESC_CENTRO_CUSTO
                  , r_all_entrada(I).DESC_EQUIPE
                  , r_all_entrada(I).DESC_OPERACAO
                  , r_all_entrada(I).DESC_PERIODO_SAFRA
                  , r_all_entrada(I).DESC_SAFRA
                  , r_all_entrada(I).DESC_UNIDADE
                  , r_all_entrada(I).DESC_FAZENDA
                  , r_all_entrada(I).DESC_ZONA
                  , r_all_entrada(I).DESC_TALHAO
                  , r_all_entrada(I).ID_ERP_ORDEM_SERVICO
                  , r_all_entrada(I).ID_ERP_CENTRO_CUSTO
                  , r_all_entrada(I).ID_ERP_EQUIPE
                  , r_all_entrada(I).ID_ERP_OPERACAO
                  , r_all_entrada(I).ID_ERP_PERIODO_SAFRA
                  , r_all_entrada(I).ID_ERP_SAFRA
                  , r_all_entrada(I).ID_ERP_UNIDADE
                  , r_all_entrada(I).ID_ERP_FAZENDA
                  , r_all_entrada(I).ID_ERP_ZONA
                  , r_all_entrada(I).ID_ERP_TALHAO
                  , TO_DATE(r_all_entrada(I).DT_ABERTURA, 'YYYY-MM-DD HH24:MI:SS')
                  , TO_DATE(r_all_entrada(I).DT_ENCERRA, 'YYYY-MM-DD HH24:MI:SS')
                  , r_all_entrada(I).FG_SITUACAO
                  , TO_DATE(r_all_entrada(I).DT_HR_SERVIDOR, 'YYYY-MM-DD HH24:MI:SS')
                  , TO_DATE(r_all_entrada(I).DT_UPDATE, 'YYYY-MM-DD HH24:MI:SS')
                  , r_all_entrada(I).CD_INSUMO
                  , r_all_entrada(I).DESC_INSUMO
                  , r_all_entrada(I).VL_DOSAGEM_TAXA1
                  , r_all_entrada(I).VL_DOSAGEM_TAXA2
                  , r_all_entrada(I).VL_DOSAGEM_TAXA3
                  , r_all_entrada(I).VL_VAZAO
                  , r_all_entrada(I).VL_RENDIMENTO
                  , r_all_entrada(I).VL_VELOCIDADE
                  , r_all_entrada(I).VL_POPULACAO
                  , r_all_entrada(I).VL_META_DIARIA
                  , r_all_entrada(I).CD_JORNADA
                  , r_all_entrada(I).CD_IMPLEMENTO
                  , r_all_entrada(I).DT_PREVISAO_EXEC_INICIO
                  , r_all_entrada(I).DT_PREVISAO_EXEC_FIM
                  , r_all_entrada(I).CD_OPERADOR
                  , r_all_entrada(I).DESC_OPERADOR
                  , r_all_entrada(I).DESC_JORNADA
                  , r_all_entrada(I).DESC_IMPLEMENTO
                  , r_all_entrada(I).CD_TEMP_MIN
                  , r_all_entrada(I).CD_TEMP_MAX
                  , r_all_entrada(I).CD_UR_MIN
                  , r_all_entrada(I).CD_UR_MAX
                  , r_all_entrada(I).CD_VENTO_MIN
                  , r_all_entrada(I).CD_VENTO_MAX
                );
              COMMIT;
            END LOOP;

            CLOSE cr_all_entrada;
            EXCEPTION WHEN OTHERS
            THEN
              v_error_code := SQLCODE;
              v_error_message := SQLERRM;
              LOGGER('E', 'Nao foi possivel concluir MERGEANDO OS. Error: ' || v_error_code || ': ' || v_error_message);
          END;

          BEGIN
            OPEN cr_all_rowid;
            LOGGER('I', 'DELETANDO DA ENTRADA');
            LOOP FETCH cr_all_rowid
            BULK COLLECT INTO r_all_rowid LIMIT 1000;
              EXIT WHEN r_all_rowid.COUNT = 0;
              FOR T IN 1.. r_all_rowid.COUNT LOOP
                DELETE FROM DDP_ENTRADA_ORDEM_SERVICO
                WHERE ROWID = r_all_rowid(T).ROWID;
              END LOOP;
              COMMIT;
            END LOOP;
            CLOSE cr_all_rowid;
            LOGGER('I', 'DELETANDO DA ENTRADA concluido com sucesso.');
            EXCEPTION WHEN OTHERS
            THEN
              v_error_code := SQLCODE;
              v_error_message := SQLERRM;
              LOGGER('E', 'Nao foi possivel concluir DELETANDO DA ENTRADA. Error: ' || v_error_code || ': ' || v_error_message);
          END;
        END IF;
        UPDATE CFG_SEMAFORO_PROCESSAMENTO
        SET FG_PRC_ATIVO = 'F', VL_TEMPO_EXECUCAO = (SYSDATE - DT_HR_EXECUCAO_PROC) * 86400
        WHERE CD_ID = v_prc_id;
        COMMIT;
      END IF;
    END PRC_INT_ORDEM_SERVICO;

  PROCEDURE GERA_COMANDO(p_equipamento VARCHAR2, fg_status VARCHAR2, cd_ordem_servico NUMBER, cd_operacao NUMBER) IS
    v_comando        COMANDO_ONLINE_BUILDER := COMANDO_ONLINE_BUILDER('');
    v_str_comando    VARCHAR2(100);
    v_equipamento    VARCHAR2(8);
    v_ordem_servico  VARCHAR2(10);
    v_operacao       VARCHAR2(10);
    v_status         VARCHAR2(1);
    v_owner          VARCHAR2(30);
    v_enabled        VARCHAR2(1);
    v_id_inserido    NUMBER;
    v_comando_gerado VARCHAR2(100);
    FUNCTION FILL(p_thevalue VARCHAR2, p_max NUMBER)
      RETURN VARCHAR2 IS
      v_result VARCHAR2(255);
      BEGIN
        SELECT CASE
               WHEN LENGTH(p_thevalue) < p_max
                 THEN LPAD(p_thevalue, p_max, '0')
               ELSE p_thevalue
               END
        INTO v_result
        FROM DUAL;
        RETURN v_result;
      END;
    BEGIN
      SELECT VL_PARAMETRO
      INTO v_enabled
      FROM CFG_PARAMETROS_GERAIS
      WHERE CD_ID = 351;
      IF v_enabled <> '0' AND p_equipamento IS NOT NULL
      THEN
        SELECT USER
        into v_owner
        FROM DUAL;
        v_equipamento := FILL(p_equipamento, 8);
        v_ordem_servico := FILL(TO_CHAR(cd_ordem_servico), 10);
        v_operacao := FILL(TO_CHAR(cd_operacao), 10);
        v_status := SUBSTR(fg_status, 1, 1);

        v_comando.APPEND('SET');
        v_comando.APPEND(v_equipamento);
        v_comando.APPEND('AS');
        v_comando.APPEND(v_status);
        v_comando.APPEND(v_ordem_servico);
        v_comando.APPEND(v_operacao);
        v_str_comando := v_comando.GET_COMANDO();
        INSERT INTO DDN_MENSAGES (CD_EQUIPAMENTO, CD_TP_MENSAGE, CD_USUARIO, VL_MENSAGE)
        VALUES (p_equipamento, fg_status, v_owner, v_str_comando)
        RETURNING CD_ID, VL_MENSAGE INTO v_id_inserido, v_comando_gerado;
        INSERT INTO CDT_COMANDO_ORDEM_SERVICO (CD_ORDEM_SERVICO, CD_COMANDO, VL_COMANDO)
        VALUES (cd_ordem_servico, v_id_inserido, v_comando_gerado);
      END IF;
    END GERA_COMANDO;
END SGPA_PROCESSADOR_OS;
/