GRANT INSERT, SELECT ON SGPA_MAP.CLIENTE TO BOMFUTURO;
GRANT INSERT, UPDATE, SELECT ON SGPA_MAP.TMP_MAPA_VAR_BATCH TO BOMFUTURO;
GRANT INSERT, UPDATE, DELETE, SELECT ON SGPA_MAP.TMP_MAPA_VAR TO BOMFUTURO;
GRANT INSERT, UPDATE, DELETE, SELECT ON SGPA_MAP.GEO_LAYER_OPERACAO_TALHAO TO BOMFUTURO;

create or replace PACKAGE PKG_ANALYTIC_DATA_UTILS AS
  TYPE t_glmv_with_rowid IS RECORD (
      CD_ID                          NUMBER,
      CD_EQUIPAMENTO                 VARCHAR2(15),
      DESC_EQUIPAMENTO               VARCHAR2(50),
      FG_TP_EQUIPAMENTO              NUMBER(11),
      FG_FRENTE_TRABALHO             NUMBER(1),
      CD_EQUIPE                      NUMBER(11),
      DT_HR_UTC_INICIAL              DATE,
      DT_HR_LOCAL_INICIAL            DATE,
      VL_LATITUDE_INICIAL            NUMBER(23, 15),
      VL_LONGITUDE_INICIAL           NUMBER(23, 15),
      CD_FAZENDA_INICIAL             VARCHAR2(10),
      CD_ZONA_INICIAL                VARCHAR2(10),
      CD_TALHAO_INICIAL              VARCHAR2(10),
      VL_TEMPO_SEGUNDOS              NUMBER(20),
      DT_HR_UTC_FINAL                DATE,
      DT_HR_LOCAL_FINAL              DATE,
      VL_LATITUDE_FINAL              NUMBER(23, 15),
      VL_LONGITUDE_FINAL             NUMBER(23, 15),
      CD_FAZENDA_FINAL               VARCHAR2(10),
      CD_ZONA_FINAL                  VARCHAR2(10),
      CD_TALHAO_FINAL                VARCHAR2(10),
      VL_DISTANCIA_METROS            NUMBER(11, 5),
      CD_OPERACAO                    NUMBER(11),
      DESC_OPERACAO                  VARCHAR2(50),
      VL_VELOCIDADE                  NUMBER(11, 5),
      CD_OPERADOR                    NUMBER(11),
      DESC_OPERADOR                  VARCHAR2(50),
      CD_ESTADO                      VARCHAR2(5),
      QT_SECAO_PULVERIZADOR_ANTERIOR NUMBER(10),
      CD_IMPLEMENTO                  VARCHAR2(15),
      VL_VELOCIDADE_VENTO            NUMBER(11, 5),
      VL_TEMPERATURA                 NUMBER(11, 5),
      VL_UMIDADE                     NUMBER(11, 5),
      VL_CONSUMO_INSTANTANEO         NUMBER(11, 5),
      VL_RPM                         NUMBER(11, 5),
      VL_TEMPERATURA_MOTOR           NUMBER(11, 5),
      VL_ORDEM_SERVICO               NUMBER,
      VL_ALARME                      VARCHAR2(50),
      VL_ALERTA_CLIMA                VARCHAR2(50),
      VL_ALARME_CLIMA                VARCHAR2(50),
      QT_SECAO_PULVERIZADOR          VARCHAR2(60),
      VL_HECTARES_HORA               NUMBER(23, 15),
      VL_AREA_HEC_INTERSEC_TALHAOINI NUMBER(23, 15),
      VL_AREA_HEC_INTERSEC_TALHAOFIM NUMBER(23, 15),
      CD_OPERAC_PARADA               NUMBER(11),
      DESC_OPERAC_PARADA             VARCHAR2(50),
      CD_UNIDADE                     NUMBER(11),
      VL_HORIMETRO_INICIAL           NUMBER(11, 3),
      VL_HORIMETRO_FINAL             NUMBER(11, 3),
      VL_LARGURA_IMPLEMENTO          NUMBER(10, 2),
      CD_JORNADA                     NUMBER(10),
      CD_ID_DETALHESOP               NUMBER,
      CD_OPERACAO_CB                 NUMBER(11),
      CD_TIPO_BICO                   NUMBER(11),
      DESC_TIPO_BICO                 VARCHAR2(100),
      VL_RENDIMENTO_COLHEITA         NUMBER(23, 15),
      VL_UMIDADE_GRAOS               NUMBER(23, 15),
      VL_HECTARES_HORA_M             NUMBER(23, 15),
      VL_AREAHEC_INTERSECTALHAOINI_M NUMBER(23, 15),
      VL_AREAHEC_INTERSECTALHAOFIM_M NUMBER(23, 15),
      VL_PONTO_ORVALHO               NUMBER(11, 5),
      VL_PARTICULAS_OLEO             VARCHAR2(100),
      VL_RENDIMENTO_COLHEITA_M       NUMBER(23, 15),
      VL_UMIDADE_GRAOS_M             NUMBER(23, 15),
      VL_VAZAO_LITROS_HA             NUMBER(23, 15),
      VL_VAZAO_LITROS_MIN            NUMBER(23, 15),
      VL_VAZAO_LITROS_HA_M           NUMBER(23, 15),
      VL_VAZAO_LITROS_MIN_M          NUMBER(23, 15),
      VL_DOSAGEM_TAXA1               NUMBER(11),
      VL_DOSAGEM_TAXA2               NUMBER(11),
      VL_DOSAGEM_TAXA3               NUMBER(11),
      VL_PRESSAO_BOMBA               NUMBER(23, 15),
      VL_PRESSAO_BOMBA_M             NUMBER(23, 15),
      ROW_ID                         ROWID
    );

  FUNCTION GET_USER_ID(p_owner VARCHAR2 DEFAULT USER) RETURN NUMBER;
  PROCEDURE INVALIDATE(id_cliente NUMBER, p_dataInicial DATE, p_dataFinal DATE, p_operacoes VARCHAR2 DEFAULT NULL);
  PROCEDURE LOAD_ANALYTIC_DATA(id_cliente NUMBER, p_dataInicial DATE, p_dataFinal DATE, p_operacoes VARCHAR2 DEFAULT NULL);
  PROCEDURE ON_REG_MODIFY(id_cliente NUMBER, p_rowid ROWID);
  PROCEDURE LOAD_BY_ROWID(id_cliente NUMBER, batch_id NUMBER, p_rowid ROWID);
  PROCEDURE LOAD_BY_REG(id_cliente NUMBER, batch_id NUMBER, p_reg t_glmv_with_rowid);

END PKG_ANALYTIC_DATA_UTILS;
/
create or replace PACKAGE BODY PKG_ANALYTIC_DATA_UTILS AS

  FUNCTION GET_USER_ID(p_owner VARCHAR2 DEFAULT USER) RETURN NUMBER IS
    id_cliente NUMBER := -1;
    BEGIN
      BEGIN
        SELECT CD_ID
        INTO id_cliente
        FROM SGPA_MAP.CLIENTE
        WHERE OWNER = p_owner;
        EXCEPTION WHEN NO_DATA_FOUND
        THEN
          INSERT INTO SGPA_MAP.CLIENTE (OWNER) VALUES (p_owner)
          RETURNING CD_ID INTO id_cliente;
      END;
      return id_cliente;
    END;

  PROCEDURE INVALIDATE(id_cliente NUMBER, p_dataInicial DATE, p_dataFinal DATE, p_operacoes VARCHAR2 DEFAULT NULL) IS
    v_count NUMBER := 0;
    BEGIN
      FOR reg IN (
        SELECT ROWID
        FROM BOMFUTURO.GEO_LAYER_MAPA_VAR glmv
        WHERE
          (
            p_dataInicial BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL
            OR p_dataFinal BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL
            OR (
              DT_HR_UTC_INICIAL BETWEEN p_dataInicial AND p_dataFinal
              AND DT_HR_UTC_FINAL BETWEEN p_dataInicial AND p_dataFinal
            )
          )
          AND CD_OPERACAO IN (NVL(p_operacoes, CD_OPERACAO)))
      LOOP
        ON_REG_MODIFY(id_cliente, reg.ROWID);
        v_count := v_count + 1;
        IF v_count = 1000 THEN
          v_count := 0;
          COMMIT;
        end if;
      END LOOP;
      COMMIT;
    END;

  PROCEDURE LOAD_ANALYTIC_DATA(id_cliente NUMBER, p_dataInicial DATE, p_dataFinal DATE, p_operacoes VARCHAR2 DEFAULT NULL) IS
    CURSOR CR_GET_DATA(cr_p_dataInicial DATE DEFAULT TO_DATE('1970-01-01', 'RRRR-MM-DD'), cr_p_dataFinal DATE DEFAULT SYSDATE, cr_p_operacoes VARCHAR2 DEFAULT NULL) IS
        SELECT glmv.*, ROWID as ROW_ID
          FROM BOMFUTURO.GEO_LAYER_MAPA_VAR glmv
          WHERE
            (
              /* Pega todos os KIJOs 33 entre as datas de inicio ou cujo incio ou fim sao
              cortados pelos parametros de data inicial e final
              :dataInicial                                                 :dataFinal
                    |           *----------------*                              |
                 *--|---*                                                 *-----|------*
              */
              cr_p_dataInicial BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL
              OR cr_p_dataFinal BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL
              OR (
                DT_HR_UTC_INICIAL BETWEEN cr_p_dataInicial AND cr_p_dataFinal
                AND DT_HR_UTC_FINAL BETWEEN cr_p_dataInicial AND cr_p_dataFinal
              )
            )
            AND CD_OPERACAO IN (NVL(cr_p_operacoes, CD_OPERACAO));

      v_row            t_glmv_with_rowid;
      v_count          NUMBER := 0;
      v_batch          NUMBER;
    BEGIN
      OPEN CR_GET_DATA(p_dataInicial, p_dataFinal, p_operacoes);
      INSERT INTO SGPA_MAP.TMP_MAPA_VAR_BATCH (CD_CLIENTE) VALUES (id_cliente) RETURNING CD_ID INTO v_batch;
      COMMIT;
      LOOP FETCH CR_GET_DATA INTO v_row;
        EXIT WHEN CR_GET_DATA%NOTFOUND;
        LOAD_BY_REG(id_cliente, v_batch, v_row);
        v_count := v_count + 1;
        IF v_count = 10000
        THEN
          v_count := 0;
          UPDATE SGPA_MAP.TMP_MAPA_VAR_BATCH SET STATUS = 'FULL' WHERE CD_CLIENTE = id_cliente AND CD_ID = v_batch;
          INSERT INTO SGPA_MAP.TMP_MAPA_VAR_BATCH (CD_CLIENTE) VALUES (id_cliente) RETURNING CD_ID INTO v_batch;
          COMMIT;
        END IF;
      END LOOP;
      CLOSE CR_GET_DATA;
      COMMIT;
    END;

  PROCEDURE ON_REG_MODIFY(id_cliente NUMBER, p_rowid ROWID) IS
    BEGIN
      DELETE FROM SGPA_MAP.TMP_MAPA_VAR WHERE CD_CLIENTE = id_cliente AND ORIGIN_ROWID = p_rowid;
      DELETE FROM SGPA_MAP.GEO_LAYER_OPERACAO_TALHAO WHERE CD_CLIENTE = id_cliente AND ORIGIN_ROWID = p_rowid;
      COMMIT;
    END;

  PROCEDURE LOAD_BY_ROWID(id_cliente NUMBER, batch_id NUMBER, p_rowid ROWID) IS
    CURSOR CR_GET_DATA(cr_p_rowid ROWID) IS
      SELECT glmv.*, ROWID as ROW_ID
        FROM BOMFUTURO.GEO_LAYER_MAPA_VAR glmv
        WHERE glmv.ROWID = cr_p_rowid;

    v_row t_glmv_with_rowid;
    BEGIN
      OPEN CR_GET_DATA(p_rowid);
      FETCH CR_GET_DATA INTO v_row;
      CLOSE CR_GET_DATA;
      LOAD_BY_REG(id_cliente, batch_id, v_row);
      COMMIT;
    END;

  PROCEDURE LOAD_BY_REG(id_cliente NUMBER, batch_id NUMBER, p_reg t_glmv_with_rowid) IS
    BEGIN
      INSERT INTO SGPA_MAP.TMP_MAPA_VAR
      (CD_CLIENTE
        , BATCH_ID
        , ORIGIN_ROWID
        , CD_MAPA_VAR
        , CD_EQUIPAMENTO
        , DESC_EQUIPAMENTO
        , FG_TP_EQUIPAMENTO
        , FG_FRENTE_TRABALHO
        , CD_EQUIPE
        , DT_HR_UTC_INICIAL
        , DT_HR_LOCAL_INICIAL
        , VL_LATITUDE_INICIAL
        , VL_LONGITUDE_INICIAL
        , CD_FAZENDA_INICIAL
        , CD_ZONA_INICIAL
        , CD_TALHAO_INICIAL
        , VL_TEMPO_SEGUNDOS
        , DT_HR_UTC_FINAL
        , DT_HR_LOCAL_FINAL
        , VL_LATITUDE_FINAL
        , VL_LONGITUDE_FINAL
        , CD_FAZENDA_FINAL
        , CD_ZONA_FINAL
        , CD_TALHAO_FINAL
        , VL_DISTANCIA_METROS
        , CD_OPERACAO
        , DESC_OPERACAO
        , VL_VELOCIDADE
        , CD_OPERADOR
        , DESC_OPERADOR
        , CD_ESTADO
        , QT_SECAO_PULVERIZADOR_ANTERIOR
        , CD_IMPLEMENTO
        , VL_VELOCIDADE_VENTO
        , VL_TEMPERATURA
        , VL_UMIDADE
        , VL_CONSUMO_INSTANTANEO
        , VL_RPM, VL_TEMPERATURA_MOTOR
        , VL_ORDEM_SERVICO
        , VL_ALARME
        , VL_ALERTA_CLIMA
        , VL_ALARME_CLIMA
        , QT_SECAO_PULVERIZADOR
        , VL_HECTARES_HORA
        , VL_AREA_HEC_INTERSEC_TALHAOINI
        , VL_AREA_HEC_INTERSEC_TALHAOFIM
        , CD_OPERAC_PARADA
        , DESC_OPERAC_PARADA
        , CD_UNIDADE
        , VL_HORIMETRO_INICIAL
        , VL_HORIMETRO_FINAL
        , VL_LARGURA_IMPLEMENTO
        , CD_JORNADA
        , CD_ID_DETALHESOP
        , CD_OPERACAO_CB
        , CD_TIPO_BICO
        , DESC_TIPO_BICO
        , VL_RENDIMENTO_COLHEITA
        , VL_UMIDADE_GRAOS
        , VL_HECTARES_HORA_M
        , VL_AREAHEC_INTERSECTALHAOINI_M
        , VL_AREAHEC_INTERSECTALHAOFIM_M
        , VL_PONTO_ORVALHO
        , VL_PARTICULAS_OLEO
        , VL_RENDIMENTO_COLHEITA_M
        , VL_UMIDADE_GRAOS_M
        , VL_VAZAO_LITROS_HA
        , VL_VAZAO_LITROS_MIN
        , VL_VAZAO_LITROS_HA_M
        , VL_VAZAO_LITROS_MIN_M
        , VL_DOSAGEM_TAXA1
        , VL_DOSAGEM_TAXA2
        , VL_DOSAGEM_TAXA3
        , VL_PRESSAO_BOMBA
        , VL_PRESSAO_BOMBA_M
      ) VALUES (
        id_cliente
        , batch_id
        , p_reg.ROW_ID
        , p_reg.CD_ID
        , p_reg.CD_EQUIPAMENTO
        , p_reg.DESC_EQUIPAMENTO
        , p_reg.FG_TP_EQUIPAMENTO
        , p_reg.FG_FRENTE_TRABALHO
        , p_reg.CD_EQUIPE
        , p_reg.DT_HR_UTC_INICIAL
        , p_reg.DT_HR_LOCAL_INICIAL
        , p_reg.VL_LATITUDE_INICIAL
        , p_reg.VL_LONGITUDE_INICIAL
        , p_reg.CD_FAZENDA_INICIAL
        , p_reg.CD_ZONA_INICIAL
        , p_reg.CD_TALHAO_INICIAL
        , p_reg.VL_TEMPO_SEGUNDOS
        , p_reg.DT_HR_UTC_FINAL
        , p_reg.DT_HR_LOCAL_FINAL
        , p_reg.VL_LATITUDE_FINAL
        , p_reg.VL_LONGITUDE_FINAL
        , p_reg.CD_FAZENDA_FINAL
        , p_reg.CD_ZONA_FINAL
        , p_reg.CD_TALHAO_FINAL
        , p_reg.VL_DISTANCIA_METROS
        , p_reg.CD_OPERACAO
        , p_reg.DESC_OPERACAO
        , p_reg.VL_VELOCIDADE
        , p_reg.CD_OPERADOR
        , p_reg.DESC_OPERADOR
        , p_reg.CD_ESTADO
        , p_reg.QT_SECAO_PULVERIZADOR_ANTERIOR
        , p_reg.CD_IMPLEMENTO
        , p_reg.VL_VELOCIDADE_VENTO
        , p_reg.VL_TEMPERATURA
        , p_reg.VL_UMIDADE
        , p_reg.VL_CONSUMO_INSTANTANEO
        , p_reg.VL_RPM
        , p_reg.VL_TEMPERATURA_MOTOR
        , p_reg.VL_ORDEM_SERVICO
        , p_reg.VL_ALARME
        , p_reg.VL_ALERTA_CLIMA
        , p_reg.VL_ALARME_CLIMA
        , p_reg.QT_SECAO_PULVERIZADOR
        , p_reg.VL_HECTARES_HORA
        , p_reg.VL_AREA_HEC_INTERSEC_TALHAOINI
        , p_reg.VL_AREA_HEC_INTERSEC_TALHAOFIM
        , p_reg.CD_OPERAC_PARADA
        , p_reg.DESC_OPERAC_PARADA
        , p_reg.CD_UNIDADE
        , p_reg.VL_HORIMETRO_INICIAL
        , p_reg.VL_HORIMETRO_FINAL
        , p_reg.VL_LARGURA_IMPLEMENTO
        , p_reg.CD_JORNADA
        , p_reg.CD_ID_DETALHESOP
        , p_reg.CD_OPERACAO_CB
        , p_reg.CD_TIPO_BICO
        , p_reg.DESC_TIPO_BICO
        , p_reg.VL_RENDIMENTO_COLHEITA
        , p_reg.VL_UMIDADE_GRAOS
        , p_reg.VL_HECTARES_HORA_M
        , p_reg.VL_AREAHEC_INTERSECTALHAOINI_M
        , p_reg.VL_AREAHEC_INTERSECTALHAOFIM_M
        , p_reg.VL_PONTO_ORVALHO
        , p_reg.VL_PARTICULAS_OLEO
        , p_reg.VL_RENDIMENTO_COLHEITA_M
        , p_reg.VL_UMIDADE_GRAOS_M
        , p_reg.VL_VAZAO_LITROS_HA
        , p_reg.VL_VAZAO_LITROS_MIN
        , p_reg.VL_VAZAO_LITROS_HA_M
        , p_reg.VL_VAZAO_LITROS_MIN_M
        , p_reg.VL_DOSAGEM_TAXA1
        , p_reg.VL_DOSAGEM_TAXA2
        , p_reg.VL_DOSAGEM_TAXA3
        , p_reg.VL_PRESSAO_BOMBA
        , p_reg.VL_PRESSAO_BOMBA_M
      );
    END;
END PKG_ANALYTIC_DATA_UTILS;
/