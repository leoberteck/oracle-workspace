CALL PRC_ADM_PLUVIOMETRO(null);
/
ALTER TABLE TMP_PLUVIOMETRO ADD VL_CONTADOR_ORG NUMBER(5);
/
CREATE OR REPLACE TRIGGER TRG_BI_TMP_PLUVIOMETRO
  BEFORE INSERT ON TMP_PLUVIOMETRO FOR EACH ROW
BEGIN
  :NEW.VL_CONTADOR_ORG := :NEW.VL_CONTADOR_CHUVA;
END;
/
-- INSERT INTO TMP_PLUVIOMETRO
-- ( CD_ID
-- , FG_TP_REGISTRO
-- , CD_EQUIPAMENTO
-- , FG_TP_EQUIPAMENTO
-- , FG_TP_COMUNICACAO
-- , CD_VERSAO_FIRMWARE
-- , DT_HR_UTC
-- , DT_HR_LOCAL
-- , DT_HR_SERVIDOR
-- , DT_HR_SERVIDOR_DMZ
-- , VL_LATITUDE
-- , VL_LONGITUDE
-- , CD_FAZENDA
-- , CD_ZONA
-- , CD_TALHAO
-- , VL_ALARME
-- , VL_FOLHA_MOLHADA
-- , VL_UMIDADE_SOLO
-- , VL_RSSI
-- , VL_SNR
-- , VL_BATERIA
-- , VL_FRAME_ID
-- , VL_CONTADOR_CHUVA
-- , VL_UMIDADE_SOLO_2
-- , FG_PROCESSADO
-- , VL_DIRECAO_VENTO
-- , VL_VEL_VENTO
-- , VL_VEL_VENTO_PICO
-- , VL_TEMPERATURA
-- , VL_UMIDADE
-- , VL_RADIACAO_SOLAR
-- , VL_PONTO_ORVALHO
-- , VL_PRESSAO_ATMOSFERICA
-- , VL_UMIDADE_SOLO_3
-- , CD_ESTADO)
-- SELECT * FROM (
--   SELECT CD_ID
--   , FG_TP_REGISTRO
--   , CD_EQUIPAMENTO
--   , FG_TP_EQUIPAMENTO
--   , FG_TP_COMUNICACAO
--   , CD_VERSAO_FIRMWARE
--   , DT_HR_UTC
--   , DT_HR_LOCAL
--   , DT_HR_SERVIDOR
--   , DT_HR_SERVIDOR_DMZ
--   , VL_LATITUDE
--   , VL_LONGITUDE
--   , CD_FAZENDA
--   , CD_ZONA
--   , CD_TALHAO
--   , VL_ALARME
--   , VL_FOLHA_MOLHADA
--   , VL_UMIDADE_SOLO
--   , VL_RSSI
--   , VL_SNR
--   , VL_BATERIA
--   , VL_FRAME_ID
--   , VL_CONTADOR_CHUVA
--   , VL_UMIDADE_SOLO_2
--   , 'F'
--   , VL_DIRECAO_VENTO
--   , VL_VEL_VENTO
--   , VL_VEL_VENTO_PICO
--   , VL_TEMPERATURA
--   , VL_UMIDADE
--   , VL_RADIACAO_SOLAR
--   , VL_PONTO_ORVALHO
--   , VL_PRESSAO_ATMOSFERICA
--   , VL_UMIDADE_SOLO_3
--   , CD_ESTADO
--   FROM DDP_PLUVIOMETRO
--   WHERE CD_EQUIPAMENTO in (207, 322)
--     AND DT_HR_UTC >= TO_DATE('2018-10-21 03:00:00', 'YYYY-MM-DD HH24:MI:SS')
--   ORDER BY DT_HR_UTC DESC
-- );
-- /
-- COMMIT;
/
CREATE TABLE CFG_CONTROLE_PLUVIOMETRO(
  CD_EQUIPAMENTO VARCHAR2(15) PRIMARY KEY,
  VL_FRAME_ID NUMBER(3),
  VL_CONTADOR NUMBER(5),
  VL_UTIMO_ORG_ANALISADO NUMBER(5),
  VL_FLAG VARCHAR2(1) DEFAULT 'F'
);
/
-- --Carga inicial na tabela de controle.
-- INSERT INTO CFG_CONTROLE_PLUVIOMETRO (CD_EQUIPAMENTO, VL_FRAME_ID, VL_CONTADOR, VL_UTIMO_ORG_ANALISADO)
--   SELECT
--     CD_EQUIPAMENTO,
--     VL_FRAME_ID,
--     VL_CONTADOR_CHUVA,
--     ULTIMO
--   FROM (
--     SELECT
--       t.*,
--       ROW_NUMBER()
--       OVER (
--         PARTITION BY CD_EQUIPAMENTO
--         ORDER BY CD_EQUIPAMENTO ) AS RN
--     FROM (
--            SELECT
--              CD_EQUIPAMENTO,
--              VL_FRAME_ID,
--              VL_CONTADOR_CHUVA,
--              ULTIMO
--            FROM (
--              SELECT
--                t.*,
--                LAG(VL_CONTADOR_CHUVA, 1, 0)
--                OVER (
--                  ORDER BY CD_EQUIPAMENTO, RN DESC ) AS ULTIMO
--              FROM (
--                     SELECT *
--                     FROM (
--                       SELECT
--                         CD_EQUIPAMENTO,
--                         DT_HR_UTC,
--                         VL_FRAME_ID,
--                         VL_CONTADOR_CHUVA,
--                         ROW_NUMBER()
--                         OVER (
--                           PARTITION BY CD_EQUIPAMENTO
--                           ORDER BY DT_HR_UTC DESC ) AS RN
--                       FROM DDP_PLUVIOMETRO x
--                       WHERE DT_HR_UTC <= NVL(
--                           (SELECT MIN(DT_HR_UTC) FROM TMP_PLUVIOMETRO t WHERE t.CD_EQUIPAMENTO = x.CD_EQUIPAMENTO)
--                           , (SELECT MAX(DT_HR_UTC) FROM DDP_PLUVIOMETRO t WHERE t.CD_EQUIPAMENTO = x.CD_EQUIPAMENTO)
--                       )
--                     )
--                     WHERE RN <= 10) t
--              ORDER BY CD_EQUIPAMENTO, RN
--            )
--            WHERE VL_CONTADOR_CHUVA >= ULTIMO
--          ) t)
--   WHERE RN = 1;
-- /
-- COMMIT;
/
CREATE OR REPLACE PROCEDURE "PRC_ADM_PLUVIOMETRO"(
  p_NmTbl VARCHAR2)
AS
  v_st               VARCHAR2(1);
  v_tmp_date         DATE;
  v_cdprc            log_processamento.cd_prc%TYPE;
  v_dscprc           log_processamento.desc_prc%TYPE;
  v_cdtpprc          log_processamento.cd_tp_prc%TYPE;
  v_cdvrs            log_processamento.cd_versao%TYPE := 'V5';
  v_found            NUMBER;
  v_periodo_inicial  DATE;
  v_periodo_final    DATE;
  v_desc_equipamento VARCHAR2(50);
  v_desc_tipo_equipamento  VARCHAR2(50);
  v_desc_icone             VARCHAR2(50);
  v_desc_grupo_equipamento VARCHAR2(50);
  v_cd_grupo_equipamento   VARCHAR2(50);
  v_desc_unidade           VARCHAR2(50);
  v_desc_regional          VARCHAR2(50);
  v_desc_corporativo       VARCHAR2(50);
  v_vl_fator_chuva         VARCHAR2(20);
  v_vl_chuva               VARCHAR2(300) := '0';
  v_vl_chuva_hora          VARCHAR2(300) := '0';
  v_fator_folha_molhada  NUMBER(3);
  v_fator_chuva          NUMBER(25, 20);
  v_fator_umidade_solo_1 NUMBER(3);
  v_fator_umidade_solo_2 NUMBER(3);
  v_latitude             NUMBER(15, 9);
  v_longitude            NUMBER(15, 9);
  v_srid                 NUMBER;
  v_count_eqpto          NUMBER := 0;
  contador               NUMBER;
  v_debug                VARCHAR2(1);

  v_vl_chuva_calculada        NUMBER := 0;
  v_vl_correcao               NUMBER := 0;
  v_dif_cont_atual_e_anterior NUMBER := 0;
  V_VL_CONTADOR_ANTERIOR      VARCHAR2(20) := 0;
  V_VL_FRAME_ID               VARCHAR2(20) := 0;
  v_erro                      VARCHAR2(4000);

  V_VL_DIRECAO_VENTO          NUMBER;
  V_VL_VELOCIDADE_VENTO       NUMBER;
  V_VL_MAX_VELOCIDADE_VENTO   NUMBER;
  V_VL_TEMPERATURA            NUMBER;
  V_VL_UMIDADE                  NUMBER;
  V_VL_RADIACAO_SOLAR           NUMBER;
  V_VL_PONTO_ORVALHO            NUMBER;
  V_VL_PRESSAO_ATMOSFERICA      NUMBER;
  V_MIN_VL_DIRECAO_VENTO        NUMBER;
  V_MIN_VL_VELOCIDADE_VENTO     NUMBER;
  V_MIN_VL_MAX_VELOCIDADE_VENTO NUMBER;
  V_MIN_VL_TEMPERATURA          NUMBER;
  V_MIN_VL_UMIDADE              NUMBER;
  V_MIN_VL_RADIACAO_SOLAR       NUMBER;
  V_MIN_VL_PONTO_ORVALHO        NUMBER;
  V_MIN_VL_PRESSAO_ATMOSFERICA  NUMBER;
  V_MAX_VL_DIRECAO_VENTO        NUMBER;
  V_MAX_VL_VELOCIDADE_VENTO     NUMBER;
  V_MAX_VL_MAX_VELOCIDADE_VENTO NUMBER;
  V_MAX_VL_TEMPERATURA          NUMBER;
  V_MAX_VL_UMIDADE              NUMBER;
  V_MAX_VL_RADIACAO_SOLAR       NUMBER;
  V_MAX_VL_PONTO_ORVALHO        NUMBER;
  V_MAX_VL_PRESSAO_ATMOSFERICA  NUMBER;

  CURSOR c_get_dados_pluviometro
  IS
    SELECT
      cd_id,
      fg_tp_registro,
      cd_equipamento,
      fg_tp_equipamento,
      fg_tp_comunicacao,
      cd_versao_firmware,
      dt_hr_utc,
      dt_hr_local,
      dt_hr_servidor,
      dt_hr_servidor_dmz,
      vl_latitude,
      vl_longitude,
      cd_fazenda,
      cd_zona,
      cd_talhao,
      vl_alarme,
      vl_folha_molhada,
      vl_umidade_solo,
      vl_umidade_solo_2,
      vl_rssi,
      vl_snr,
      vl_bateria,
      vl_frame_id,
      vl_contador_chuva,
      vl_direcao_vento,
      vl_vel_vento,
      vl_vel_vento_pico,
      vl_temperatura,
      vl_umidade,
      vl_radiacao_solar,
      vl_ponto_orvalho,
      vl_pressao_atmosferica,
      vl_umidade_solo_3,
      cd_estado
    FROM tmp_pluviometro
    WHERE FG_PROCESSADO = 'F'
    ORDER BY CD_EQUIPAMENTO, DT_HR_UTC;
  cGDPluviometro c_get_dados_pluviometro%rowtype;

  PROCEDURE PRC_CORRIGE_CONTADOR_TMP IS

    CURSOR cr_get_equipamentos IS SELECT CD_EQUIPAMENTO
                                  FROM TMP_PLUVIOMETRO
                                  GROUP BY CD_EQUIPAMENTO;

    CURSOR cr_get_dados_tmp(p_cd_equipamento VARCHAR2) IS
      SELECT
        tp2.*
      FROM TMP_PLUVIOMETRO tp2
      WHERE CD_EQUIPAMENTO = p_cd_equipamento
      ORDER BY DT_HR_UTC ASC;
    r_registro              cr_get_dados_tmp%ROWTYPE;
    r_equipamento           cr_get_equipamentos%ROWTYPE;
    v_vl_flag               VARCHAR2(1);
    v_vl_contador           NUMBER(5);
    v_vl_ult_cont_analisado NUMBER(5);
    v_vl_equipamento        VARCHAR2(15);
    BEGIN

      OPEN cr_get_equipamentos;

      LOOP FETCH cr_get_equipamentos INTO r_equipamento;
        EXIT WHEN cr_get_equipamentos%NOTFOUND;

        BEGIN
          SELECT CD_EQUIPAMENTO INTO v_vl_equipamento FROM CFG_CONTROLE_PLUVIOMETRO WHERE CD_EQUIPAMENTO = r_equipamento.CD_EQUIPAMENTO;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          INSERT INTO CFG_CONTROLE_PLUVIOMETRO (CD_EQUIPAMENTO, VL_FRAME_ID, VL_CONTADOR, VL_UTIMO_ORG_ANALISADO)
          SELECT CD_EQUIPAMENTO, VL_FRAME_ID, VL_CONTADOR_CHUVA, ULTIMO FROM (
           SELECT
             t.*,
             LAG(VL_CONTADOR_CHUVA, 1, 0)
             OVER (
               ORDER BY DT_HR_UTC ) AS ULTIMO
           FROM (
                  SELECT *
                  FROM (
                    SELECT
                      CD_EQUIPAMENTO,
                      DT_HR_UTC,
                      VL_FRAME_ID,
                      VL_CONTADOR_CHUVA,
                      ROW_NUMBER()
                      OVER (ORDER BY DT_HR_UTC DESC ) AS RN
                    FROM DDP_PLUVIOMETRO
                    WHERE CD_EQUIPAMENTO =  r_equipamento.CD_EQUIPAMENTO
                    AND DT_HR_UTC <= (SELECT MIN(DT_HR_UTC) FROM TMP_PLUVIOMETRO)
                  )
                  WHERE RN <= 10) t
           ORDER BY DT_HR_UTC DESC
          ) WHERE VL_CONTADOR_CHUVA >= ULTIMO AND ROWNUM = 1;
          COMMIT;
        END;

        OPEN cr_get_dados_tmp(r_equipamento.CD_EQUIPAMENTO);
        LOOP FETCH cr_get_dados_tmp INTO r_registro;
          EXIT WHEN cr_get_dados_tmp%NOTFOUND;

          SELECT
            VL_FLAG,
            VL_CONTADOR,
            VL_UTIMO_ORG_ANALISADO
          INTO v_vl_flag
            , v_vl_contador
            , v_vl_ult_cont_analisado
          FROM CFG_CONTROLE_PLUVIOMETRO
          WHERE CD_EQUIPAMENTO = r_registro.CD_EQUIPAMENTO;

          IF v_vl_flag = 'F'
          THEN
            IF r_registro.VL_CONTADOR_ORG >= v_vl_contador
            THEN
              UPDATE CFG_CONTROLE_PLUVIOMETRO
              SET VL_CONTADOR = r_registro.VL_CONTADOR_ORG
              WHERE CD_EQUIPAMENTO = r_registro.CD_EQUIPAMENTO;
            ELSE
              UPDATE CFG_CONTROLE_PLUVIOMETRO
              SET VL_FLAG = 'T'
              WHERE CD_EQUIPAMENTO = r_registro.CD_EQUIPAMENTO;

              UPDATE TMP_PLUVIOMETRO
              SET VL_CONTADOR_CHUVA = v_vl_contador
              WHERE CD_EQUIPAMENTO = r_registro.CD_EQUIPAMENTO AND DT_HR_UTC = r_registro.DT_HR_UTC;
            END IF;
          ELSIF v_vl_flag = 'T'
            THEN
              IF r_registro.VL_CONTADOR_ORG > v_vl_ult_cont_analisado
              THEN
                UPDATE CFG_CONTROLE_PLUVIOMETRO
                SET VL_FLAG = 'F', VL_CONTADOR = r_registro.VL_CONTADOR_ORG
                WHERE CD_EQUIPAMENTO = r_registro.CD_EQUIPAMENTO;
              END IF;
              UPDATE TMP_PLUVIOMETRO
              SET VL_CONTADOR_CHUVA = v_vl_contador
              WHERE CD_EQUIPAMENTO = r_registro.CD_EQUIPAMENTO AND DT_HR_UTC = r_registro.DT_HR_UTC;
          END IF;

          UPDATE CFG_CONTROLE_PLUVIOMETRO
          SET VL_UTIMO_ORG_ANALISADO = r_registro.VL_CONTADOR_ORG, VL_FRAME_ID = r_registro.VL_FRAME_ID
          WHERE CD_EQUIPAMENTO = r_registro.CD_EQUIPAMENTO;
          COMMIT;
        END LOOP;
        CLOSE cr_get_dados_tmp;
      END LOOP;

      CLOSE cr_get_equipamentos;
    END PRC_CORRIGE_CONTADOR_TMP;

  BEGIN
    --INICIO: PARAMETRO SRID
    BEGIN
      SELECT to_number(vl_parametro)
      INTO v_srid
      FROM cfg_parametros_gerais
      WHERE cd_id = 15;
      EXCEPTION
      WHEN no_data_found
      THEN
        INSERT
        INTO cfg_parametros_gerais
        (
          cd_id,
          desc_parametro,
          vl_parametro,
          desc_comentario
        )
        VALUES
          (
            15,
            'PARAM SRID',
            '0',
            'GIS - parametro do srid'
          );
        COMMIT;
        v_srid := 0;
    END;
    --FIM: PARAMETRO SRID

    --INICIO: BUSCA O ESTADO ATUAL DA PROCEDURE NO SEMAFORO DE PROCESSAMENTOS
    BEGIN
      SELECT
        cd_id,
        desc_nome_prc,
        cd_tp_prc,
        fg_prc_ativo
      INTO v_CdPrc,
        v_DscPrc,
        v_CdTpPrc,
        v_St
      FROM cfg_semaforo_processamento
      WHERE cd_id = 151;
      EXCEPTION
      WHEN No_Data_Found
      THEN
        BEGIN
          INSERT
          INTO cfg_semaforo_processamento
          (
            cd_id,
            desc_nome_prc,
            fg_prc_ativo,
            desc_comentario,
            cd_tp_prc
          )
          VALUES
            (
              151,
              'PRC_ADM_PLUVIOMETRO',
              'F',
              'Processamento Pluviometro',
              151
            );
          COMMIT;
          v_St := 'F';
        END;
    END;
    --FIM: BUSCA O ESTADO ATUAL DA PROCEDURE NO SEMAFORO DE PROCESSAMENTOS

    BEGIN
      --HABILITA LOGS
      v_debug := 'F';
      v_periodo_inicial := sysdate;
      v_tmp_date := SYSDATE;

      --INICIO: INICIA PROCESSO CASO NAO ESTEJA SENDO EXECUTAO SEGUNDO O SEMAFORO
      IF (v_St = 'F') THEN
        --INICIO: ATUALIZA O SEMAFORO DE PROCESSAMENTO PARA TRUE
        BEGIN
          UPDATE cfg_semaforo_processamento
          SET fg_prc_ativo = 'T'
          WHERE cd_id = v_CdPrc;
          COMMIT;
        END;

        --Corrige os registros que estao na tmp
        BEGIN
          IF (v_debug = 'T') THEN
            PRC_SALVAR_LOG_V5(v_CdPrc, v_DscPrc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                'Comecando processo de correçao da tmp', 'I',
                cGDPluviometro.fg_tp_equipamento, cGDPluviometro.cd_equipamento, v_CdVrs, v_CdTpPrc);
          END IF;
          PRC_CORRIGE_CONTADOR_TMP();
        EXCEPTION WHEN OTHERS THEN
          IF (v_debug = 'T') THEN
            PRC_SALVAR_LOG_V5(v_CdPrc, v_DscPrc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                'Problemas para corrigir pluviometro. Erro: ' || SQLERRM, 'E',
                cGDPluviometro.fg_tp_equipamento, cGDPluviometro.cd_equipamento, v_CdVrs, v_CdTpPrc);
          END IF;
        END;
        --FIM: ATUALIZA O SEMAFORO DE PROCESSAMENTO PARA TRUE

        IF (v_debug = 'T')
        THEN
          PRC_SALVAR_LOG_V5(v_CdPrc, v_DscPrc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                            'ENTRADA: ' || TO_CHAR(cGDPluviometro.dt_hr_utc, 'DD/MM/YYYY HH24:MI:SS') || ' ' ||
                            'PLUVIOMETRO: ' || TO_CHAR(cGDPluviometro.dt_hr_utc, 'DD/MM/YYYY HH24:MI:SS'), 'I',
                            cGDPluviometro.fg_tp_equipamento, cGDPluviometro.cd_equipamento, v_CdVrs, v_CdTpPrc);
        END IF;

        contador := 0;
        --INICIO: LOOP NOS DADOS DO PLUVIOMETRO ATUAL
        OPEN c_get_dados_pluviometro;
        LOOP
          FETCH c_get_dados_pluviometro INTO cGDPluviometro;
          EXIT
          WHEN c_get_dados_pluviometro%notfound;
          contador := contador + 1;

          IF (v_debug = 'T')
          THEN
            PRC_SALVAR_LOG_V5(v_CdPrc, v_DscPrc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                              'ENTRADA LOOP: ' || TO_CHAR(cGDPluviometro.dt_hr_utc, 'DD/MM/YYYY HH24:MI:SS') || ' ' ||
                              'PLUVIOMETRO: ' || TO_CHAR(cGDPluviometro.dt_hr_utc, 'DD/MM/YYYY HH24:MI:SS'), 'I',
                              cGDPluviometro.fg_tp_equipamento, cGDPluviometro.cd_equipamento, v_CdVrs, v_CdTpPrc);
          END IF;

          --INICIO: BUSCA INFORMACOES DO PLUVIOMETRO CADASTRADO NA TABELA DE EQUIPAMENTOS
          BEGIN
            SELECT
              equip.desc_equipamento,
              cte.desc_tp_equipamento,
              icone.desc_icone,
              cge.desc_grupo_equipamento,
              cge.cd_grupo_equipamento,
              unidade.desc_unidade,
              regional.desc_regional,
              corp.desc_corporativo
            INTO v_desc_equipamento,
              v_desc_tipo_equipamento,
              v_desc_icone,
              v_desc_grupo_equipamento,
              v_cd_grupo_equipamento,
              v_desc_unidade,
              v_desc_regional,
              v_desc_corporativo
            FROM cdt_equipamento equip
              INNER JOIN cdt_tipo_equipamento cte
                ON (cte.cd_tp_equipamento = equip.cd_tp_equipamento)
              INNER JOIN cdt_modelo_equipamento modelo
                ON (modelo.cd_modelo_equipamento = equip.cd_modelo_equipamento)
              INNER JOIN cdt_icone icone
                ON (icone.cd_icone = modelo.cd_icone)
              INNER JOIN cdt_grupo_equipamento cge
                ON (cge.cd_grupo_equipamento = equip.cd_grupo_equipamento)
              INNER JOIN cdt_unidade unidade
                ON (unidade.cd_unidade = cge.cd_unidade)
              INNER JOIN cdt_regional regional
                ON (regional.cd_regional = unidade.cd_regional)
              INNER JOIN cdt_corporativo corp
                ON (corp.cd_corporativo = regional.cd_corporativo)
            WHERE equip.cd_equipamento = cGDPluviometro.cd_equipamento;
            EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
              v_desc_equipamento := 'NÃO CADASTRADO';
              v_desc_tipo_equipamento := 'NÃO CADASTRADO';
              v_desc_icone := 'i_default';
              v_desc_grupo_equipamento := 'NÃO CADASTRADO';
              v_desc_unidade := 'NÃO CADASTRADO';
              v_desc_regional := 'NÃO CADASTRADO';
              v_desc_corporativo := 'NÃO CADASTRADO';
              v_cd_grupo_equipamento := NULL;
          END;
          --FIM: BUSCA INFORMACOES DO PLUVIOMETRO CADASTRADO NA TABELA DE EQUIPAMENTOS

          --INICIO: BUSCA INFORMACOES ESPECIFICAS DE PARAMETROS DO PLUVIOMETRO
          BEGIN
            SELECT
              vl_fator_folha_molhada,
              vl_fator_umidade_solo,
              vl_fator_umidade_solo,
              vl_fator_chuva,
              vl_latitude,
              vl_longitude
            INTO v_fator_folha_molhada,
              v_fator_umidade_solo_1,
              v_fator_umidade_solo_2,
              v_fator_chuva,
              v_latitude,
              v_longitude
            FROM cdt_parametros_pluviometro
            WHERE cd_equipamento = cGDPluviometro.cd_equipamento;
            EXCEPTION
            WHEN no_data_found
            THEN
              v_fator_folha_molhada := 0;
              v_fator_chuva := 0;
              v_fator_umidade_solo_1 := 0;
              v_fator_umidade_solo_2 := 0;
              v_latitude := NULL;
              v_longitude := NULL;
          END;
          --FIM: BUSCA INFORMACOES ESPECIFICAS DE PARAMETROS DO PLUVIOMETRO

          --FOLHA MOLHADA
          IF (cGDPluviometro.vl_folha_molhada > v_fator_folha_molhada)
          THEN
            v_fator_folha_molhada := 1;
          ELSE
            v_fator_folha_molhada := 0;
          END IF;

          --UMIDADE SOLO 1
          IF (cGDPluviometro.vl_umidade_solo > v_fator_umidade_solo_1)
          THEN
            v_fator_umidade_solo_1 := 1;
          ELSE
            v_fator_umidade_solo_1 := 0;
          END IF;

          --UMIDADE SOLO 2
          IF (cGDPluviometro.vl_umidade_solo_2 > v_fator_umidade_solo_2)
          THEN
            v_fator_umidade_solo_2 := 1;
          ELSE
            v_fator_umidade_solo_2 := 0;
          END IF;

          --INICIO: PERSISTENCIA DOS DADOS NAS TABELAS
          BEGIN
            IF (v_debug = 'T')
            THEN
              PRC_SALVAR_LOG_V5(v_CdPrc, v_DscPrc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                                'MONITORAMENTO: ' || TO_CHAR(cGDPluviometro.dt_hr_utc, 'DD/MM/YYYY HH24:MI:SS') || ' '
                                || 'PLUVIOMETRO: ' || TO_CHAR(cGDPluviometro.dt_hr_utc, 'DD/MM/YYYY HH24:MI:SS'), 'I',
                                cGDPluviometro.fg_tp_equipamento, cGDPluviometro.cd_equipamento, v_CdVrs, v_CdTpPrc);
            END IF;

            --INICIO: TOTALIZANDO AS CHUVAS DA ULTIMA HORA
            BEGIN
              SELECT
                sum(nvl(vl_chuva, '0')),
                trunc(AVG(nvl(VL_DIRECAO_VENTO, 0)), 2),
                trunc(AVG(nvl(VL_VEL_VENTO, 0)), 2),
                trunc(AVG(nvl(VL_VEL_VENTO_PICO, 0)), 2),
                trunc(AVG(nvl(VL_TEMPERATURA, 0)), 2),
                trunc(AVG(nvl(VL_UMIDADE, 0)), 2),
                trunc(AVG(nvl(VL_RADIACAO_SOLAR, 0)), 2),
                trunc(AVG(nvl(VL_PONTO_ORVALHO, 0)), 2),
                trunc(AVG(nvl(VL_PRESSAO_ATMOSFERICA, 0)), 2),
                MIN(nvl(VL_VEL_VENTO, 0)),
                MIN(nvl(VL_DIRECAO_VENTO, 0)),
                MIN(nvl(VL_TEMPERATURA, 0)),
                MIN(nvl(VL_UMIDADE, 0)),
                MIN(nvl(VL_RADIACAO_SOLAR, 0)),
                MIN(nvl(VL_PONTO_ORVALHO, 0)),
                MIN(nvl(VL_PRESSAO_ATMOSFERICA, 0)),
                MAX(nvl(VL_DIRECAO_VENTO, 0)),
                MAX(nvl(VL_VEL_VENTO, 0)),
                MAX(nvl(VL_TEMPERATURA, 0)),
                MAX(nvl(VL_UMIDADE, 0)),
                MAX(nvl(VL_RADIACAO_SOLAR, 0)),
                MAX(nvl(VL_PONTO_ORVALHO, 0)),
                MAX(nvl(VL_PRESSAO_ATMOSFERICA, 0))
              INTO v_vl_chuva_hora,
                V_VL_DIRECAO_VENTO,
                V_VL_VELOCIDADE_VENTO,
                V_VL_MAX_VELOCIDADE_VENTO,
                V_VL_TEMPERATURA,
                V_VL_UMIDADE,
                V_VL_RADIACAO_SOLAR,
                V_VL_PONTO_ORVALHO,
                V_VL_PRESSAO_ATMOSFERICA,
                V_MIN_VL_VELOCIDADE_VENTO,
                V_MIN_VL_DIRECAO_VENTO,
                V_MIN_VL_TEMPERATURA,
                V_MIN_VL_UMIDADE,
                V_MIN_VL_RADIACAO_SOLAR,
                V_MIN_VL_PONTO_ORVALHO,
                V_MIN_VL_PRESSAO_ATMOSFERICA,
                V_MAX_VL_DIRECAO_VENTO,
                V_MAX_VL_VELOCIDADE_VENTO,
                V_MAX_VL_TEMPERATURA,
                V_MAX_VL_UMIDADE,
                V_MAX_VL_RADIACAO_SOLAR,
                V_MAX_VL_PONTO_ORVALHO,
                V_MAX_VL_PRESSAO_ATMOSFERICA
              FROM ddn_pluviometro
              WHERE cd_equipamento = cGDPluviometro.cd_equipamento
                    AND DT_HR_UTC >= TO_DATE(TO_CHAR(cGDPluviometro.dt_hr_utc, 'DD/MM/YYYY HH24') || ':00:00',
                                             'DD/MM/YYYY HH24:MI:SS')
                    AND DT_HR_UTC <= TO_DATE(TO_CHAR(cGDPluviometro.dt_hr_utc, 'DD/MM/YYYY HH24') || ':59:59',
                                             'DD/MM/YYYY HH24:MI:SS');
              EXCEPTION
              WHEN OTHERS
              THEN
                v_vl_chuva_hora := 0;
                V_VL_DIRECAO_VENTO := 0;
                V_VL_VELOCIDADE_VENTO := 0;
                V_VL_MAX_VELOCIDADE_VENTO := 0;
                V_VL_TEMPERATURA := 0;
                V_VL_UMIDADE := 0;
                V_VL_RADIACAO_SOLAR := 0;
                V_VL_PONTO_ORVALHO := 0;
                V_VL_PRESSAO_ATMOSFERICA := 0;
                V_MIN_VL_VELOCIDADE_VENTO := 0;
                V_MIN_VL_MAX_VELOCIDADE_VENTO := 0;
                V_MIN_VL_DIRECAO_VENTO := 0;
                V_MIN_VL_TEMPERATURA := 0;
                V_MIN_VL_UMIDADE := 0;
                V_MIN_VL_RADIACAO_SOLAR := 0;
                V_MIN_VL_PONTO_ORVALHO := 0;
                V_MIN_VL_PRESSAO_ATMOSFERICA := 0;
                V_MAX_VL_DIRECAO_VENTO := 0;
                V_MAX_VL_VELOCIDADE_VENTO := 0;
                V_MAX_VL_MAX_VELOCIDADE_VENTO := 0;
                V_MAX_VL_TEMPERATURA := 0;
                V_MAX_VL_UMIDADE := 0;
                V_MAX_VL_RADIACAO_SOLAR := 0;
                V_MAX_VL_PONTO_ORVALHO := 0;
                V_MAX_VL_PRESSAO_ATMOSFERICA := 0;
            END;

            IF to_number(v_vl_chuva_hora) < 0
            THEN
              v_vl_chuva_hora := 0;
            END IF;
            --FIM: TOTALIZANDO AS CHUVAS DA ULTIMA HORA

            -- CHUVA POR HORA
            IF (v_debug = 'T')
            THEN
              PRC_SALVAR_LOG_V5(v_CdPrc, v_DscPrc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                                'REALIZANDO Chuva por Hora = ' || v_vl_chuva_hora, 'I',
                                cGDPluviometro.fg_tp_equipamento, cGDPluviometro.cd_equipamento, v_CdVrs, v_CdTpPrc);
            END IF;

            --INICIO: INSERT OR UPDATE NA HISTORICO CHUVA
            BEGIN
              INSERT
              INTO DDN_METEOROLOGIA
              (
                CD_EQUIPAMENTO,
                DT_HR_LOCAL,
                VL_CHUVA,
                CD_TP_EQUIPAMENTO,
                VL_DIRECAO_VENTO,
                VL_VELOCIDADE_VENTO,
                VL_MAX_VELOCIDADE_VENTO,
                VL_TEMPERATURA,
                VL_UMIDADE,
                VL_RADIACAO_SOLAR,
                VL_PONTO_ORVALHO,
                VL_PRESSAO_ATMOSFERICA,
                MIN_VL_VELOCIDADE_VENTO,
                MIN_VL_DIRECAO_VENTO,
                MIN_VL_TEMPERATURA,
                MIN_VL_UMIDADE,
                MIN_VL_RADIACAO_SOLAR,
                MIN_VL_PONTO_ORVALHO,
                MIN_VL_PRESSAO_ATMOSFERICA,
                MAX_VL_DIRECAO_VENTO,
                MAX_VL_VELOCIDADE_VENTO,
                MAX_VL_TEMPERATURA,
                MAX_VL_UMIDADE,
                MAX_VL_RADIACAO_SOLAR,
                MAX_VL_PONTO_ORVALHO,
                MAX_VL_PRESSAO_ATMOSFERICA
              )
              VALUES
                (
                  cGDPluviometro.cd_equipamento,
                  TO_DATE(TO_CHAR(cGDPluviometro.dt_hr_local, 'DD/MM/YYYY HH24') || ':00:00', 'DD/MM/YYYY HH24:MI:SS'),
                  v_vl_chuva_hora,
                  cGDPluviometro.fg_tp_equipamento,
                  V_VL_DIRECAO_VENTO,
                  V_VL_VELOCIDADE_VENTO,
                  V_VL_MAX_VELOCIDADE_VENTO,
                  V_VL_TEMPERATURA,
                  V_VL_UMIDADE,
                  V_VL_RADIACAO_SOLAR,
                  V_VL_PONTO_ORVALHO,
                  V_VL_PRESSAO_ATMOSFERICA,
                  V_MIN_VL_VELOCIDADE_VENTO,
                  V_MIN_VL_DIRECAO_VENTO,
                  V_MIN_VL_TEMPERATURA,
                  V_MIN_VL_UMIDADE,
                  V_MIN_VL_RADIACAO_SOLAR,
                  V_MIN_VL_PONTO_ORVALHO,
                  V_MIN_VL_PRESSAO_ATMOSFERICA,
                  V_MAX_VL_DIRECAO_VENTO,
                  V_MAX_VL_VELOCIDADE_VENTO,
                  V_MAX_VL_TEMPERATURA,
                  V_MAX_VL_UMIDADE,
                  V_MAX_VL_RADIACAO_SOLAR,
                  V_MAX_VL_PONTO_ORVALHO,
                  V_MAX_VL_PRESSAO_ATMOSFERICA
                );

              INSERT
              INTO DDN_METEOROLOGIA_RESUMIDA
              (
                CD_EQUIPAMENTO,
                DT_HR_LOCAL,
                VL_CHUVA,
                CD_TP_EQUIPAMENTO,
                VL_DIRECAO_VENTO,
                VL_VELOCIDADE_VENTO,
                VL_MAX_VELOCIDADE_VENTO,
                VL_TEMPERATURA,
                VL_UMIDADE,
                VL_RADIACAO_SOLAR,
                VL_PONTO_ORVALHO,
                VL_PRESSAO_ATMOSFERICA,
                MIN_VL_VELOCIDADE_VENTO,
                MIN_VL_DIRECAO_VENTO,
                MIN_VL_TEMPERATURA,
                MIN_VL_UMIDADE,
                MIN_VL_RADIACAO_SOLAR,
                MIN_VL_PONTO_ORVALHO,
                MIN_VL_PRESSAO_ATMOSFERICA,
                MAX_VL_DIRECAO_VENTO,
                MAX_VL_VELOCIDADE_VENTO,
                MAX_VL_TEMPERATURA,
                MAX_VL_UMIDADE,
                MAX_VL_RADIACAO_SOLAR,
                MAX_VL_PONTO_ORVALHO,
                MAX_VL_PRESSAO_ATMOSFERICA
              )
              VALUES
                (
                  cGDPluviometro.cd_equipamento,
                  TO_DATE(TO_CHAR(cGDPluviometro.dt_hr_local, 'DD/MM/YYYY HH24') || ':00:00', 'DD/MM/YYYY HH24:MI:SS'),
                  v_vl_chuva_hora,
                  cGDPluviometro.fg_tp_equipamento,
                  V_VL_DIRECAO_VENTO,
                  V_VL_VELOCIDADE_VENTO,
                  V_VL_MAX_VELOCIDADE_VENTO,
                  V_VL_TEMPERATURA,
                  V_VL_UMIDADE,
                  V_VL_RADIACAO_SOLAR,
                  V_VL_PONTO_ORVALHO,
                  V_VL_PRESSAO_ATMOSFERICA,
                  V_MIN_VL_VELOCIDADE_VENTO,
                  V_MIN_VL_DIRECAO_VENTO,
                  V_MIN_VL_TEMPERATURA,
                  V_MIN_VL_UMIDADE,
                  V_MIN_VL_RADIACAO_SOLAR,
                  V_MIN_VL_PONTO_ORVALHO,
                  V_MIN_VL_PRESSAO_ATMOSFERICA,
                  V_MAX_VL_DIRECAO_VENTO,
                  V_MAX_VL_VELOCIDADE_VENTO,
                  V_MAX_VL_TEMPERATURA,
                  V_MAX_VL_UMIDADE,
                  V_MAX_VL_RADIACAO_SOLAR,
                  V_MAX_VL_PONTO_ORVALHO,
                  V_MAX_VL_PRESSAO_ATMOSFERICA
                );

              IF (v_debug = 'T')
              THEN
                PRC_SALVAR_LOG_V5(v_CdPrc, v_DscPrc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                                  'Estatistica INSERT DDP_HISTORICO_CHUVAS. Erro: ' || SQLERRM, 'I',
                                  cGDPluviometro.fg_tp_equipamento, cGDPluviometro.cd_equipamento, v_CdVrs, v_CdTpPrc);
              END IF;

              --COMMIT;
              EXCEPTION
              WHEN OTHERS
              THEN

                IF (v_debug = 'T')
                THEN
                  PRC_SALVAR_LOG_V5(v_CdPrc, v_DscPrc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                                    TO_CHAR(cGDPluviometro.dt_hr_local, 'DD/MM/YYYY HH24:MM:SS') ||
                                    ' - ATUALIZANDO dados na ddn_meteorologia. Erro: ' || SQLERRM, 'I',
                                    cGDPluviometro.fg_tp_equipamento, cGDPluviometro.cd_equipamento, v_CdVrs,
                                    v_CdTpPrc);
                END IF;

                UPDATE DDN_METEOROLOGIA
                SET VL_CHUVA                 = v_vl_chuva_hora,
                  VL_DIRECAO_VENTO           = V_VL_DIRECAO_VENTO,
                  VL_VELOCIDADE_VENTO        = V_VL_VELOCIDADE_VENTO,
                  VL_MAX_VELOCIDADE_VENTO    = V_VL_MAX_VELOCIDADE_VENTO,
                  VL_TEMPERATURA             = V_VL_TEMPERATURA,
                  VL_UMIDADE                 = V_VL_UMIDADE,
                  VL_RADIACAO_SOLAR          = V_VL_RADIACAO_SOLAR,
                  VL_PONTO_ORVALHO           = V_VL_PONTO_ORVALHO,
                  VL_PRESSAO_ATMOSFERICA     = V_VL_PRESSAO_ATMOSFERICA,
                  MIN_VL_VELOCIDADE_VENTO    = V_MIN_VL_VELOCIDADE_VENTO,
                  MIN_VL_DIRECAO_VENTO       = V_MIN_VL_DIRECAO_VENTO,
                  MIN_VL_TEMPERATURA         = V_MIN_VL_TEMPERATURA,
                  MIN_VL_UMIDADE             = V_MIN_VL_UMIDADE,
                  MIN_VL_RADIACAO_SOLAR      = V_MIN_VL_RADIACAO_SOLAR,
                  MIN_VL_PONTO_ORVALHO       = V_MIN_VL_PONTO_ORVALHO,
                  MIN_VL_PRESSAO_ATMOSFERICA = V_MIN_VL_PRESSAO_ATMOSFERICA,
                  MAX_VL_DIRECAO_VENTO       = V_MAX_VL_DIRECAO_VENTO,
                  MAX_VL_VELOCIDADE_VENTO    = V_MAX_VL_VELOCIDADE_VENTO,
                  MAX_VL_TEMPERATURA         = V_MAX_VL_TEMPERATURA,
                  MAX_VL_UMIDADE             = V_MAX_VL_UMIDADE,
                  MAX_VL_RADIACAO_SOLAR      = V_MAX_VL_RADIACAO_SOLAR,
                  MAX_VL_PONTO_ORVALHO       = V_MAX_VL_PONTO_ORVALHO,
                  MAX_VL_PRESSAO_ATMOSFERICA = V_MAX_VL_PRESSAO_ATMOSFERICA
                WHERE CD_EQUIPAMENTO = cGDPluviometro.cd_equipamento
                      AND
                      TO_CHAR(DT_HR_LOCAL, 'DD/MM/YYYY HH24') = TO_CHAR(cGDPluviometro.dt_hr_local, 'DD/MM/YYYY HH24');

                UPDATE DDN_METEOROLOGIA_RESUMIDA
                SET VL_CHUVA                 = v_vl_chuva_hora,
                  VL_DIRECAO_VENTO           = V_VL_DIRECAO_VENTO,
                  VL_VELOCIDADE_VENTO        = V_VL_VELOCIDADE_VENTO,
                  VL_MAX_VELOCIDADE_VENTO    = V_VL_MAX_VELOCIDADE_VENTO,
                  VL_TEMPERATURA             = V_VL_TEMPERATURA,
                  VL_UMIDADE                 = V_VL_UMIDADE,
                  VL_RADIACAO_SOLAR          = V_VL_RADIACAO_SOLAR,
                  VL_PONTO_ORVALHO           = V_VL_PONTO_ORVALHO,
                  VL_PRESSAO_ATMOSFERICA     = V_VL_PRESSAO_ATMOSFERICA,
                  MIN_VL_VELOCIDADE_VENTO    = V_MIN_VL_VELOCIDADE_VENTO,
                  MIN_VL_DIRECAO_VENTO       = V_MIN_VL_DIRECAO_VENTO,
                  MIN_VL_TEMPERATURA         = V_MIN_VL_TEMPERATURA,
                  MIN_VL_UMIDADE             = V_MIN_VL_UMIDADE,
                  MIN_VL_RADIACAO_SOLAR      = V_MIN_VL_RADIACAO_SOLAR,
                  MIN_VL_PONTO_ORVALHO       = V_MIN_VL_PONTO_ORVALHO,
                  MIN_VL_PRESSAO_ATMOSFERICA = V_MIN_VL_PRESSAO_ATMOSFERICA,
                  MAX_VL_DIRECAO_VENTO       = V_MAX_VL_DIRECAO_VENTO,
                  MAX_VL_VELOCIDADE_VENTO    = V_MAX_VL_VELOCIDADE_VENTO,
                  MAX_VL_TEMPERATURA         = V_MAX_VL_TEMPERATURA,
                  MAX_VL_UMIDADE             = V_MAX_VL_UMIDADE,
                  MAX_VL_RADIACAO_SOLAR      = V_MAX_VL_RADIACAO_SOLAR,
                  MAX_VL_PONTO_ORVALHO       = V_MAX_VL_PONTO_ORVALHO,
                  MAX_VL_PRESSAO_ATMOSFERICA = V_MAX_VL_PRESSAO_ATMOSFERICA
                WHERE CD_EQUIPAMENTO = cGDPluviometro.cd_equipamento
                      AND
                      TO_CHAR(DT_HR_LOCAL, 'DD/MM/YYYY HH24') = TO_CHAR(cGDPluviometro.dt_hr_local, 'DD/MM/YYYY HH24');

                IF (v_debug = 'T')
                THEN
                  PRC_SALVAR_LOG_V5(v_CdPrc, v_DscPrc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                                    'ATUALIZADO dados na ddn_meteorologia. Erro: ' || SQLERRM, 'I',
                                    cGDPluviometro.fg_tp_equipamento, cGDPluviometro.cd_equipamento, v_CdVrs,
                                    v_CdTpPrc);
                END IF;
              --COMMIT;
            END;
            --FIM: INSERT OR UPDATE NA HISTORICO CHUVA
            -- FIM CHUVA POR HORA

            --INICIO: INSERINDO NA DDN_PLUVIOMETRO
            BEGIN
              INSERT
              INTO ddn_pluviometro
              (
                cd_id,
                cd_equipamento,
                cd_fazenda,
                cd_grupo_equipamento,
                cd_talhao,
                cd_zona,
                desc_corporativo,
                desc_grupo_equipamento,
                desc_regional,
                desc_tipo_equipamento,
                desc_unidade,
                desc_equipamento,
                dt_hr_local,
                dt_hr_utc,
                dt_hr_servidor,
                dt_hr_servidor_dmz,
                fg_tp_equipamento,
                vl_alarme,
                vl_latitude,
                vl_longitude,
                vl_frame_id,
                vl_contador_chuva,
                vl_folha_molhada,
                vl_umidade_solo,
                vl_umidade_solo_2,
                vl_chuva,
                vl_rssi,
                vl_snr,
                vl_bateria,
                vl_direcao_vento,
                vl_vel_vento,
                vl_vel_vento_pico,
                vl_temperatura,
                vl_umidade,
                vl_radiacao_solar,
                vl_ponto_orvalho,
                vl_pressao_atmosferica,
                vl_umidade_solo_3,
                cd_estado
              )
              VALUES
                (
                  cgdpluviometro.cd_id,
                  cgdpluviometro.cd_equipamento,
                  fnc_localizacao_v1(cgdpluviometro.vl_latitude, cgdpluviometro.vl_longitude).cd_fazenda,
                  v_cd_grupo_equipamento,
                  fnc_localizacao_v1(cgdpluviometro.vl_latitude, cgdpluviometro.vl_longitude).cd_talhao,
                  fnc_localizacao_v1(cgdpluviometro.vl_latitude, cgdpluviometro.vl_longitude).cd_zona,
                  v_desc_corporativo,
                  v_desc_grupo_equipamento,
                  v_desc_regional,
                  v_desc_tipo_equipamento,
                  v_desc_unidade,
                  v_desc_equipamento,
                  TO_CHAR(cgdpluviometro.dt_hr_local, 'DD/MM/YYYY HH24:MI:SS'),
                  cgdpluviometro.dt_hr_utc,
                  TO_CHAR(cgdpluviometro.dt_hr_servidor, 'DD/MM/YYYY HH24:MI:SS'),
                  TO_CHAR(cgdpluviometro.dt_hr_servidor_dmz, 'DD/MM/YYYY HH24:MI:SS'),
                  cgdpluviometro.fg_tp_equipamento,
                  cgdpluviometro.vl_alarme,
                  cgdpluviometro.vl_latitude,
                  cgdpluviometro.vl_longitude,
                  cgdpluviometro.vl_frame_id,
                  cgdpluviometro.vl_contador_chuva,
                  v_fator_folha_molhada,
                  v_fator_umidade_solo_1,
                  v_fator_umidade_solo_2,
                  to_number(v_vl_chuva),
                  cgdpluviometro.vl_rssi,
                  cgdpluviometro.vl_snr,
                  cgdpluviometro.vl_bateria,
                  cgdpluviometro.vl_direcao_vento,
                  cgdpluviometro.vl_vel_vento,
                  cgdpluviometro.vl_vel_vento_pico,
                  cgdpluviometro.vl_temperatura,
                  cgdpluviometro.vl_umidade,
                  cgdpluviometro.vl_radiacao_solar,
                  cgdpluviometro.vl_ponto_orvalho,
                  cgdpluviometro.vl_pressao_atmosferica,
                  cgdpluviometro.vl_umidade_solo_3,
                  cgdpluviometro.cd_estado
                );

              IF (v_debug = 'T')
              THEN
                prc_salvar_log_v5(v_cdprc, v_dscprc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                                  'Estatistica INSERT DDN_PLUVIOMETRO. - Equipamento: ' ||
                                  cgdpluviometro.cd_equipamento, 'I', cgdpluviometro.fg_tp_equipamento,
                                  cgdpluviometro.cd_equipamento, v_cdvrs, v_cdtpprc);
              END IF;

              --INICIO: ALIMENTANDO CONTADOR DE CHUVA
              BEGIN
                SELECT
                  NVL(VL_CONTADOR_ANTERIOR, nvl(VL_CONTADOR_CHUVA, 0)),
                  nvl(VL_FRAME_ID, 0)
                INTO V_VL_CONTADOR_ANTERIOR,
                  V_VL_FRAME_ID
                FROM
                  (SELECT
                     CD_ID,
                     VL_CONTADOR_CHUVA,
                     VL_FRAME_ID,
                     LAG(VL_CONTADOR_CHUVA, 1)
                     OVER (
                       ORDER BY CD_EQUIPAMENTO, DT_HR_UTC ) AS VL_CONTADOR_ANTERIOR
                   FROM DDN_PLUVIOMETRO
                   WHERE CD_EQUIPAMENTO = cgdpluviometro.cd_equipamento
                  )
                WHERE CD_ID = cgdpluviometro.cd_id;
                EXCEPTION
                WHEN no_data_found
                THEN
                  V_VL_CONTADOR_ANTERIOR := 0;
                  V_VL_FRAME_ID := 0;
              END;

              v_dif_cont_atual_e_anterior := cgdpluviometro.vl_contador_chuva - to_number(V_VL_CONTADOR_ANTERIOR);

              IF v_dif_cont_atual_e_anterior is null
              THEN
                v_dif_cont_atual_e_anterior := 0;
              END IF;

              --FRAME ID seria um numero entre 0 e 255 ele eh sequencial e reseta para 0 quando 255+1
              --to_number(V_VL_FRAME_ID) <> cgdpluviometro.vl_frame_id AND  comentado essa condição por não haver uso
              IF (v_dif_cont_atual_e_anterior < 0)
              THEN
                IF (65536 - to_number(V_VL_CONTADOR_ANTERIOR) + cgdpluviometro.vl_contador_chuva) < 10000
                THEN
                  v_vl_chuva_calculada :=
                  (65536 - to_number(V_VL_CONTADOR_ANTERIOR) + cgdpluviometro.vl_contador_chuva) * v_fator_chuva;
                  v_vl_correcao := 0.0862 * ln(ABS(v_vl_chuva_calculada)) - 0.2037;
                  IF v_vl_correcao < 0
                  THEN
                    v_vl_correcao := ABS(v_vl_correcao) + 1;
                  ELSE
                    v_vl_correcao := ABS(v_vl_correcao - 1);
                  END IF;
                  v_vl_chuva_calculada := v_vl_correcao * v_vl_chuva_calculada;
                  v_vl_chuva_calculada := trim(TO_CHAR(v_vl_chuva_calculada, '99990D99'));
                ELSE
                  v_vl_chuva_calculada := 0;
                  INSERT INTO LOG_ERRO_PLUVIOMETRO
                  (
                    cd_id,
                    fg_tp_registro,
                    cd_equipamento,
                    fg_tp_equipamento,
                    fg_tp_comunicacao,
                    cd_versao_firmware,
                    dt_hr_utc,
                    dt_hr_local,
                    dt_hr_servidor,
                    dt_hr_servidor_dmz,
                    vl_latitude,
                    vl_longitude,
                    cd_fazenda,
                    cd_zona,
                    cd_talhao,
                    vl_alarme,
                    vl_folha_molhada,
                    vl_umidade_solo,
                    vl_umidade_solo_2,
                    vl_rssi,
                    vl_snr,
                    vl_bateria,
                    vl_frame_id,
                    vl_contador_chuva,
                    vl_direcao_vento,
                    vl_vel_vento,
                    vl_vel_vento_pico,
                    vl_temperatura,
                    vl_umidade,
                    vl_radiacao_solar,
                    vl_ponto_orvalho,
                    vl_pressao_atmosferica,
                    vl_umidade_solo_3,
                    cd_estado
                  )
                  VALUES
                    (
                      cGDPluviometro.cd_id,
                      cGDPluviometro.fg_tp_registro,
                      cGDPluviometro.cd_equipamento,
                      cGDPluviometro.fg_tp_equipamento,
                      cGDPluviometro.fg_tp_comunicacao,
                      cGDPluviometro.cd_versao_firmware,
                      cGDPluviometro.dt_hr_utc,
                      cGDPluviometro.dt_hr_local,
                      cGDPluviometro.dt_hr_servidor,
                      cGDPluviometro.dt_hr_servidor_dmz,
                      cGDPluviometro.vl_latitude,
                      cGDPluviometro.vl_longitude,
                      cGDPluviometro.cd_fazenda,
                      cGDPluviometro.cd_zona,
                      cGDPluviometro.cd_talhao,
                      cGDPluviometro.vl_alarme,
                      cGDPluviometro.vl_folha_molhada,
                      cGDPluviometro.vl_umidade_solo,
                      cGDPluviometro.vl_umidade_solo_2,
                      cGDPluviometro.vl_rssi,
                      cGDPluviometro.vl_snr,
                      cGDPluviometro.vl_bateria,
                      cGDPluviometro.vl_frame_id,
                      cGDPluviometro.vl_contador_chuva,
                      cGDPluviometro.vl_direcao_vento,
                      cGDPluviometro.vl_vel_vento,
                      cGDPluviometro.vl_vel_vento_pico,
                      cGDPluviometro.vl_temperatura,
                      cGDPluviometro.vl_umidade,
                      cGDPluviometro.vl_radiacao_solar,
                      cGDPluviometro.vl_ponto_orvalho,
                      cGDPluviometro.vl_pressao_atmosferica,
                      cGDPluviometro.vl_umidade_solo_3,
                      cGDPluviometro.cd_estado
                    );
                END IF;
              ELSE
                IF v_dif_cont_atual_e_anterior <> 0 AND v_dif_cont_atual_e_anterior > 0
                THEN
                  begin
                    v_vl_chuva_calculada := v_dif_cont_atual_e_anterior * v_fator_chuva;
                    v_vl_correcao := 0.0862 * ln(ABS(v_vl_chuva_calculada)) - 0.2037;
                    IF v_vl_correcao < 0
                    THEN
                      v_vl_correcao := abs(v_vl_correcao) + 1;
                    ELSE
                      v_vl_correcao := abs(v_vl_correcao - 1);
                    END IF;
                    v_vl_chuva_calculada := v_vl_correcao * v_vl_chuva_calculada;
                    v_vl_chuva_calculada := trim(TO_CHAR(v_vl_chuva_calculada, '99999D99'));
                    exception when others
                    then
                      v_erro := SQLERRM;
                      INSERT INTO log_processamento (cd_prc,
                                                     desc_prc,
                                                     dt_hr_inicio_cfg,
                                                     dt_hr_fim_cfg,
                                                     vl_qtde_dias_cfg,
                                                     dt_hr_inicio_prc,
                                                     dt_hr_fim_prc,
                                                     vl_tempo_prc,
                                                     desc_log_prc,
                                                     fg_tipo,
                                                     fg_tp_equipamento,
                                                     cd_equipamento,
                                                     cd_versao,
                                                     cd_tp_prc)
                      VALUES (151,
                              'trg_stat_bi_ddn_pluviometro',
                        sysdate,
                        sysdate,
                        null,
                        sysdate,
                        SYSDATE,
                        null,
                        v_erro || ' - ' || v_vl_chuva_calculada || ' - ' || v_dif_cont_atual_e_anterior || ' - ' ||
                        v_fator_chuva,
                        null,
                        null,
                        null,
                              null,
                              null);
                  end;
                ELSE
                  v_vl_chuva_calculada := 0;
                END IF;
              END IF;

              IF v_vl_chuva_calculada < 0
              THEN
                v_vl_chuva_calculada := 0;
              END IF;
              --FIM: ALIMENTANDO CONTADOR DE CHUVA

              UPDATE DDN_PLUVIOMETRO
              SET VL_CHUVA = v_vl_chuva_calculada
              WHERE CD_ID = cgdpluviometro.cd_id;
              --FIM: ALIMENTANDO CONTADOR DE CHUVA

              EXCEPTION
              WHEN OTHERS
              THEN
                IF (v_debug = 'T')
                THEN
                  prc_salvar_log_v5(v_cdprc, v_dscprc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                                    'Problemas para inserir registros na tabela DDN_PLUVIOMETRO. Erro: ' || sqlerrm,
                                    'E', cgdpluviometro.fg_tp_equipamento, cgdpluviometro.cd_equipamento, v_cdvrs,
                                    v_cdtpprc);
                END IF;
            END;
            --FIM: INSERINDO NA DDN_PLUVIOMETRO

            BEGIN
              DELETE TMP_PLUVIOMETRO
              WHERE CD_ID = cGDPluviometro.cd_id;
            END;

            EXCEPTION
            WHEN OTHERS
            THEN
              ROLLBACK;
              IF (v_debug = 'T')
              THEN
                PRC_SALVAR_LOG_V5(v_CdPrc, v_DscPrc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                                  'Problemas para Processar o Equipamento. Erro: ' || SQLERRM, 'E',
                                  cGDPluviometro.fg_tp_equipamento, cGDPluviometro.cd_equipamento, v_CdVrs, v_CdTpPrc);
              END IF;
          END;
          --FIM: PERSISTENCIA DOS DADOS NAS TABELAS

          IF contador >= 1000
          THEN
            contador := 0;
            COMMIT;
          END IF;

        END LOOP;
        CLOSE c_get_dados_pluviometro;
        --FIM: LOOP NOS DADOS DO PLUVIOMETRO ATUAL

        --INICIO: ATUALIZA O SEMAFORO DE PROCESSAMENTO PARA FALSE
        UPDATE cfg_semaforo_processamento
        SET fg_prc_ativo      = 'F',
          dt_hr_execucao_proc = sysdate
        WHERE cd_id = v_CdPrc;
        --FIM: ATUALIZA O SEMAFORO DE PROCESSAMENTO PARA FALSE

        v_periodo_final := sysdate;
        PRC_SALVAR_LOG_V5(v_CdPrc, v_DscPrc, v_periodo_inicial, v_periodo_final, v_tmp_date,
                          'Processamento PLUVIOMETRO finalizado. Total Equipamentos: ' || v_count_eqpto, 'I',
                          cGDPluviometro.fg_tp_equipamento, cGDPluviometro.cd_equipamento, v_CdVrs, v_CdTpPrc);

        COMMIT;

        --Corrige dados de pluviometro
        PKG_PLUVIOMETRO.PRC_PROCESSAR();
        prc_upd_vl_radiacao_solar_est(TO_CHAR(sysdate, 'DD/MM/YYYY'), TO_CHAR(sysdate, 'DD/MM/YYYY'), 151);
      --FIM: INICIA PROCESSO CASO NAO ESTEJA SENDO EXECUTAO SEGUNDO O SEMAFORO
      END IF;
    END;
  END PRC_ADM_PLUVIOMETRO;
/
CALL PRC_ADM_PLUVIOMETRO(null);
/