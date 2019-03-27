CREATE OR REPLACE PACKAGE SGPA_PROCESSADOR_OPENWEATHER AS
  PROCEDURE PROC_INTEGRA_NDVI_HISTORICO;
  PROCEDURE PROC_INTEGRA_PRECIPITACAO_HIST;
  PROCEDURE PROC_INTEGRA_UVI_HISTORICO;
  PROCEDURE PROC_INTEGRA_SOLO_HISTORICO;
  PROCEDURE PROC_INTEGRA_CLIMA_HISTORICO;
END SGPA_PROCESSADOR_OPENWEATHER;

CREATE OR REPLACE PACKAGE BODY SGPA_PROCESSADOR_OPENWEATHER AS
  PROCEDURE PROC_INTEGRA_NDVI_HISTORICO IS
    CURSOR cr_dados_entrada IS
      SELECT
        t.DDP_ID,
        z.CD_UNIDADE,
        z.CD_FAZENDA,
        z.CD_ZONA,
        z.CD_TALHAO,
        TO_TIMESTAMP(t.DT_ANALISE, 'YYYY-MM-DD HH24:MI:SS') as DT_ANALISE,
        t.FONTE,
        t.ZOOM,
        t.AREA_DISPONIVEL,
        t.PERCENTUAL_NUVENS,
        t.MINIMO,
        t.PRIMEIRO_QUARTIL,
        t.MEDIANA,
        t.TERCEIRO_QUARTIL,
        t.MAXIMO,
        t.MEDIA,
        t.DESVIO_PADRAO,
        t.TOTAL_PIXELS
      FROM DDP_NDVI_TALHAO_HISTORICO t
        JOIN CDT_NDVI_TALHAO_POLIGONO z ON t.POLIGONO = z.POLIGONO;
    TYPE T_DADO_NAO_PROCESSADO IS TABLE OF cr_dados_entrada%ROWTYPE INDEX BY BINARY_INTEGER;
    v_nao_processados T_DADO_NAO_PROCESSADO;
    v_ativo            VARCHAR2(1);
    BEGIN

      BEGIN
        SELECT FG_PRC_ATIVO INTO v_ativo
        FROM CFG_SEMAFORO_PROCESSAMENTO
        WHERE CD_ID = 307;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        INSERT INTO CFG_SEMAFORO_PROCESSAMENTO (CD_ID, DESC_NOME_PRC, FG_PRC_ATIVO, DESC_COMENTARIO)
        VALUES (307, 'SGPA_PROCESSADOR_OPENWEATHER.PROC_INTEGRA_NDVI_HISTORICO', 'F', 'Move dados da ddp para a oficial');
        COMMIT;
        v_ativo := 'F';
      END;

      IF v_ativo = 'F' THEN
        UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'T', DT_HR_EXECUCAO_PROC = SYSDATE WHERE CD_ID = 307;
        COMMIT;

        OPEN cr_dados_entrada;
        LOOP FETCH cr_dados_entrada
        BULK COLLECT INTO v_nao_processados
        LIMIT 1000;
          EXIT WHEN v_nao_processados.COUNT = 0;
          FORALL I IN 1.. v_nao_processados.COUNT
          MERGE INTO DDN_NDVI_TALHAO_HISTORICO
          USING DUAL
          ON (
            CD_UNIDADE = v_nao_processados(I).CD_UNIDADE
            AND CD_FAZENDA = v_nao_processados(I).CD_FAZENDA
            AND CD_ZONA = v_nao_processados(I).CD_ZONA
            AND CD_TALHAO = v_nao_processados(I).CD_TALHAO
            AND DT_ANALISE = v_nao_processados(I).DT_ANALISE
            AND FONTE = v_nao_processados(I).FONTE
            AND ZOOM = v_nao_processados(I).ZOOM
          )
          WHEN NOT MATCHED THEN
            INSERT (CD_UNIDADE, CD_FAZENDA, CD_ZONA, CD_TALHAO, DT_ANALISE
              , FONTE, ZOOM, AREA_DISPONIVEL, PERCENTUAL_NUVENS, MINIMO
              , PRIMEIRO_QUARTIL, MEDIANA, TERCEIRO_QUARTIL, MAXIMO, MEDIA
              , DESVIO_PADRAO, TOTAL_PIXELS)
            VALUES (v_nao_processados(I).CD_UNIDADE, v_nao_processados(I).CD_FAZENDA, v_nao_processados(I).CD_ZONA
              , v_nao_processados(I).CD_TALHAO, v_nao_processados(I).DT_ANALISE, v_nao_processados(I).FONTE,v_nao_processados(I).ZOOM
              , v_nao_processados(I).AREA_DISPONIVEL, v_nao_processados(I).PERCENTUAL_NUVENS, v_nao_processados(I).MINIMO
              , v_nao_processados(I).PRIMEIRO_QUARTIL, v_nao_processados(I).MEDIANA, v_nao_processados(I).TERCEIRO_QUARTIL
              , v_nao_processados(I).MAXIMO, v_nao_processados(I).MEDIA, v_nao_processados(I).DESVIO_PADRAO
              , v_nao_processados(I).TOTAL_PIXELS)
          WHEN MATCHED THEN
            UPDATE
              SET AREA_DISPONIVEL = v_nao_processados(I).AREA_DISPONIVEL
                , PERCENTUAL_NUVENS = v_nao_processados(I).PERCENTUAL_NUVENS
                , MINIMO = v_nao_processados(I).MINIMO
                , PRIMEIRO_QUARTIL = v_nao_processados(I).PRIMEIRO_QUARTIL
                , MEDIANA = v_nao_processados(I).MEDIANA
                , TERCEIRO_QUARTIL = v_nao_processados(I).TERCEIRO_QUARTIL
                , MAXIMO = v_nao_processados(I).MAXIMO
                , MEDIA = v_nao_processados(I).MEDIA
                , DESVIO_PADRAO = v_nao_processados(I).DESVIO_PADRAO
                , TOTAL_PIXELS = v_nao_processados(I).TOTAL_PIXELS;
          FORALL I IN 1.. v_nao_processados.COUNT
          DELETE FROM DDP_NDVI_TALHAO_HISTORICO
          WHERE DDP_ID = v_nao_processados(I).DDP_ID;
          COMMIT;
        END LOOP;
        CLOSE cr_dados_entrada;

        UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'F', VL_TEMPO_EXECUCAO = ( SYSDATE - DT_HR_EXECUCAO_PROC ) * 86400 WHERE CD_ID = 307;
        COMMIT;
      END IF;
    END PROC_INTEGRA_NDVI_HISTORICO;
  PROCEDURE PROC_INTEGRA_PRECIPITACAO_HIST IS
    CURSOR cr_dados_entrada IS
      SELECT
        t.DDP_ID,
        z.CD_UNIDADE,
        z.CD_FAZENDA,
        z.CD_ZONA,
        z.CD_TALHAO,
        TO_TIMESTAMP(t.DT_ANALISE, 'YYYY-MM-DD HH24:MI:SS') as DT_ANALISE,
        t.CHUVA_ACUMULADA,
        t.QUANTIDADE_MEDICOES
      FROM DDP_PRECIPITACAO_TALHAO_HIST t
        JOIN CDT_NDVI_TALHAO_POLIGONO z ON t.POLIGONO = z.POLIGONO;
    TYPE T_DADO_NAO_PROCESSADO IS TABLE OF cr_dados_entrada%ROWTYPE INDEX BY BINARY_INTEGER;
    v_nao_processados T_DADO_NAO_PROCESSADO;
    v_ativo            VARCHAR2(1);
    BEGIN
      BEGIN
        SELECT FG_PRC_ATIVO INTO v_ativo
        FROM CFG_SEMAFORO_PROCESSAMENTO
        WHERE CD_ID = 308;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        INSERT INTO CFG_SEMAFORO_PROCESSAMENTO (CD_ID, DESC_NOME_PRC, FG_PRC_ATIVO, DESC_COMENTARIO)
        VALUES (308, 'SGPA_PROCESSADOR_OPENWEATHER.PROC_INTEGRA_PRECIPITACAO_HIST', 'F', 'Move dados da ddp para a oficial');
        COMMIT;
        v_ativo := 'F';
      END;

      IF v_ativo = 'F' THEN
        UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'T', DT_HR_EXECUCAO_PROC = SYSDATE WHERE CD_ID = 308;
        COMMIT;

        OPEN cr_dados_entrada;
        LOOP FETCH cr_dados_entrada
        BULK COLLECT INTO v_nao_processados
        LIMIT 1000;
          EXIT WHEN v_nao_processados.COUNT = 0;
          FORALL I IN 1.. v_nao_processados.COUNT
          MERGE INTO DDN_PRECIPITACAO_TALHAO_HIST
          USING DUAL
          ON (
            CD_UNIDADE = v_nao_processados(I).CD_UNIDADE
            AND CD_FAZENDA = v_nao_processados(I).CD_FAZENDA
            AND CD_ZONA = v_nao_processados(I).CD_ZONA
            AND CD_TALHAO = v_nao_processados(I).CD_TALHAO
            AND DT_ANALISE = v_nao_processados(I).DT_ANALISE
          )
          WHEN NOT MATCHED THEN
            INSERT (CD_UNIDADE, CD_FAZENDA, CD_ZONA, CD_TALHAO, DT_ANALISE, CHUVA_ACUMULADA, QUANTIDADE_MEDICOES)
            VALUES (v_nao_processados(I).CD_UNIDADE, v_nao_processados(I).CD_FAZENDA, v_nao_processados(I).CD_ZONA
              , v_nao_processados(I).CD_TALHAO, v_nao_processados(I).DT_ANALISE, v_nao_processados(I).CHUVA_ACUMULADA
              , v_nao_processados(I).QUANTIDADE_MEDICOES)
          WHEN MATCHED THEN
            UPDATE SET CHUVA_ACUMULADA = v_nao_processados(I).CHUVA_ACUMULADA
              , QUANTIDADE_MEDICOES = v_nao_processados(I).QUANTIDADE_MEDICOES;

          FORALL I IN 1.. v_nao_processados.COUNT
          DELETE FROM DDP_PRECIPITACAO_TALHAO_HIST
          WHERE DDP_ID = v_nao_processados(I).DDP_ID;
          COMMIT;
        END LOOP;
        CLOSE cr_dados_entrada;

        UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'F', VL_TEMPO_EXECUCAO = ( SYSDATE - DT_HR_EXECUCAO_PROC ) * 86400 WHERE CD_ID = 308;
        COMMIT;
      END IF;
    END PROC_INTEGRA_PRECIPITACAO_HIST;
  PROCEDURE PROC_INTEGRA_UVI_HISTORICO IS
    CURSOR cr_dados_entrada IS
      SELECT
        t.DDP_ID,
        z.CD_UNIDADE,
        z.CD_FAZENDA,
        z.CD_ZONA,
        z.CD_TALHAO,
        TO_TIMESTAMP(t.DT_ANALISE, 'YYYY-MM-DD HH24:MI:SS') as DT_ANALISE,
        t.UVI
      FROM DDP_UVI_TALHAO_HISTORICO t
        JOIN CDT_NDVI_TALHAO_POLIGONO z ON t.POLIGONO = z.POLIGONO;
    TYPE T_DADO_NAO_PROCESSADO IS TABLE OF cr_dados_entrada%ROWTYPE INDEX BY BINARY_INTEGER;
    v_nao_processados T_DADO_NAO_PROCESSADO;
    v_ativo            VARCHAR2(1);
    BEGIN
      BEGIN
        SELECT FG_PRC_ATIVO INTO v_ativo
        FROM CFG_SEMAFORO_PROCESSAMENTO
        WHERE CD_ID = 309;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        INSERT INTO CFG_SEMAFORO_PROCESSAMENTO (CD_ID, DESC_NOME_PRC, FG_PRC_ATIVO, DESC_COMENTARIO)
        VALUES (309, 'SGPA_PROCESSADOR_OPENWEATHER.PROC_INTEGRA_UVI_HISTORICO', 'F', 'Move dados da ddp para a oficial');
        COMMIT;
        v_ativo := 'F';
      END;

      IF v_ativo = 'F' THEN
        UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'T', DT_HR_EXECUCAO_PROC = SYSDATE WHERE CD_ID = 309;
        COMMIT;

        OPEN cr_dados_entrada;
        LOOP FETCH cr_dados_entrada
        BULK COLLECT INTO v_nao_processados
        LIMIT 1000;
          EXIT WHEN v_nao_processados.COUNT = 0;
          FORALL I IN 1.. v_nao_processados.COUNT
          MERGE INTO DDN_UVI_TALHAO_HISTORICO
          USING DUAL
          ON (
            CD_UNIDADE = v_nao_processados(I).CD_UNIDADE
            AND CD_FAZENDA = v_nao_processados(I).CD_FAZENDA
            AND CD_ZONA = v_nao_processados(I).CD_ZONA
            AND CD_TALHAO = v_nao_processados(I).CD_TALHAO
            AND DT_ANALISE = v_nao_processados(I).DT_ANALISE
          )
          WHEN NOT MATCHED THEN
            INSERT (CD_UNIDADE, CD_FAZENDA, CD_ZONA, CD_TALHAO, DT_ANALISE, UVI)
            VALUES (v_nao_processados(I).CD_UNIDADE, v_nao_processados(I).CD_FAZENDA, v_nao_processados(I).CD_ZONA
              , v_nao_processados(I).CD_TALHAO, v_nao_processados(I).DT_ANALISE, v_nao_processados(I).UVI)
          WHEN MATCHED THEN
            UPDATE SET UVI = v_nao_processados(I).UVI;
          FORALL I IN 1.. v_nao_processados.COUNT
            DELETE FROM DDP_UVI_TALHAO_HISTORICO WHERE DDP_ID = v_nao_processados(I).DDP_ID;
          COMMIT;
        END LOOP;
        CLOSE cr_dados_entrada;

        UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'F', VL_TEMPO_EXECUCAO = ( SYSDATE - DT_HR_EXECUCAO_PROC ) * 86400 WHERE CD_ID = 309;
        COMMIT;
      END IF;
    END PROC_INTEGRA_UVI_HISTORICO;
  PROCEDURE PROC_INTEGRA_SOLO_HISTORICO IS
    CURSOR cr_dados_entrada IS
      SELECT
        t.DDP_ID,
        z.CD_UNIDADE,
        z.CD_FAZENDA,
        z.CD_ZONA,
        z.CD_TALHAO,
        TO_TIMESTAMP(t.DT_ANALISE, 'YYYY-MM-DD HH24:MI:SS') as DT_ANALISE,
        t.TEMP_SOLO,
        t.TEMP_SOLO_10_CM,
        t.HUMIDADE
      FROM DDP_DADOS_SOLO_HISTORICO t
        JOIN CDT_NDVI_TALHAO_POLIGONO z ON t.POLIGONO = z.POLIGONO;
    TYPE T_DADO_NAO_PROCESSADO IS TABLE OF cr_dados_entrada%ROWTYPE INDEX BY BINARY_INTEGER;
    v_nao_processados T_DADO_NAO_PROCESSADO;
    v_ativo            VARCHAR2(1);
    BEGIN
      BEGIN
        SELECT FG_PRC_ATIVO INTO v_ativo
        FROM CFG_SEMAFORO_PROCESSAMENTO
        WHERE CD_ID = 310;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        INSERT INTO CFG_SEMAFORO_PROCESSAMENTO (CD_ID, DESC_NOME_PRC, FG_PRC_ATIVO, DESC_COMENTARIO)
        VALUES (310, 'SGPA_PROCESSADOR_OPENWEATHER.PROC_INTEGRA_SOLO_HISTORICO', 'F', 'Move dados da ddp para a oficial');
        COMMIT;
        v_ativo := 'F';
      END;

      IF v_ativo = 'F' THEN
        UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'T', DT_HR_EXECUCAO_PROC = SYSDATE WHERE CD_ID = 310;
        COMMIT;

        OPEN cr_dados_entrada;
        LOOP FETCH cr_dados_entrada
        BULK COLLECT INTO v_nao_processados
        LIMIT 1000;
          EXIT WHEN v_nao_processados.COUNT = 0;
          FORALL I IN 1.. v_nao_processados.COUNT
          MERGE INTO DDN_DADOS_SOLO_HISTORICO
          USING DUAL
          ON (
            CD_UNIDADE = v_nao_processados(I).CD_UNIDADE
            AND CD_FAZENDA = v_nao_processados(I).CD_FAZENDA
            AND CD_ZONA = v_nao_processados(I).CD_ZONA
            AND CD_TALHAO = v_nao_processados(I).CD_TALHAO
            AND DT_ANALISE = v_nao_processados(I).DT_ANALISE
          )
          WHEN NOT MATCHED THEN
            INSERT (CD_UNIDADE, CD_FAZENDA, CD_ZONA, CD_TALHAO, DT_ANALISE, TEMP_SOLO, TEMP_SOLO_10_CM, HUMIDADE)
            VALUES (v_nao_processados(I).CD_UNIDADE, v_nao_processados(I).CD_FAZENDA, v_nao_processados(I).CD_ZONA
              , v_nao_processados(I).CD_TALHAO, v_nao_processados(I).DT_ANALISE, v_nao_processados(I).TEMP_SOLO
              , v_nao_processados(I).TEMP_SOLO_10_CM, v_nao_processados(I).HUMIDADE)
          WHEN MATCHED THEN
            UPDATE SET TEMP_SOLO = v_nao_processados(I).TEMP_SOLO
              , TEMP_SOLO_10_CM = v_nao_processados(I).TEMP_SOLO_10_CM
              ,  HUMIDADE = v_nao_processados(I).HUMIDADE;
          FORALL I IN 1.. v_nao_processados.COUNT
          DELETE FROM DDP_DADOS_SOLO_HISTORICO
            WHERE DDP_ID = v_nao_processados(I).DDP_ID;
          COMMIT;
        END LOOP;
        CLOSE cr_dados_entrada;

        UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'F', VL_TEMPO_EXECUCAO = ( SYSDATE - DT_HR_EXECUCAO_PROC ) * 86400 WHERE CD_ID = 310;
        COMMIT;
      END IF;
  END PROC_INTEGRA_SOLO_HISTORICO;

  PROCEDURE PROC_INTEGRA_CLIMA_HISTORICO IS
    CURSOR cr_dados_entrada IS
      SELECT
        t.DDP_ID,
        z.CD_UNIDADE,
        z.CD_FAZENDA,
        z.CD_ZONA,
        z.CD_TALHAO,
        TO_TIMESTAMP(t.DT_ANALISE, 'YYYY-MM-DD HH24:MI:SS') as DT_ANALISE,
        t.CLIMATE_ID,
        t.TEMPERATURA,
        t.PRESSAO,
        t.HUMIDADE,
        t.TEMPERATURA_MIN,
        t.TEMPERATURA_MAX,
        t.VELOCIDADE_VENTO,
        t.DIRECAO_GRAUS,
        t.PORCENTAGEM_NUVENS
      FROM DDP_CLIMA_TALHAO_HISTORICO t
        JOIN CDT_NDVI_TALHAO_POLIGONO z ON t.POLIGONO = z.POLIGONO;
    TYPE T_DADO_NAO_PROCESSADO IS TABLE OF cr_dados_entrada%ROWTYPE INDEX BY BINARY_INTEGER;
    v_nao_processados T_DADO_NAO_PROCESSADO;
    v_ativo            VARCHAR2(1);
    BEGIN
      BEGIN
        SELECT FG_PRC_ATIVO INTO v_ativo
        FROM CFG_SEMAFORO_PROCESSAMENTO
        WHERE CD_ID = 311;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        INSERT INTO CFG_SEMAFORO_PROCESSAMENTO (CD_ID, DESC_NOME_PRC, FG_PRC_ATIVO, DESC_COMENTARIO)
        VALUES (311, 'SGPA_PROCESSADOR_OPENWEATHER.PROC_INTEGRA_CLIMA_HISTORICO', 'F', 'Move dados da ddp para a oficial');
        COMMIT;
        v_ativo := 'F';
      END;

      IF v_ativo = 'F' THEN
        UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'T', DT_HR_EXECUCAO_PROC = SYSDATE WHERE CD_ID = 311;
        COMMIT;

        OPEN cr_dados_entrada;
        LOOP FETCH cr_dados_entrada
        BULK COLLECT INTO v_nao_processados
        LIMIT 10000;
          EXIT WHEN v_nao_processados.COUNT = 0;
          FORALL I IN 1.. v_nao_processados.COUNT
            MERGE INTO DDN_CLIMA_TALHAO_HISTORICO
            USING DUAL
            ON (
              CD_UNIDADE = v_nao_processados(I).CD_UNIDADE
              AND CD_FAZENDA = v_nao_processados(I).CD_FAZENDA
              AND CD_ZONA = v_nao_processados(I).CD_ZONA
              AND CD_TALHAO = v_nao_processados(I).CD_TALHAO
              AND DT_ANALISE = v_nao_processados(I).DT_ANALISE
            )
            WHEN NOT MATCHED THEN
              INSERT (CD_UNIDADE, CD_FAZENDA, CD_ZONA, CD_TALHAO, DT_ANALISE, CLIMATE_ID
              , TEMPERATURA, PRESSAO, HUMIDADE, TEMPERATURA_MIN, TEMPERATURA_MAX
              , VELOCIDADE_VENTO, DIRECAO_GRAUS, PORCENTAGEM_NUVENS)
              VALUES (v_nao_processados(I).CD_UNIDADE, v_nao_processados(I).CD_FAZENDA, v_nao_processados(I).CD_ZONA
                , v_nao_processados(I).CD_TALHAO, v_nao_processados(I).DT_ANALISE, v_nao_processados(I).CLIMATE_ID
                , v_nao_processados(I).TEMPERATURA, v_nao_processados(I).PRESSAO, v_nao_processados(I).HUMIDADE
                , v_nao_processados(I).TEMPERATURA_MIN, v_nao_processados(I).TEMPERATURA_MAX, v_nao_processados(I).VELOCIDADE_VENTO
                , v_nao_processados(I).DIRECAO_GRAUS, v_nao_processados(I).PORCENTAGEM_NUVENS)
            WHEN MATCHED THEN
              UPDATE SET CLIMATE_ID = v_nao_processados(I).CLIMATE_ID
                , TEMPERATURA = v_nao_processados(I).TEMPERATURA
                , PRESSAO = v_nao_processados(I).PRESSAO
                , HUMIDADE = v_nao_processados(I).HUMIDADE
                , TEMPERATURA_MIN = v_nao_processados(I).TEMPERATURA_MIN
                , TEMPERATURA_MAX = v_nao_processados(I).TEMPERATURA_MAX
                , VELOCIDADE_VENTO = v_nao_processados(I).VELOCIDADE_VENTO
                , DIRECAO_GRAUS = v_nao_processados(I).DIRECAO_GRAUS
                , PORCENTAGEM_NUVENS = v_nao_processados(I).PORCENTAGEM_NUVENS;
          FORALL I IN 1.. v_nao_processados.COUNT
            DELETE FROM DDP_CLIMA_TALHAO_HISTORICO
              WHERE DDP_ID = v_nao_processados(I).DDP_ID;
          COMMIT;
        END LOOP;
        CLOSE cr_dados_entrada;

        UPDATE CFG_SEMAFORO_PROCESSAMENTO SET FG_PRC_ATIVO = 'F', VL_TEMPO_EXECUCAO = ( SYSDATE - DT_HR_EXECUCAO_PROC ) * 86400 WHERE CD_ID = 311;
        COMMIT;
      END IF;
  END PROC_INTEGRA_CLIMA_HISTORICO;
END SGPA_PROCESSADOR_OPENWEATHER;
