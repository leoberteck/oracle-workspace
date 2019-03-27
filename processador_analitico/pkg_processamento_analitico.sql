CREATE OR REPLACE TYPE MAP_GEOJSON_T AS OBJECT (
  TYPE VARCHAR2(255), ---LINHAS, SOBREPOSICAO, AREA_TRABALHADA, AREA_FALHA
  GEOJSON BLOB
);

CREATE OR REPLACE TYPE TABLE_MAP_GEOJSON_T IS TABLE OF MAP_GEOJSON_T;

CREATE OR REPLACE FUNCTION RANDOM_COLOR RETURN VARCHAR2 IS
  v_color VARCHAR2(7);
BEGIN
  SELECT '#' || TO_CHAR( TRUNC( DBMS_RANDOM.VALUE( 0, 256*256*256 ) ), 'FM0XXXXX' ) INTO v_color FROM DUAL;
  RETURN v_color;
END;
/
CREATE OR REPLACE PACKAGE PKG_PROCESSAMENTO_ANALITICO AS
  /*
  ================================================================================================
  ================================CALLBACKS DE NOTIFICATION CHANGE================================
  ================================================================================================
  */

  PROCEDURE ON_BATCH_UPDATE(ntfnds IN CQ_NOTIFICATION$_DESCRIPTOR);

  /*
  Busca todos registros de uma batch de um determinado cliente e batch
  processa esses dados e os prepara para serem inseridos na tabela
  GEO_LAYER_OPERACAO_TALHAO.

  Se, ao final da execucao, detectar que nao haverao mais funcoes DO_BATCH
  executando paralelamente, dispara a funcao UPDATE_EVENTOS_TALHAO de maneira
  asincrona e termina.
  */
  PROCEDURE DO_BATCH(cdCliente NUMBER, batchId NUMBER);
  PROCEDURE DO_BATCHES;
  /*
  Busca todos os registros da GEO_LAYER_OPERACAO_TALHAO que foram inseridos ou
  alterados desde a ultima execucao desta funcao.

  Agrupa esses registros conforme o tempo passado entre a realizacao de uma
  determinada operacao em um mesmo talhao talhao. Quando esse tempo passa
  do valor do parametro em dias, um novo evento e criado. Exemplo:
  -> Se o valor do parametro de dias for igual a 30
  -> Um registro indica a realizacao da operacao 4017 no talhao 1 nas datas
    2019-01-01 12:00:00 -----|
    2019-01-01 12:10:00      |
    ...                      | -> um evento que foi de 2019-01-01 12:00:00 a 2019-01-15 15:00:00
    2019-01-15 14:30:00      |
    2019-01-15 15:00:00 -----|
    2019-02-26 06:00:00 -----|
    ...                      | -> outro evento que foi de 2019-02-26 06:00:00 a 2019-02-28 18:00:00
    2019-02-28 18:00:00 -----| pois se passaram mais de 30 dias desde a ultima realizacao da mesma
                               operacao no mesmo talhao

  */
  PROCEDURE UPDATE_EVENTOS_TALHAO;

  /*
  Normaliza e tabula os metadados gerados pelo tippecanoe na geracao do mapa.
  */
  PROCEDURE PRC_DESCRIBE_ATTRIBUTES(p_map_id NUMBER);
  /*
  Gera as mapbox layers para um deteminado mapa baseado no que foi cadastrado na tabela EVENTOS_OPERACAO_LAYERS
  */
  PROCEDURE PRC_BUILD_LAYERS(p_map_id NUMBER, p_event_id NUMBER);

  /*
  ================================================================================================
  ===================FUNCOES QUE SAO RESPONSAVEIS PELA GERACAO DE LAYERS MAPBOX===================
  ================================================================================================
  */
  FUNCTION FNC_VELOCIDADE_LAYER(p_map_id NUMBER) RETURN CLOB;
  FUNCTION FNC_VELOCIDADE_VENTO_LAYER(p_map_id NUMBER) RETURN CLOB;
  FUNCTION FNC_RPM_LAYER(p_map_id NUMBER) RETURN CLOB;
  FUNCTION FNC_ESTADO_LAYER(p_map_id NUMBER) RETURN CLOB;
  FUNCTION FNC_EQUIPAMENTO_LAYER(p_map_id NUMBER) RETURN CLOB;
  FUNCTION FNC_SOBREPOSICAO_LAYER RETURN CLOB;
  FUNCTION FNC_AREA_TRABALHADA_LAYER RETURN CLOB;
  FUNCTION FNC_AREA_FALHA_LAYER RETURN CLOB;
  FUNCTION FNC_SYMBOL_LAYER(p_prop_name VARCHAR2) RETURN JSON_OBJECT_T;
  FUNCTION FNC_LINE_LAYER_NUMBER_PROP(p_map_id NUMBER, p_prop_name VARCHAR2) RETURN JSON_OBJECT_T;
  FUNCTION FNC_LINE_LAYER_STATIC_PROP(p_map_id NUMBER, p_prop_name VARCHAR2) RETURN JSON_OBJECT_T;

  /*
  ================================================================================================
  =======================FUNCOES QUE SAO RESPONSAVEIS PELA GERACAO GEOJSON========================
  ================================================================================================
  */

  FUNCTION FNC_GEOJSON_EVENTO(p_cd_cliente NUMBER, p_cd_evento NUMBER) RETURN TABLE_MAP_GEOJSON_T PIPELINED;

END PKG_PROCESSAMENTO_ANALITICO;
/
CREATE OR REPLACE PACKAGE BODY PKG_PROCESSAMENTO_ANALITICO AS

  PROCEDURE ON_BATCH_UPDATE(ntfnds IN CQ_NOTIFICATION$_DESCRIPTOR) IS
    v_row_id ROWID;
    v_logger LOGGER;
    v_proc_name VARCHAR2(128) := 'PKG_PROCESSAMENTO_ANALITICO.ON_BATCH_UPDATE';

    v_cd_cliente NUMBER;
    v_batch_id NUMBER;
    v_status VARCHAR2(50);

    v_job NUMBER;
  BEGIN
    v_logger := GET_LOGGER(v_proc_name, '{}', 'PKG_PROCESSAMENTO_ANALITICO', 'PACKAGE BODY');
    BEGIN
      IF ntfnds.event_type = DBMS_CQ_NOTIFICATION.EVENT_QUERYCHANGE THEN
        v_logger.DEBUG('query changed');
        FOR i IN 1..ntfnds.query_desc_array.count LOOP -- loop over queries
          v_logger.DEBUG('changed query id: ' || ntfnds.query_desc_array(i).queryid || '. operation type: ' || ntfnds.query_desc_array(i).queryop);
          FOR j IN 1..ntfnds.query_desc_array(i).table_desc_array.count LOOP -- loop over tables
            v_logger.DEBUG('changed table name: ' || ntfnds.query_desc_array(i).table_desc_array(j).table_name || '. operation type: ' || ntfnds.query_desc_array(i).table_desc_array(j).opflags);
            --Se a operacao da tabela for UPDATE
            FOR k IN 1..ntfnds.query_desc_array(i).table_desc_array(j).numrows LOOP -- loop over rows
              v_row_id := ntfnds.query_desc_array(i).table_desc_array(j).row_desc_array(k).row_id;
              v_logger.DEBUG('changed rowid: ' || v_row_id);
              BEGIN
                SELECT CD_CLIENTE, CD_ID, STATUS INTO v_cd_cliente, v_batch_id, v_status
                FROM TMP_MAPA_VAR_BATCH WHERE ROWID = v_row_id;
                IF v_status = 'FULL' THEN
                    DBMS_JOB.SUBMIT(
                      job => v_job
                      , what => 'BEGIN PKG_PROCESSAMENTO_ANALITICO.DO_BATCH(' || v_cd_cliente || ',' || v_batch_id || '); END;'
                      , instance => 1
                    );
                END IF;
              EXCEPTION WHEN NO_DATA_FOUND THEN
                v_logger.DEBUG('deleted');
              END;
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

  PROCEDURE DO_BATCHES IS
    v_cd_cliente NUMBER;
    v_batch_id NUMBER;

    v_logger LOGGER;
  BEGIN
    v_logger := GET_LOGGER('PKG_PROCESSAMENTO_ANALITICO.DO_BATCHES', '{}', 'PKG_PROCESSAMENTO_ANALITICO', 'PACKAGE BODY');
    BEGIN
      BEGIN
        SELECT CD_CLIENTE, CD_ID INTO v_cd_cliente, v_batch_id FROM TMP_MAPA_VAR_BATCH WHERE STATUS = 'FULL'
          FETCH FIRST 1 ROWS ONLY;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        v_cd_cliente := NULL;
        v_batch_id := NULL;
      END;

      WHILE v_cd_cliente IS NOT NULL AND v_batch_id IS NOT NULL LOOP
        DO_BATCH(v_cd_cliente, v_batch_id);

        BEGIN
          SELECT CD_CLIENTE, CD_ID INTO v_cd_cliente, v_batch_id FROM TMP_MAPA_VAR_BATCH WHERE STATUS = 'FULL'
            FETCH FIRST 1 ROWS ONLY;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          v_cd_cliente := NULL;
          v_batch_id := NULL;
        END;
      END LOOP;
    EXCEPTION WHEN OTHERS THEN
      v_logger.FATAL(SQLCODE, SQLERRM, 'FATAL ERROR');
    END;
    v_logger.ENDRUN();
    COMMIT;
  END;

  PROCEDURE DO_BATCH(cdCliente NUMBER, batchId NUMBER) IS
    v_logger LOGGER;
    v_error_line NUMBER;
    v_proc_name VARCHAR2(255) := 'PKG_PROCESSAMENTO_ANALITICO.DO_BATCH';
    v_other_instances_count NUMBER;
    v_job NUMBER;
    v_proc_instance NUMBER := 1;
  BEGIN
    v_logger := GET_LOGGER('PKG_PROCESSAMENTO_ANALITICO.DO_BATCH', JSON_OBJECT('cdCliente' VALUE cdCliente, 'batchId' VALUE batchId), 'PKG_PROCESSAMENTO_ANALITICO', 'PACKAGE BODY');
    BEGIN
      v_logger.INFO('Inicio do processamento da batch ' || batchId);
      UPDATE TMP_MAPA_VAR_BATCH SET STATUS = 'IN_PROCESS' WHERE CD_CLIENTE = cdCliente AND CD_ID = batchId;
      COMMIT;
      MERGE INTO GEO_LAYER_OPERACAO_TALHAO t_glot
      USING (
        WITH
          /*Dentro de uma batch selecionada, seleciona o id da tabela GEO_LAYER_MAPA_VAR de origem e, para cada registro
          , cria uma linha ou um ponto, dependendo da relacao de equalidade entre as lat long inicias e finais.
          */
          build_lines as (
            SELECT glmv.CD_MAPA_VAR,
              CASE
               WHEN glmv.VL_LONGITUDE_INICIAL <> glmv.VL_LONGITUDE_FINAL
                   AND glmv.VL_LATITUDE_INICIAL <> glmv.VL_LATITUDE_FINAL
               THEN
                 --Cria uma lina
                 MDSYS.SDO_GEOMETRY(
                     2002
                   , 4326
                   , NULL
                   , MDSYS.SDO_ELEM_INFO_ARRAY(1, 2, 1)
                   , MDSYS.SDO_ORDINATE_ARRAY(
                       glmv.VL_LONGITUDE_INICIAL
                     , glmv.VL_LATITUDE_INICIAL
                     , glmv.VL_LONGITUDE_FINAL
                     , glmv.VL_LATITUDE_FINAL
                     )
                   )
               ELSE
                 --Cria um ponto
                 MDSYS.SDO_GEOMETRY(
                   2001,
                   4326,
                   MDSYS.SDO_POINT_TYPE(VL_LONGITUDE_INICIAL, VL_LATITUDE_FINAL, NULL),
                   NULL,
                   NULL
                 )
               END LINE
            FROM TMP_MAPA_VAR glmv
            WHERE CD_CLIENTE = cdCliente
              AND BATCH_ID = batchId
          )
          , localize_fields as (
            /*
            Localiza as geometrias geradas
            1 - Busca os talhoes proximos em um raio de 10 metros
            2 - Determina a relacao entre a linha e o talhao com o metodo MDSYS.SDO_GEOM.RELATE.
            3 - Determina o talhao da geometria como os talhoes mais proximos e que tenham uma relacaoigual a TRUE
            */
            SELECT
              --Forca o uso do indice geometrico na busca
              /*+ LEADING(wl) USE_NL(wl, glt) INDEX(glt GEO_LAYER_TALHAO_IDX)*/
              wl.CD_MAPA_VAR CD_MAPA_VAR,
              glt.CD_UNIDADE,
              glt.CD_FAZENDA,
              glt.CD_ZONA,
              glt.CD_TALHAO,
              glt.GEOMETRIA,
              wl.LINE,
              MDSYS.SDO_GEOM.RELATE(glt.GEOMETRIA, 'DETERMINE', wl.LINE, 0.05) RELATION
            FROM build_lines wl, GEO_LAYER_TALHAO glt
            WHERE glt.CD_CLIENTE = cdCliente
              AND MDSYS.SDO_NN(glt.GEOMETRIA, wl.LINE, 'sdo_batch_size=10 distance=10 unit=meter', 1) = 'TRUE'
          )
          -- Retorna somente linhas com alguma interacao com o talhao
          , have_interact as (
            SELECT *
            FROM localize_fields
            WHERE MDSYS.SDO_GEOM.RELATE(GEOMETRIA, 'DETERMINE', LINE, 0.05) <> 'DISJOINT'
          )
          /*
          Faz a uniao das linhas que passam dentro de algum talhao com aquelas que estao completamente fora de qualquer talhao
          */
          , field_interaction as (
          SELECT CD_MAPA_VAR
              , CD_UNIDADE
              , CD_FAZENDA
              , CD_ZONA
              , CD_TALHAO
              , GEOMETRIA
              , LINE
              , RELATION
            FROM have_interact
            UNION ALL
            SELECT CD_MAPA_VAR
              , NULL       CD_UNIDADE
              , NULL       CD_FAZENDA
              , NULL       CD_ZONA
              , NULL       CD_TALHAO
              , NULL       GEOMETRIA
              , LINE
              , 'DISJOINT' RELATION
            FROM build_lines
            WHERE CD_MAPA_VAR NOT IN (SELECT CD_MAPA_VAR FROM have_interact)
          )
          /*Para as linhas que passam dentro de algum talhao, retira partes da linha que estao fora do talhao onde interagem
               antes		  depois
            ___________        ___________
            |	        |        |	       |
            |	        |        |	       |
            |	    ----|-----   |     ----|
            |	        |        |	       |
            ___________        ___________
          */
          , trim_lines_within_field as (
            SELECT wni.CD_MAPA_VAR
              , wni.CD_UNIDADE
              , wni.CD_FAZENDA
              , wni.CD_ZONA
              , wni.CD_TALHAO
              , CASE WHEN RELATION = 'DISJOINT' OR RELATION LIKE '%UNKNOWN%'
               THEN wni.LINE
               ELSE MDSYS.SDO_GEOM.SDO_INTERSECTION(
                  wni.GEOMETRIA
                 , wni.LINE
                 , 0.5
                 )
              END LINE
            FROM field_interaction wni
          )
          /*Para as linhas que passam dentro de algum talhao, retira partes da linha que estao dentro do talhao e geram um registro nao localizado

              antes		            depois
            ___________        ___________
            | 	      |        |	       |
            | 	      |        |	       |
            | 	  ----|-----   |	       |-----
            | 	      |        |	       |
            ___________        ___________
          */
          , trim_lines_outside_field as (
            SELECT t.CD_MAPA_VAR
              , NULL CD_UNIDADE
              , NULL CD_FAZENDA
              , NULL CD_ZONA
              , NULL CD_TALHAO
              , MDSYS.SDO_GEOM.SDO_DIFFERENCE(
                (SELECT LINE FROM field_interaction WHERE CD_MAPA_VAR = t.CD_MAPA_VAR AND ROWNUM = 1)
               , t.GEOMETRIES
               , 0.05
               )  LINE
            FROM (
             -- Cria um poligono so de todos os talhoes que interagem com a linha
             SELECT wni2.CD_MAPA_VAR
              , MDSYS.SDO_AGGR_UNION(
                  MDSYS.SDOAGGRTYPE(wni2.GEOMETRIA, 0.05)
              ) GEOMETRIES
             FROM field_interaction wni2
             WHERE wni2.RELATION = 'OVERLAPBDYDISJOINT'
             GROUP BY wni2.CD_MAPA_VAR
            ) t
          )
          --Une todas as linhas em um resultset so
          , all_resulting_lines as (
            SELECT *
            FROM trim_lines_within_field
            UNION ALL
            SELECT *
            FROM trim_lines_outside_field
          )
          --Finalmente retorna o resultado pronto para ser inserido na GEO_LAYER_O PERACAO_TALHAO
          SELECT cdCliente CD_CLIENTE
            , tmv.ORIGIN_ROWID
            , arl.CD_MAPA_VAR
            , glt.CD_ID CD_TALHAO
            , arl.LINE  LINHA
            , CASE
             WHEN MDSYS.SDO_GEOMETRY.GET_GTYPE(arl.LINE) = 2 AND tmv.CD_ESTADO IN ('E', 'S') THEN
               MDSYS.SDO_GEOM.SDO_BUFFER(
                   arl.LINE
                 , (select DIMINFO from MDSYS.USER_SDO_GEOM_METADATA
                    WHERE TABLE_NAME = 'GEO_LAYER_OPERACAO_TALHAO' AND COLUMN_NAME = 'GEOMETRIA')
                 , tmv.VL_LARGURA_IMPLEMENTO / 2
                 , 'unit=m arc_tolerance=0.05'
                 )
             ELSE arl.LINE
            END GEOMETRIA
            , tmv.CD_EQUIPAMENTO
            , tmv.DESC_EQUIPAMENTO
            , tmv.FG_TP_EQUIPAMENTO
            , tmv.FG_FRENTE_TRABALHO
            , tmv.CD_EQUIPE
            , tmv.DT_HR_UTC_INICIAL
            , tmv.DT_HR_UTC_FINAL
            , tmv.VL_TEMPO_SEGUNDOS
            , tmv.VL_DISTANCIA_METROS
            , tmv.CD_OPERACAO
            , tmv.DESC_OPERACAO
            , tmv.VL_VELOCIDADE
            , tmv.CD_OPERADOR
            , tmv.DESC_OPERADOR
            , tmv.CD_ESTADO
            , tmv.QT_SECAO_PULVERIZADOR_ANTERIOR
            , tmv.CD_IMPLEMENTO
            , tmv.VL_VELOCIDADE_VENTO
            , tmv.VL_TEMPERATURA
            , tmv.VL_UMIDADE
            , tmv.VL_CONSUMO_INSTANTANEO
            , tmv.VL_RPM
            , tmv.VL_TEMPERATURA_MOTOR
            , tmv.VL_ORDEM_SERVICO
            , tmv.VL_ALARME
            , tmv.VL_ALERTA_CLIMA
            , tmv.VL_ALARME_CLIMA
            , tmv.QT_SECAO_PULVERIZADOR
            , tmv.VL_HECTARES_HORA
            , tmv.CD_OPERAC_PARADA
            , tmv.DESC_OPERAC_PARADA
            , tmv.VL_HORIMETRO_INICIAL
            , tmv.VL_HORIMETRO_FINAL
            , tmv.VL_LARGURA_IMPLEMENTO
            , tmv.CD_JORNADA
            , tmv.CD_ID_DETALHESOP
            , tmv.CD_OPERACAO_CB
            , tmv.CD_TIPO_BICO
            , tmv.DESC_TIPO_BICO
            , tmv.VL_RENDIMENTO_COLHEITA
            , tmv.VL_UMIDADE_GRAOS
            , tmv.VL_HECTARES_HORA_M
            , tmv.VL_PONTO_ORVALHO
            , tmv.VL_PARTICULAS_OLEO
            , tmv.VL_RENDIMENTO_COLHEITA_M
            , tmv.VL_UMIDADE_GRAOS_M
            , tmv.VL_VAZAO_LITROS_HA
            , tmv.VL_VAZAO_LITROS_MIN
            , tmv.VL_VAZAO_LITROS_HA_M
            , tmv.VL_VAZAO_LITROS_MIN_M
            , tmv.VL_DOSAGEM_TAXA1
            , tmv.VL_DOSAGEM_TAXA2
            , tmv.VL_DOSAGEM_TAXA3
            , tmv.VL_PRESSAO_BOMBA
            , tmv.VL_PRESSAO_BOMBA_M
          FROM all_resulting_lines arl
          join TMP_MAPA_VAR tmv on (
              tmv.CD_CLIENTE = cdCliente
              AND tmv.BATCH_ID = batchId
              AND tmv.CD_MAPA_VAR = arl.CD_MAPA_VAR
            )
          left join GEO_LAYER_TALHAO glt ON (
              glt.CD_CLIENTE = cdCliente
              AND glt.CD_UNIDADE = arl.CD_UNIDADE
              AND glt.CD_FAZENDA = arl.CD_FAZENDA
              AND glt.CD_ZONA = arl.CD_ZONA
              AND glt.CD_TALHAO = arl.CD_TALHAO
            )
      ) dataset
      ON (
        NVL(t_glot.CD_CLIENTE, 0) = NVL(dataset.CD_CLIENTE, 0)
        AND NVL(t_glot.CD_TALHAO, 0) = NVL(dataset.CD_TALHAO, 0)
        AND t_glot.DT_HR_UTC_INICIAL = dataset.DT_HR_UTC_INICIAL
        AND t_glot.DT_HR_UTC_FINAL = dataset.DT_HR_UTC_FINAL
        AND NVL(t_glot.CD_OPERACAO, 0) = NVL(dataset.CD_OPERACAO, 0)
        AND NVL(t_glot.CD_OPERAC_PARADA, 0) = NVL(dataset.CD_OPERAC_PARADA, 0)
        AND NVL(t_glot.CD_JORNADA, 0) = NVL(dataset.CD_JORNADA, 0)
        AND NVL(t_glot.FG_TP_EQUIPAMENTO, 0) = NVL(dataset.FG_TP_EQUIPAMENTO, 0)
        AND NVL(t_glot.CD_EQUIPAMENTO, '0') = NVL(dataset.CD_EQUIPAMENTO, '0')
        AND NVL(t_glot.CD_IMPLEMENTO, '0') = NVL(dataset.CD_IMPLEMENTO, '0')
        AND NVL(t_glot.CD_EQUIPE, 0) = NVL(dataset.CD_EQUIPE, 0)
        AND NVL(t_glot.CD_OPERADOR, 0) = NVL(dataset.CD_OPERADOR, 0)
        AND NVL(t_glot.CD_ESTADO, 0) = NVL(dataset.CD_ESTADO, 0)
      ) WHEN NOT MATCHED THEN
        INSERT (
          t_glot.CD_CLIENTE
          , t_glot.ORIGIN_ROWID
          , t_glot.CD_MAPA_VAR
          , t_glot.CD_TALHAO
          , t_glot.LINHA
          , t_glot.GEOMETRIA
          , t_glot.CD_EQUIPAMENTO
          , t_glot.DESC_EQUIPAMENTO
          , t_glot.FG_TP_EQUIPAMENTO
          , t_glot.FG_FRENTE_TRABALHO
          , t_glot.CD_EQUIPE
          , t_glot.DT_HR_UTC_INICIAL
          , t_glot.DT_HR_UTC_FINAL
          , t_glot.VL_TEMPO_SEGUNDOS
          , t_glot.VL_DISTANCIA_METROS
          , t_glot.CD_OPERACAO
          , t_glot.DESC_OPERACAO
          , t_glot.VL_VELOCIDADE
          , t_glot.CD_OPERADOR
          , t_glot.DESC_OPERADOR
          , t_glot.CD_ESTADO
          , t_glot.QT_SECAO_PULVERIZADOR_ANTERIOR
          , t_glot.CD_IMPLEMENTO
          , t_glot.VL_VELOCIDADE_VENTO
          , t_glot.VL_TEMPERATURA
          , t_glot.VL_UMIDADE
          , t_glot.VL_CONSUMO_INSTANTANEO
          , t_glot.VL_RPM
          , t_glot.VL_TEMPERATURA_MOTOR
          , t_glot.VL_ORDEM_SERVICO
          , t_glot.VL_ALARME
          , t_glot.VL_ALERTA_CLIMA
          , t_glot.VL_ALARME_CLIMA
          , t_glot.QT_SECAO_PULVERIZADOR
          , t_glot.VL_HECTARES_HORA
          , t_glot.CD_OPERAC_PARADA
          , t_glot.DESC_OPERAC_PARADA
          , t_glot.VL_HORIMETRO_INICIAL
          , t_glot.VL_HORIMETRO_FINAL
          , t_glot.VL_LARGURA_IMPLEMENTO
          , t_glot.CD_JORNADA
          , t_glot.CD_ID_DETALHESOP
          , t_glot.CD_OPERACAO_CB
          , t_glot.CD_TIPO_BICO
          , t_glot.DESC_TIPO_BICO
          , t_glot.VL_RENDIMENTO_COLHEITA
          , t_glot.VL_UMIDADE_GRAOS
          , t_glot.VL_HECTARES_HORA_M
          , t_glot.VL_PONTO_ORVALHO
          , t_glot.VL_PARTICULAS_OLEO
          , t_glot.VL_RENDIMENTO_COLHEITA_M
          , t_glot.VL_UMIDADE_GRAOS_M
          , t_glot.VL_VAZAO_LITROS_HA
          , t_glot.VL_VAZAO_LITROS_MIN
          , t_glot.VL_VAZAO_LITROS_HA_M
          , t_glot.VL_VAZAO_LITROS_MIN_M
          , t_glot.VL_DOSAGEM_TAXA1
          , t_glot.VL_DOSAGEM_TAXA2
          , t_glot.VL_DOSAGEM_TAXA3
          , t_glot.VL_PRESSAO_BOMBA
          , t_glot.VL_PRESSAO_BOMBA_M)
        VALUES (
          dataset.CD_CLIENTE
          , dataset.ORIGIN_ROWID
          , dataset.CD_MAPA_VAR
          , dataset.CD_TALHAO
          , dataset.LINHA
          , dataset.GEOMETRIA
          , dataset.CD_EQUIPAMENTO
          , dataset.DESC_EQUIPAMENTO
          , dataset.FG_TP_EQUIPAMENTO
          , dataset.FG_FRENTE_TRABALHO
          , dataset.CD_EQUIPE
          , dataset.DT_HR_UTC_INICIAL
          , dataset.DT_HR_UTC_FINAL
          , dataset.VL_TEMPO_SEGUNDOS
          , dataset.VL_DISTANCIA_METROS
          , dataset.CD_OPERACAO
          , dataset.DESC_OPERACAO
          , dataset.VL_VELOCIDADE
          , dataset.CD_OPERADOR
          , dataset.DESC_OPERADOR
          , dataset.CD_ESTADO
          , dataset.QT_SECAO_PULVERIZADOR_ANTERIOR
          , dataset.CD_IMPLEMENTO
          , dataset.VL_VELOCIDADE_VENTO
          , dataset.VL_TEMPERATURA
          , dataset.VL_UMIDADE
          , dataset.VL_CONSUMO_INSTANTANEO
          , dataset.VL_RPM
          , dataset.VL_TEMPERATURA_MOTOR
          , dataset.VL_ORDEM_SERVICO
          , dataset.VL_ALARME
          , dataset.VL_ALERTA_CLIMA
          , dataset.VL_ALARME_CLIMA
          , dataset.QT_SECAO_PULVERIZADOR
          , dataset.VL_HECTARES_HORA
          , dataset.CD_OPERAC_PARADA
          , dataset.DESC_OPERAC_PARADA
          , dataset.VL_HORIMETRO_INICIAL
          , dataset.VL_HORIMETRO_FINAL
          , dataset.VL_LARGURA_IMPLEMENTO
          , dataset.CD_JORNADA
          , dataset.CD_ID_DETALHESOP
          , dataset.CD_OPERACAO_CB
          , dataset.CD_TIPO_BICO
          , dataset.DESC_TIPO_BICO
          , dataset.VL_RENDIMENTO_COLHEITA
          , dataset.VL_UMIDADE_GRAOS
          , dataset.VL_HECTARES_HORA_M
          , dataset.VL_PONTO_ORVALHO
          , dataset.VL_PARTICULAS_OLEO
          , dataset.VL_RENDIMENTO_COLHEITA_M
          , dataset.VL_UMIDADE_GRAOS_M
          , dataset.VL_VAZAO_LITROS_HA
          , dataset.VL_VAZAO_LITROS_MIN
          , dataset.VL_VAZAO_LITROS_HA_M
          , dataset.VL_VAZAO_LITROS_MIN_M
          , dataset.VL_DOSAGEM_TAXA1
          , dataset.VL_DOSAGEM_TAXA2
          , dataset.VL_DOSAGEM_TAXA3
          , dataset.VL_PRESSAO_BOMBA
          , dataset.VL_PRESSAO_BOMBA_M
        );
      DELETE FROM TMP_MAPA_VAR_BATCH WHERE CD_CLIENTE = cdCliente AND CD_ID = batchId;
      COMMIT;
      SELECT COUNT(1) INTO v_other_instances_count FROM PROCEDURE_EXECUCAO
        WHERE PROC_NAME = v_proc_name AND RUNNING = 1 AND CD_ID <> v_logger.V_SEMAFORO_ID;

      /* Caso esta seja a ultima instancia da procedure a ser
      concluida, dispara o processamento do update de eventos de talhao
      */
      IF v_other_instances_count = 0 THEN
        DBMS_JOB.SUBMIT(
          job => v_job
          , what => 'BEGIN PKG_PROCESSAMENTO_ANALITICO.UPDATE_EVENTOS_TALHAO(); END;'
          , instance => v_proc_instance
        );
        v_logger.INFO('Fim da ultima instancia DO_BATCH. Comecando UPDATE_EVENTOS_TALHAO com JobId: ' || v_job);
      END IF;
    EXCEPTION WHEN OTHERS THEN
      UPDATE TMP_MAPA_VAR_BATCH SET STATUS = 'ERROR' WHERE CD_CLIENTE = cdCliente AND CD_ID = batchId;
      v_logger.FATAL(SQLCODE, SQLERRM, 'FATAL ERROR');
    END;
    v_logger.ENDRUN();
    COMMIT;
  END;

  PROCEDURE UPDATE_EVENTOS_TALHAO IS

    CURSOR cr_cursor IS SELECT * FROM (
      WITH dt_start AS (
        --Pega a ultima data em que a procedure foi executada
          SELECT MAX(DT_CREATED) dt
          FROM PROCEDURE_EXECUCAO
          WHERE PROC_NAME = 'PKG_PROCESSAMENTO_ANALITICO.UPDATE_EVENTOS_TALHAO'
            AND RUNNING = 0 AND HAS_ERROS = 0
        )
        , changed_events_glot as (
          /* Pega a menor data de inicio dos registros
          que foram criados ou alterados desde a ultima
          execucao da procedure.
          agrupando tudo por cliente, operacao e talhao
          */
          SELECT
            glot.CD_CLIENTE
            , glot.CD_TALHAO
            , glot.CD_OPERACAO_CB
            , MIN(glot.DT_HR_UTC_INICIAL) DATA
          FROM GEO_LAYER_OPERACAO_TALHAO glot
          WHERE
            ( glot.DT_CREATED >= NVL((select dt from dt_start), glot.DT_CREATED)
              OR glot.DT_UPDATED >= NVL((select dt from dt_start), glot.DT_UPDATED)
            ) AND glot.CD_OPERACAO_CB IS NOT NULL
            AND glot.CD_TALHAO IS NOT NULL
          GROUP BY glot.CD_CLIENTE, glot.CD_TALHAO, glot.CD_OPERACAO_CB
        )
        , changed_events_eot as (
          /* Para os registros que mudaram e ja
          possuem registro na EVENTOS_OPERACAO_TALHAO
          busca a menor data de inicio desse registro
          na tabela de evento e agrupa por cliente, operacao e talhao
          */
          SELECT ce.CD_CLIENTE
               , ce.CD_TALHAO
               , ce.CD_OPERACAO_CB
               , MIN(eot.DT_INICIO) DATA
          FROM changed_events_glot ce
            LEFT JOIN EVENTOS_OPERACAO_TALHAO eot ON (
              eot.CD_CLIENTE = ce.CD_CLIENTE
              AND eot.CD_OPERACAO = ce.CD_OPERACAO_CB
              AND eot.CD_TALHAO = ce.CD_TALHAO
              AND ce.DATA BETWEEN eot.DT_INICIO AND eot.DT_FIM
            )
          GROUP BY ce.CD_CLIENTE, ce.CD_TALHAO, ce.CD_OPERACAO_CB
        )
        , geo_layer_filtro as (
          /*
          Define o filtro a ser feito na GEO_LAYER_OPERACAO_TALHAO
          Como o cliente, talhao, operacao que mudaram
          e buscando registros com data de inicio maior do que
          */
          SELECT t.CD_CLIENTE, t.CD_TALHAO, t.CD_OPERACAO_CB, MIN(DATA) DATA
          FROM (
             SELECT *
             from changed_events_glot
             UNION
             SELECT *
             from changed_events_eot
             WHERE DATA IS NOT NULL
           ) t
          GROUP BY t.CD_CLIENTE, t.CD_TALHAO, t.CD_OPERACAO_CB
        )
        , geo_layer_filtrada as (
          SELECT glot.CD_CLIENTE
            , glot.CD_TALHAO
            , glot.CD_OPERACAO_CB
            , glot.DT_HR_UTC_INICIAL
            , glot.DT_HR_UTC_FINAL
          FROM GEO_LAYER_OPERACAO_TALHAO glot
            , geo_layer_filtro glf
            WHERE glot.CD_CLIENTE = glf.CD_CLIENTE
              AND glot.CD_TALHAO = glf.CD_TALHAO
              AND glot.CD_OPERACAO_CB = glf.CD_OPERACAO_CB
              AND glot.DT_HR_UTC_INICIAL >= glf.DATA
        )
        , starts AS (
          SELECT t.CD_CLIENTE
           , t.CD_TALHAO
           , t.CD_OPERACAO_CB
           , t.DATA
           , ROW_NUMBER() OVER ( ORDER BY t.CD_OPERACAO_CB, t.DATA ) RN
          FROM (
            SELECT r.CD_CLIENTE
              , r.CD_TALHAO
              , r.CD_OPERACAO_CB
              , r.DT_HR_UTC_INICIAL DATA
              , r.DT_HR_UTC_INICIAL - NVL(
                LAG(r.DT_HR_UTC_FINAL) OVER (
                  PARTITION BY
                    r.CD_CLIENTE, r.CD_TALHAO, r.CD_OPERACAO_CB
                  ORDER BY
                    r.CD_CLIENTE, r.CD_TALHAO, r.CD_OPERACAO_CB, r.DT_HR_UTC_INICIAL
                )
                , r.DT_HR_UTC_INICIAL + 1
              ) DIFF
            FROM geo_layer_filtrada r
            ) t
          where t.DIFF = -1 or t.DIFF >= 30
        )
        , ends AS (
          SELECT t.CD_CLIENTE
            , t.CD_TALHAO
            , t.CD_OPERACAO_CB
            , t.DATA
            , ROW_NUMBER() OVER ( ORDER BY t.CD_OPERACAO_CB, t.DATA ) RN
          FROM (
            SELECT r.CD_CLIENTE
              , r.CD_TALHAO
              , r.CD_OPERACAO_CB
              , r.DT_HR_UTC_FINAL DATA
              , NVL(LEAD(r.DT_HR_UTC_INICIAL) OVER (
                PARTITION BY
                  r.CD_CLIENTE, r.CD_TALHAO, r.CD_OPERACAO_CB
                ORDER BY
                  r.CD_CLIENTE, r.CD_TALHAO, r.CD_OPERACAO_CB, r.DT_HR_UTC_INICIAL
                )
              , r.DT_HR_UTC_FINAL - 1
              ) - r.DT_HR_UTC_FINAL DIFF
             FROM geo_layer_filtrada r
          ) t
          where t.DIFF = -1 or t.DIFF >= 30)
        , result as (
          select s.CD_CLIENTE
               , s.CD_TALHAO
               , s.CD_OPERACAO_CB CD_OPERACAO
               , s.DATA           INICIO
               , e.DATA           FIM
          from starts s
                 join ends e on (s.RN = e.RN)
        )
        select r.CD_CLIENTE
          , r.CD_TALHAO
          , r.CD_OPERACAO
          , r.INICIO
          , r.FIM
        from result r
        left join EVENTOS_OPERACAO_TALHAO eot on (
          r.CD_CLIENTE = eot.CD_CLIENTE
          AND r.CD_TALHAO = eot.CD_TALHAO
          AND r.CD_OPERACAO = eot.CD_OPERACAO
          AND r.INICIO = eot.DT_INICIO
          AND r.FIM = eot.DT_FIM
        ) where eot.CD_ID IS NULL
    );


    TYPE t_recs IS TABLE OF cr_cursor%ROWTYPE INDEX BY BINARY_INTEGER;
    v_recs t_recs;
    v_logger LOGGER;
    v_error_line NUMBER;
    v_proc_name VARCHAR2(128) := 'PKG_PROCESSAMENTO_ANALITICO.UPDATE_EVENTOS_TALHAO';
    v_event_id NUMBER;
  BEGIN
    v_logger := GET_LOGGER(v_proc_name, '{}', 'PKG_PROCESSAMENTO_ANALITICO', 'PACKAGE BODY');
    IF v_logger.FNC_CANRUN() = 1 THEN
      v_logger.FNC_BLOCKNEWINSTANCES();
      BEGIN
        OPEN cr_cursor;
        LOOP FETCH cr_cursor
          BULK COLLECT INTO v_recs
          LIMIT 10;
          EXIT WHEN v_recs.COUNT = 0;

          FOR I IN 1..v_recs.COUNT LOOP
            BEGIN
              MERGE INTO EVENTOS_OPERACAO_TALHAO eot
              USING DUAL ON (
                eot.CD_CLIENTE = v_recs(I).CD_CLIENTE
                AND eot.CD_TALHAO = v_recs(I).CD_TALHAO
                AND eot.CD_OPERACAO = v_recs(I).CD_OPERACAO
                AND (
                  v_recs(I).INICIO BETWEEN eot.DT_INICIO AND eot.DT_FIM
                  OR v_recs(I).FIM BETWEEN eot.DT_INICIO AND eot.DT_FIM
                  OR eot.DT_INICIO BETWEEN v_recs(I).INICIO AND v_recs(I).FIM
                  OR eot.DT_FIM BETWEEN v_recs(I).INICIO AND v_recs(I).FIM
                )
              )
              WHEN MATCHED THEN UPDATE SET
                eot.DT_INICIO = v_recs(I).INICIO, eot.DT_FIM = v_recs(I).FIM, eot.STATUS = 'CHANGED'
              WHEN NOT MATCHED THEN
                INSERT (CD_CLIENTE, CD_TALHAO, CD_OPERACAO, DT_INICIO, DT_FIM, STATUS)
                VALUES ( v_recs(I).CD_CLIENTE, v_recs(I).CD_TALHAO, v_recs(I).CD_OPERACAO, v_recs(I).INICIO, v_recs(I).FIM, 'CHANGED');

              SELECT CD_ID INTO v_event_id FROM EVENTOS_OPERACAO_TALHAO
                WHERE CD_CLIENTE  = v_recs(I).CD_CLIENTE
                  AND CD_TALHAO   =  v_recs(I).CD_TALHAO
                  AND CD_OPERACAO = v_recs(I).CD_OPERACAO
                  AND DT_INICIO   = v_recs(I).INICIO
                  AND DT_FIM      = v_recs(I).FIM;

              UPDATE GEO_LAYER_OPERACAO_TALHAO SET CD_EVENTO = v_event_id
                WHERE CD_ID IN (
                  SELECT t1.CD_ID FROM GEO_LAYER_OPERACAO_TALHAO t1
                    WHERE t1.CD_CLIENTE = v_recs(I).CD_CLIENTE
                    AND (t1.CD_TALHAO = v_recs(I).CD_TALHAO OR t1.CD_TALHAO IS NULL)
                    AND t1.CD_OPERACAO_CB = v_recs(I).CD_OPERACAO
                    AND t1.DT_HR_UTC_INICIAL >= v_recs(I).INICIO
                    AND t1.DT_HR_UTC_FINAL <= v_recs(I).FIM
                );
            EXCEPTION WHEN DUP_VAL_ON_INDEX THEN
              v_logger.WARNING('Tentativa de adicionar um registro duplicado na tabela de eventos');
            END;
          END LOOP;
          COMMIT;
        END LOOP;
        CLOSE cr_cursor;
        COMMIT;
      EXCEPTION WHEN OTHERS THEN
        v_logger.FATAL(SQLCODE, SQLERRM, 'FATAL ERROR');
      END;
      v_logger.FNC_UNBLOCKNEWINSTANCES();
    END IF;
    v_logger.ENDRUN();
    COMMIT;
  END UPDATE_EVENTOS_TALHAO;

  PROCEDURE PRC_DESCRIBE_ATTRIBUTES(p_map_id NUMBER)
  IS
    jo JSON_OBJECT_T;
    ja JSON_ARRAY_T;
    ja_values JSON_ARRAY_T;
    json CLOB;

    v_attribute_id NUMBER;

    v_name VARCHAR2(255);
    v_type VARCHAR2(255);
    v_min NUMBER;
    v_max NUMBER;
    v_value VARCHAR2(4000);
  BEGIN
    select JSON_VALUE(m.map_attributes, '$.json' RETURNING CLOB ERROR ON ERROR) INTO json
    from maps m
    where m.MAP_ID = p_map_id;
    jo := JSON_OBJECT_T.parse(json);
    jo := jo.get_object('tilestats');
    ja := jo.get_array('layers');
    jo := JSON_OBJECT_T(ja.get(0));
    ja := jo.get_array('attributes');


    FOR I IN 0.. ja.get_size - 1 LOOP
      jo := JSON_OBJECT_T(ja.get(I));

      v_name := jo.get_String('attribute');
      v_type := jo.get_String('type');
      v_min  := jo.get_Number('max');
      v_max  := jo.get_Number('min');

      INSERT INTO MAP_ATTRIBUTE (MAP_ID, NAME, TYPE, MAX, MIN)
      VALUES (p_map_id
        , v_name
        , v_type
        , v_min
        , v_max
      ) RETURNING CD_ID INTO v_attribute_id;

      ja_values := jo.get_Array('values');
      FOR X IN 0.. ja_values.get_size - 1 LOOP
        v_value := REPLACE(ja_values.get(X).to_String, '"', '');
        INSERT INTO MAP_ATTRIBUTE_UNIQ_VALUE(ATTRIBUTE_ID, VALUE) VALUES (v_attribute_id, v_value);
      END LOOP;
    END LOOP;
  END PRC_DESCRIBE_ATTRIBUTES;

  PROCEDURE PRC_BUILD_LAYERS(p_map_id NUMBER, p_event_id NUMBER) IS
    v_layer CLOB := EMPTY_CLOB();
  BEGIN
    FOR v_rec_layer IN (
      SELECT * FROM EVENTOS_OPERACAO_LAYERS
      WHERE MAP_ID = p_map_id
      AND EVENT_ID = p_event_id
    ) LOOP
      IF v_rec_layer.LAYER_TYPE = 1 THEN
        v_layer := FNC_VELOCIDADE_LAYER(p_map_id);
      ELSIF v_rec_layer.LAYER_TYPE = 2 THEN
        v_layer := FNC_VELOCIDADE_VENTO_LAYER(p_map_id);
      ELSIF v_rec_layer.LAYER_TYPE = 3 THEN
        v_layer := FNC_RPM_LAYER(p_map_id);
      ELSIF v_rec_layer.LAYER_TYPE = 4 THEN
        v_layer := FNC_ESTADO_LAYER(p_map_id);
      ELSIF v_rec_layer.LAYER_TYPE = 5 THEN
        v_layer := FNC_EQUIPAMENTO_LAYER(p_map_id);
      ELSIF v_rec_layer.LAYER_TYPE = 7 THEN
        v_layer := FNC_SOBREPOSICAO_LAYER();
      ELSIF v_rec_layer.LAYER_TYPE = 8 THEN
        v_layer := FNC_AREA_TRABALHADA_LAYER();
      ELSIF v_rec_layer.LAYER_TYPE = 9 THEN
        v_layer := FNC_AREA_FALHA_LAYER();
      END IF;
      UPDATE EVENTOS_OPERACAO_LAYERS SET JSON_LAYER = v_layer WHERE CD_ID = v_rec_layer.CD_ID;
    END LOOP;
    COMMIT;
  END PRC_BUILD_LAYERS;

  FUNCTION FNC_VELOCIDADE_LAYER(p_map_id NUMBER) RETURN CLOB IS
    v_layer_array JSON_ARRAY_T;
  BEGIN
    v_layer_array := JSON_ARRAY_T();
    v_layer_array.append(FNC_SYMBOL_LAYER('vlVelocidade'));
    v_layer_array.append(FNC_LINE_LAYER_NUMBER_PROP(p_map_id, 'vlVelocidade'));
    RETURN v_layer_array.to_String;
  END FNC_VELOCIDADE_LAYER;

  FUNCTION FNC_VELOCIDADE_VENTO_LAYER(p_map_id NUMBER) RETURN CLOB IS
    v_layer_array JSON_ARRAY_T;
  BEGIN
    v_layer_array := JSON_ARRAY_T();
    v_layer_array.append(FNC_SYMBOL_LAYER('vlVelocidadeVento'));
    v_layer_array.append(FNC_LINE_LAYER_NUMBER_PROP(p_map_id, 'vlVelocidadeVento'));
    RETURN v_layer_array.to_String;
  END FNC_VELOCIDADE_VENTO_LAYER;

  FUNCTION FNC_RPM_LAYER(p_map_id NUMBER) RETURN CLOB IS
    v_layer_array JSON_ARRAY_T;
  BEGIN
    v_layer_array := JSON_ARRAY_T();
    v_layer_array.append(FNC_SYMBOL_LAYER('vlRpm'));
    v_layer_array.append(FNC_LINE_LAYER_NUMBER_PROP(p_map_id, 'vlRpm'));
    RETURN v_layer_array.to_String;
  END FNC_RPM_LAYER;

  FUNCTION FNC_ESTADO_LAYER(p_map_id NUMBER) RETURN CLOB IS
    v_layer_array JSON_ARRAY_T;
  BEGIN
    v_layer_array := JSON_ARRAY_T();
    v_layer_array.append(FNC_SYMBOL_LAYER('cdEstado'));
    v_layer_array.append(FNC_LINE_LAYER_STATIC_PROP(p_map_id, 'cdEstado'));
    RETURN v_layer_array.to_String;
  END FNC_ESTADO_LAYER;

  FUNCTION FNC_EQUIPAMENTO_LAYER(p_map_id NUMBER) RETURN CLOB IS
    v_layer_array JSON_ARRAY_T;
  BEGIN
    v_layer_array := JSON_ARRAY_T();
    v_layer_array.append(FNC_SYMBOL_LAYER('cdEquipamento'));
    v_layer_array.append(FNC_LINE_LAYER_STATIC_PROP(p_map_id, 'cdEquipamento'));
    RETURN v_layer_array.to_String;
  END FNC_EQUIPAMENTO_LAYER;

  FUNCTION FNC_SOBREPOSICAO_LAYER RETURN CLOB IS
  BEGIN
    RETURN TO_CLOB(
      '{
          "id": "dadosSobreposicao",
          "type": "fill",
          "source": "sobreposicaoSource",
          "source-layer": "map",
          "paint": {
              "fill-opacity": 0.6,
              "fill-color": "#228b22"
          }
      }');
  END FNC_SOBREPOSICAO_LAYER;

  FUNCTION FNC_AREA_TRABALHADA_LAYER RETURN CLOB IS
  BEGIN
    RETURN TO_CLOB(
      '{
          "id": "dadosAreaTrabalhada",
          "type": "fill",
          "source": "areaTrabalhadaSource",
          "source-layer": "map",
          "paint": {
              "fill-opacity": 1,
              "fill-color": "#228b22"
          }
      }');
  END FNC_AREA_TRABALHADA_LAYER;

  FUNCTION FNC_AREA_FALHA_LAYER RETURN CLOB IS
  BEGIN
    RETURN TO_CLOB(
      '{
          "id": "dadosAreaFalha",
          "type": "fill",
          "source": "areaFalhaSource",
          "source-layer": "map",
          "paint": {
              "fill-opacity": 1,
              "fill-color": "#ff0000"
          }
      }');
  END FNC_AREA_FALHA_LAYER;

  FUNCTION FNC_SYMBOL_LAYER(p_prop_name VARCHAR2) RETURN JSON_OBJECT_T IS
    v_layer JSON_OBJECT_T;
  BEGIN
    v_layer := JSON_OBJECT_T.parse(
      '{
          "id": "dadosAnaliticosLabel",
          "type": "symbol",
          "source": "analyticMapSource",
          "source-layer": "map",
          "minzoom": 16,
          "paint": {
              "text-halo-color": "#FFF",
              "text-halo-width": 1,
              "text-halo-blur": 1
          },
          "layout": {
              "text-field": "{' || p_prop_name || '}",
              "text-font": ["Roboto Condensed Bold"],
              "symbol-placement": "line",
              "text-justify": "center",
              "text-anchor": "center",
              "text-size": 14
          }
      }');
    RETURN v_layer;
  END FNC_SYMBOL_LAYER;

  FUNCTION FNC_LINE_LAYER_NUMBER_PROP(p_map_id NUMBER, p_prop_name VARCHAR2) RETURN JSON_OBJECT_T IS
    v_layer JSON_OBJECT_T;
    v_step_size NUMBER;
    v_color_steps JSON_ARRAY_T;
    v_max NUMBER;
    v_value NUMBER;
  BEGIN
    SELECT ((max - min) / 10), min, max
      INTO v_step_size, v_value, v_max
    FROM MAP_ATTRIBUTE WHERE MAP_ID = p_map_id
      AND NAME = p_prop_name;

    IF v_step_size <= 0 THEN
      v_step_size := 1;
    END IF;

    v_color_steps := JSON_ARRAY_T();

    v_color_steps.append('step');
    v_color_steps.append(JSON_ELEMENT_T.parse('["get", "' || p_prop_name ||'"]'));
    v_color_steps.append(RANDOM_COLOR());
    v_value := v_value + v_step_size;

    WHILE v_value < v_max LOOP
      v_color_steps.append(v_value);
      v_color_steps.append(RANDOM_COLOR());
      v_value := v_value + v_step_size;
    END LOOP;

    v_layer := JSON_OBJECT_T.parse(
      '{
          "id": "dadosAnaliticos",
          "type": "line",
          "source": "analyticMapSource",
          "source-layer": "map",
          "layout": {
              "line-cap": "round",
              "line-join": "round"
          },
          "paint": {
              "line-opacity": 1,
              "line-color": ' || v_color_steps.to_String || ',
              "line-width": [
                  "interpolate"
                  , ["linear"], ["zoom"]
                  , 12, 1
                  , 16, 5
                  , 22, 30
              ]
          }
      }');

    RETURN v_layer;
  END FNC_LINE_LAYER_NUMBER_PROP;

  FUNCTION FNC_LINE_LAYER_STATIC_PROP(p_map_id NUMBER, p_prop_name VARCHAR2) RETURN JSON_OBJECT_T IS
    v_layer JSON_OBJECT_T;
    v_attr_type VARCHAR2(255);
    v_color_steps JSON_ARRAY_T;
  BEGIN

    v_color_steps := JSON_ARRAY_T();
    v_color_steps.append('match');
    v_color_steps.append(JSON_ELEMENT_T.parse('["get", "' || p_prop_name ||'"]'));

    FOR v_rec IN (
      SELECT
        mauv.VALUE v_value,
        ma.TYPE v_type
      FROM MAP_ATTRIBUTE ma
        JOIN MAP_ATTRIBUTE_UNIQ_VALUE mauv on ma.CD_ID = mauv.ATTRIBUTE_ID
      WHERE ma.MAP_ID = p_map_id
        AND ma.NAME = p_prop_name
      ORDER BY 1 ASC
    ) LOOP
      IF v_rec.v_type = 'number' THEN
        v_color_steps.append(TO_NUMBER(v_rec.v_value));
      ELSE
        v_color_steps.append(v_rec.v_value);
      END IF;

      v_color_steps.append(RANDOM_COLOR());
    END LOOP;
    --Gray for unknow values
    v_color_steps.append('#CCC');

    v_layer := JSON_OBJECT_T.parse(
      '{
          "id": "dadosAnaliticos",
          "type": "line",
          "source": "analyticMapSource",
          "source-layer": "map",
          "layout": {
              "line-cap": "round",
              "line-join": "round"
          },
          "paint": {
              "line-opacity": 1,
              "line-color": ' || v_color_steps.to_String || ',
              "line-width": [
                  "interpolate"
                  , ["linear"], ["zoom"]
                  , 12, 1
                  , 16, 5
                  , 22, 30
              ]
          }
      }');

    RETURN v_layer;
  END FNC_LINE_LAYER_STATIC_PROP;

  FUNCTION FNC_GEOJSON_EVENTO(p_cd_cliente NUMBER, p_cd_evento NUMBER) RETURN TABLE_MAP_GEOJSON_T PIPELINED IS
    CURSOR cr_cursor IS (
      SELECT * FROM (
        WITH dataset as (
          SELECT
          glt.CD_UNIDADE
          , glt.CD_FAZENDA
          , glt.CD_ZONA
          , glt.CD_TALHAO
          , glot.DT_HR_UTC_INICIAL
          , glot.DT_HR_UTC_FINAL
          , glot.CD_OPERACAO
          , glot.CD_OPERAC_PARADA
          , glot.FG_TP_EQUIPAMENTO
          , glot.CD_EQUIPAMENTO
          , glot.CD_IMPLEMENTO
          , glot.CD_EQUIPE
          , glot.CD_OPERADOR
          , glot.CD_ESTADO
          , glot.DESC_OPERACAO
          , glot.DESC_EQUIPAMENTO
          , glot.DESC_OPERADOR
          , glot.DESC_TIPO_BICO
          , glot.DESC_OPERAC_PARADA
          , glot.FG_FRENTE_TRABALHO
          , glot.CD_TIPO_BICO
          , glot.VL_TEMPO_SEGUNDOS
          , glot.VL_AREA
          , glot.VL_DISTANCIA_METROS
          , glot.VL_VELOCIDADE
          , glot.VL_VELOCIDADE_VENTO
          , glot.VL_TEMPERATURA
          , glot.VL_UMIDADE
          , glot.VL_CONSUMO_INSTANTANEO
          , glot.VL_RPM
          , glot.VL_TEMPERATURA_MOTOR
          , glot.VL_ORDEM_SERVICO
          , glot.VL_ALARME
          , glot.VL_ALERTA_CLIMA
          , glot.VL_ALARME_CLIMA
          , glot.VL_HECTARES_HORA
          , glot.VL_HORIMETRO_INICIAL
          , glot.VL_HORIMETRO_FINAL
          , glot.VL_LARGURA_IMPLEMENTO
          , glot.VL_RENDIMENTO_COLHEITA
          , glot.VL_UMIDADE_GRAOS
          , glot.VL_PONTO_ORVALHO
          , glot.VL_VAZAO_LITROS_HA
          , glot.VL_VAZAO_LITROS_MIN
          , glot.VL_DOSAGEM_TAXA1
          , glot.VL_DOSAGEM_TAXA2
          , glot.VL_DOSAGEM_TAXA3
          , glot.VL_PRESSAO_BOMBA
          , glot.LINHA
          , glot.GEOMETRIA
          FROM GEO_LAYER_OPERACAO_TALHAO glot
          JOIN EVENTOS_OPERACAO_TALHAO eot ON glot.CD_EVENTO = eot.CD_ID AND glot.CD_CLIENTE = eot.CD_CLIENTE
          LEFT JOIN GEO_LAYER_TALHAO glt ON glot.CD_TALHAO = glt.CD_ID
          WHERE
            eot.CD_CLIENTE = p_cd_cliente
            AND eot.CD_ID = p_cd_evento
            AND (glot.CD_TALHAO = eot.CD_TALHAO OR glot.CD_TALHAO IS NULL)
            AND glot.CD_OPERACAO_CB = eot.CD_OPERACAO
            AND (eot.DT_INICIO BETWEEN glot.DT_HR_UTC_INICIAL AND glot.DT_HR_UTC_FINAL OR
                 eot.DT_FIM BETWEEN glot.DT_HR_UTC_INICIAL AND glot.DT_HR_UTC_FINAL OR
                 (glot.DT_HR_UTC_INICIAL BETWEEN eot.DT_INICIO AND eot.DT_FIM AND
                  glot.DT_HR_UTC_FINAL BETWEEN eot.DT_INICIO AND eot.DT_FIM)
              )
            AND glot.LINHA IS NOT NULL AND glot.GEOMETRIA IS NOT NULL
        )
        , geojson_linhas as (
          SELECT
          'LINHAS' TYPE
          , JSON_OBJECT(
            'type' VALUE 'FeatureCollection'
          , 'features' VALUE JSON_ARRAYAGG(
            JSON_OBJECT('type'  VALUE 'Feature'
              , 'properties'    VALUE JSON_OBJECT(
                'cdUnidade'               VALUE CD_UNIDADE
                , 'cdFazenda'             VALUE CD_FAZENDA
                , 'cdZona'                VALUE CD_ZONA
                , 'cdTalhao'              VALUE CD_TALHAO
                , 'dtHrUtcInicial'        VALUE TO_EPOCH(DT_HR_UTC_INICIAL)
                , 'dtHrUtcFinal'          VALUE TO_EPOCH(DT_HR_UTC_FINAL)
                , 'cdOperacao'            VALUE CD_OPERACAO
                , 'cdOperacParada'        VALUE CD_OPERAC_PARADA
                , 'fgTpEquipamento'       VALUE FG_TP_EQUIPAMENTO
                , 'cdEquipamento'         VALUE CD_EQUIPAMENTO
                , 'cdImplemento'          VALUE CD_IMPLEMENTO
                , 'cdEquipe'              VALUE CD_EQUIPE
                , 'cdOperador'            VALUE CD_OPERADOR
                , 'cdEstado'              VALUE CD_ESTADO
                , 'descOperacao'          VALUE DESC_OPERACAO
                , 'descEquipamento'       VALUE DESC_EQUIPAMENTO
                , 'descOperador'          VALUE DESC_OPERADOR
                , 'descTipoBico'          VALUE DESC_TIPO_BICO
                , 'descOperacParada'      VALUE DESC_OPERAC_PARADA
                , 'fgFrenteTrabalho'      VALUE FG_FRENTE_TRABALHO
                , 'cdTipoBico'            VALUE CD_TIPO_BICO
                , 'vlTempoSegundos'       VALUE VL_TEMPO_SEGUNDOS
                , 'vlArea'                VALUE VL_AREA
                , 'vlDistanciaMetros'     VALUE VL_DISTANCIA_METROS
                , 'vlVelocidade'          VALUE VL_VELOCIDADE
                , 'vlVelocidadeVento'     VALUE VL_VELOCIDADE_VENTO
                , 'vlTemperatura'         VALUE VL_TEMPERATURA
                , 'vlUmidade'             VALUE VL_UMIDADE
                , 'vlConsumoInstantaneo'  VALUE VL_CONSUMO_INSTANTANEO
                , 'vlRpm'                 VALUE VL_RPM
                , 'vlTemperaturaMotor'    VALUE VL_TEMPERATURA_MOTOR
                , 'vlOrdemServico'        VALUE VL_ORDEM_SERVICO
                , 'vlAlarme'              VALUE VL_ALARME
                , 'vlAlertaClima'         VALUE VL_ALERTA_CLIMA
                , 'vlAlarmeClima'         VALUE VL_ALARME_CLIMA
                , 'vlHectaresHora'        VALUE VL_HECTARES_HORA
                , 'vlHorimetroInicial'    VALUE VL_HORIMETRO_INICIAL
                , 'vlHorimetroFinal'      VALUE VL_HORIMETRO_FINAL
                , 'vlLarguraImplemento'   VALUE VL_LARGURA_IMPLEMENTO
                , 'vlRendimentoColheita'  VALUE VL_RENDIMENTO_COLHEITA
                , 'vlUmidadeGraos'        VALUE VL_UMIDADE_GRAOS
                , 'vlPontoOrvalho'        VALUE VL_PONTO_ORVALHO
                , 'vlVazaoLitrosHa'       VALUE VL_VAZAO_LITROS_HA
                , 'vlVazaoLitrosMin'      VALUE VL_VAZAO_LITROS_MIN
                , 'vlDosagemTaxa1'        VALUE VL_DOSAGEM_TAXA1
                , 'vlDosagemTaxa2'        VALUE VL_DOSAGEM_TAXA2
                , 'vlDosagemTaxa3'        VALUE VL_DOSAGEM_TAXA3
                , 'vlPressaoBomba'        VALUE VL_PRESSAO_BOMBA
                ABSENT ON NULL )
                , 'geometry' VALUE MDSYS.SDO_UTIL.TO_GEOJSON(LINHA) FORMAT JSON RETURNING BLOB
              ) RETURNING BLOB
            ) RETURNING BLOB
          ) GEOJSON
          FROM dataset
        )
        , geojson_sobreposicao as (
          SELECT
          'SOBREPOSICAO' TYPE
          , JSON_OBJECT('type' VALUE 'FeatureCollection'
          , 'features' VALUE JSON_ARRAYAGG(
            JSON_OBJECT('type'  VALUE 'Feature'
              , 'properties'    VALUE JSON_OBJECT(
                'cdUnidade'         VALUE CD_UNIDADE
                , 'cdFazenda'       VALUE CD_FAZENDA
                , 'cdZona'          VALUE CD_ZONA
                , 'cdTalhao'        VALUE CD_TALHAO
                , 'dtHrUtcInicial'  VALUE TO_EPOCH(DT_HR_UTC_INICIAL)
                , 'dtHrUtcFinal'    VALUE TO_EPOCH(DT_HR_UTC_FINAL)
                ABSENT ON NULL )
                , 'geometry' VALUE MDSYS.SDO_UTIL.TO_GEOJSON(GEOMETRIA) FORMAT JSON RETURNING BLOB
              ) RETURNING BLOB
            ) RETURNING BLOB
          ) GEOJSON
          FROM dataset
        )
        , union_geometrias AS (
          SELECT sdo_aggr_union(sdoaggrtype(aggr_geom, 0.5)) aggr_geom
          FROM (SELECT sdo_aggr_union(sdoaggrtype(aggr_geom, 0.5)) aggr_geom
            FROM (SELECT sdo_aggr_union(sdoaggrtype(aggr_geom, 0.5)) aggr_geom
              FROM (SELECT sdo_aggr_union(sdoaggrtype(aggr_geom, 0.5)) aggr_geom
                FROM (SELECT sdo_aggr_union(sdoaggrtype(aggr_geom, 0.5)) aggr_geom
                  FROM (SELECT sdo_aggr_union(sdoaggrtype(aggr_geom, 0.5)) aggr_geom
                    FROM (SELECT sdo_aggr_union(mdsys.sdoaggrtype(z.GEOMETRIA, 0.5)) aggr_geom
                      FROM dataset z
                    GROUP BY mod(rownum, 2024))
                  GROUP BY mod(rownum, 512))
                GROUP BY mod(rownum, 128))
              GROUP BY mod(rownum, 32))
            GROUP BY mod(rownum, 8))
          GROUP BY mod(rownum, 2))
        )
        , geojson_area_trabalhada AS (
          SELECT
          'AREA_TRABALHADA' TYPE
          , JSON_OBJECT('type' VALUE 'FeatureCollection'
          , 'features' VALUE JSON_ARRAYAGG(
            JSON_OBJECT('type'  VALUE 'Feature'
              , 'properties'    VALUE JSON_OBJECT(
                'cdUnidade'       VALUE t.CD_UNIDADE
                , 'cdFazenda'     VALUE t.CD_FAZENDA
                , 'cdZona'        VALUE t.CD_ZONA
                , 'cdTalhao'      VALUE t.CD_TALHAO
                ABSENT ON NULL )
                , 'geometry' VALUE MDSYS.SDO_UTIL.TO_GEOJSON((
                      SELECT MDSYS.SDO_GEOM.SDO_BUFFER(z.aggr_geom, 2, 2.5, 'unit=M') FROM union_geometrias z
                    ))
                  FORMAT JSON RETURNING BLOB
              ) RETURNING BLOB
            ) RETURNING BLOB
          ) GEOJSON
          FROM (
            SELECT
               dataset.CD_UNIDADE
              , dataset.CD_FAZENDA
              , dataset.CD_ZONA
              , dataset.CD_TALHAO
            FROM dataset where CD_TALHAO IS NOT NULL AND ROWNUM = 1
          ) t
        )
        , geojson_area_falha AS (
         SELECT
          'AREA_FALHA' TYPE
          , JSON_OBJECT('type' VALUE 'FeatureCollection'
          , 'features' VALUE JSON_ARRAYAGG(
            JSON_OBJECT('type'  VALUE 'Feature'
              , 'properties'    VALUE JSON_OBJECT(
                'cdUnidade'       VALUE t.CD_UNIDADE
                , 'cdFazenda'     VALUE t.CD_FAZENDA
                , 'cdZona'        VALUE t.CD_ZONA
                , 'cdTalhao'      VALUE t.CD_TALHAO
                ABSENT ON NULL )
                , 'geometry' VALUE MDSYS.SDO_UTIL.TO_GEOJSON((
                      SELECT
                        MDSYS.SDO_GEOM.SDO_DIFFERENCE(
                          glt.GEOMETRIA
                          , z.aggr_geom
                          , 0.05
                        )
                      FROM union_geometrias z
                    ))
                  FORMAT JSON RETURNING BLOB
              ) RETURNING BLOB
            ) RETURNING BLOB
          ) GEOJSON
          FROM (
            SELECT
               dataset.CD_UNIDADE
              , dataset.CD_FAZENDA
              , dataset.CD_ZONA
              , dataset.CD_TALHAO
            FROM dataset where CD_TALHAO IS NOT NULL AND ROWNUM = 1
          ) t
          JOIN GEO_LAYER_TALHAO glt ON (
            glt.CD_CLIENTE = p_cd_cliente
            AND glt.CD_UNIDADE = t.CD_UNIDADE
            AND glt.CD_FAZENDA = t.CD_FAZENDA
            AND glt.CD_ZONA = t.CD_ZONA
            AND glt.CD_TALHAO = t.CD_TALHAO
          )
        )
        SELECT * FROM geojson_linhas
        UNION ALL
        SELECT * FROM geojson_sobreposicao
        UNION ALL
        SELECT * FROM geojson_area_trabalhada
        UNION ALL
        SELECT * FROM geojson_area_falha
      )
    );

    v_rec cr_cursor%ROWTYPE;
    v_obj MAP_GEOJSON_T;
  BEGIN
    OPEN cr_cursor;
    LOOP FETCH cr_cursor INTO v_rec;
      EXIT WHEN cr_cursor%NOTFOUND;
      v_obj := MAP_GEOJSON_T(v_rec.TYPE, v_rec.GEOJSON);
      PIPE ROW (v_obj);
    END LOOP;
    CLOSE cr_cursor;
    RETURN;
  END;
END PKG_PROCESSAMENTO_ANALITICO;

BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name => 'UPDATE_EVENTOS_TALHAO_JOB'
    , job_type           =>  'STORED_PROCEDURE'
    , job_action         =>  'PKG_PROCESSAMENTO_ANALITICO.UPDATE_EVENTOS_TALHAO'
    , start_date         =>  SYSDATE
    , repeat_interval    =>  'FREQ=MINUTELY; INTERVAL=5;'
    , enabled            =>  TRUE
  );
END;

-- PROCEDURE CREATE_BATCHES IS
--   v_logger LOGGER;
--   v_error_line NUMBER;
--   v_job_name VARCHAR2(128) := 'PKG_PROCESSAMENTO_ANALITICO_CREATE_BATCHES_JOB';
--   v_proc_name VARCHAR2(255) := 'PKG_PROCESSAMENTO_ANALITICO.CREATE_BATCHES';
--   v_proc_instance NUMBER := 1;
--   v_batch_id NUMBER;
--   v_job NUMBER;
--   has_any NUMBER := 0;
-- BEGIN
--   v_logger := GET_LOGGER(v_proc_name, '{}', 'PKG_PROCESSAMENTO_ANALITICO', 'PACKAGE BODY');
--   --Verifica se a procedure esta bloqueada para execucao
--   IF v_logger.FNC_CANRUN() = 1 THEN
--     --Se nao estiver, bloqueia e comeca a criar batches
--     v_logger.FNC_BLOCKNEWINSTANCES();
--     BEGIN
--       --Para cada cliente que possui registros nao processados na TMP_MAPA_VAR, cria um novo batch e dispara um processamento concorrente
--       FOR v_rec IN (SELECT CD_CLIENTE FROM TMP_MAPA_VAR WHERE BATCH_ID IS NULL GROUP BY CD_CLIENTE) LOOP
--         has_any := 1;
--         INSERT INTO TMP_MAPA_VAR_BATCH (CD_CLIENTE) VALUES (v_rec.CD_CLIENTE) RETURNING CD_ID INTO v_batch_id;
--         UPDATE TMP_MAPA_VAR SET BATCH_ID = v_batch_id WHERE CD_CLIENTE = v_rec.CD_CLIENTE AND BATCH_ID IS NULL;
--         COMMIT;
--
--         DBMS_JOB.SUBMIT(
--           job => v_job
--           , what => 'BEGIN PKG_PROCESSAMENTO_ANALITICO.DO_BATCH(' || v_rec.CD_CLIENTE || ',' || v_batch_id || '); END;'
--           , instance => v_proc_instance
--         );
--       END LOOP;
--       --Se nao houver mais nenhum registro para processar, desabilita o job ate que sejam inseridos novos registros
--       IF has_any = 0 THEN
--         DBMS_SCHEDULER.DISABLE(v_job_name, TRUE);
--       END IF;
--     EXCEPTION WHEN OTHERS THEN
--       v_logger.FATAL(SQLCODE, SQLERRM, 'FATAL ERROR');
--     END;
--     v_logger.FNC_UNBLOCKNEWINSTANCES();
--   ELSE
--     v_logger.WARNING('cannot run due to concurrence block');
--   END IF;
--   v_logger.ENDRUN();
--   COMMIT;
-- END CREATE_BATCHES;
-- BEGIN
--   DBMS_SCHEDULER.CREATE_JOB(
--     job_name           =>  'PKG_PROCESSAMENTO_ANALITICO_CREATE_BATCHES_JOB',
--     job_type           =>  'STORED_PROCEDURE',
--     job_action         =>  'PKG_PROCESSAMENTO_ANALITICO.CREATE_BATCHES',
--     start_date         =>  SYSDATE,
--     repeat_interval    =>  'FREQ=SECONDLY; INTERVAL=5;',
--     enabled            =>  FALSE
--   );
-- END;
--
--Registra um callback, caso ainda nao exista, para mudancas na tabela TMP_MAPA_VAR
DECLARE
  reginfo  CQ_NOTIFICATION$_REG_INFO;
  v_cursor SYS_REFCURSOR;
  regid    NUMBER;
BEGIN

  reginfo := cq_notification$_reg_info(
        'PKG_PROCESSAMENTO_ANALITICO.ON_BATCH_UPDATE',
        DBMS_CQ_NOTIFICATION.QOS_QUERY + DBMS_CQ_NOTIFICATION.QOS_ROWIDS,
        0,
        DBMS_CQ_NOTIFICATION.UPDATEOP, -- operations_filter
        0
    );
    regid := DBMS_CQ_NOTIFICATION.NEW_REG_START(reginfo);
    OPEN v_cursor FOR
    SELECT STATUS FROM TMP_MAPA_VAR_BATCH;
    CLOSE v_cursor;
    DBMS_CQ_NOTIFICATION.REG_END();
END;