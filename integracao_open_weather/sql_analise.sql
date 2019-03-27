-----------------------------------------------------------------------------------
------------------------------------SOLUCAO 1--------------------------------------
-----------------------------------------------------------------------------------
with operacoes_periodo as (
    SELECT
      ddo.CD_UNIDADE,
      ddo.CD_FAZENDA,
      ddo.CD_ZONA,
      ddo.CD_TALHAO,
      ddo.CD_OPERACAO,
      ddo.DESC_OPERACAO,
      TRUNC(ddo.DT_HR_INI_REGNAJORNADA) AS DATA
    FROM DDN_DETALHES_OPERACAO ddo
      JOIN CDT_OPERACAO co ON (ddo.CD_OPERACAO = co.CD_OPERACAO)
    WHERE
      ddo.CD_UNIDADE = :cdUnidade
      AND ddo.CD_FAZENDA = :cdFazenda
      AND ddo.CD_ZONA = :cdZona
      AND ddo.CD_TALHAO = :cdTalhao
      AND co.CD_GRUPO_ATIVIDADE <> 999
      AND co.CD_GRUPO_ATIVIDADE IS NOT NULL
      AND ddo.CD_ESTADO = 'E'
    --   AND ddo.DT_HR_INI_REGNAJORNADA BETWEEN TO_DATE(:dataIni, 'RRRR-MM-DD HH24:MI:SS') AND TO_DATE(:dataFim, 'RRRR-MM-DD HH24:MI:SS')
    GROUP BY
      ddo.CD_UNIDADE
      , ddo.CD_FAZENDA
      , ddo.CD_ZONA
      , ddo.CD_TALHAO
      , ddo.CD_OPERACAO
      , ddo.DESC_OPERACAO
      , TRUNC(ddo.DT_HR_INI_REGNAJORNADA)
    ORDER BY 5, 7)
  , diffed as (
    SELECT
      op.CD_UNIDADE,
      op.CD_FAZENDA,
      op.CD_ZONA,
      op.CD_TALHAO,
      op.CD_OPERACAO,
      op.DESC_OPERACAO,
      op.DATA,
      OP.DATA - NVL(LAG(op.DATA)
                    OVER (
                      PARTITION BY OP.CD_OPERACAO
                      ORDER BY OP.CD_OPERACAO, OP.DATA ), OP.DATA) DIFF_LAG,
      NVL(LEAD(op.DATA)
          OVER (
            PARTITION BY OP.CD_OPERACAO
            ORDER BY OP.CD_OPERACAO, OP.DATA ), OP.DATA) - OP.DATA DIFF_LEAD
    FROM operacoes_periodo op)
  , ungroupped as (
    SELECT
      d.CD_UNIDADE,
      d.CD_FAZENDA,
      d.CD_ZONA,
      d.CD_TALHAO,
      d.CD_OPERACAO,
      d.DESC_OPERACAO,
      CASE
      WHEN d.DIFF_LAG = 0 OR d.DIFF_LAG > :maxDelay
        THEN d.DATA
      END AS INICIO,
      CASE
      WHEN d.DIFF_LEAD = 0 OR d.DIFF_LEAD > :maxDelay
        THEN d.DATA
      END AS FIM
    FROM diffed d)
  , ungroupped_filtered as (
    SELECT
      u.CD_UNIDADE,
      u.CD_FAZENDA,
      u.CD_ZONA,
      u.CD_TALHAO,
      u.CD_OPERACAO,
      u.DESC_OPERACAO,
      u.INICIO,
      u.FIM
    FROM ungroupped u
    WHERE u.INICIO IS NOT NULL OR u.FIM IS NOT NULL)
  , starts as (
    SELECT
      u.CD_UNIDADE,
      u.CD_FAZENDA,
      u.CD_ZONA,
      u.CD_TALHAO,
      u.CD_OPERACAO,
      u.DESC_OPERACAO,
      u.INICIO,
      ROW_NUMBER()
      OVER (
        ORDER BY CD_OPERACAO, DESC_OPERACAO, INICIO ) RN
    FROM ungroupped_filtered u
    WHERE INICIO IS NOT NULL
)
  , ends as (
    SELECT
      u.CD_UNIDADE,
      u.CD_FAZENDA,
      u.CD_ZONA,
      u.CD_TALHAO,
      u.CD_OPERACAO,
      u.DESC_OPERACAO,
      u.FIM,
      ROW_NUMBER()
      OVER (
        ORDER BY CD_OPERACAO, DESC_OPERACAO, FIM ) RN
    FROM ungroupped_filtered u
    WHERE FIM IS NOT NULL
)
SELECT
  s.CD_UNIDADE,
  s.CD_FAZENDA,
  s.CD_ZONA,
  s.CD_TALHAO,
  s.CD_OPERACAO,
  s.DESC_OPERACAO,
  s.INICIO,
  e.FIM
FROM starts s
  JOIN ends e on (s.RN = e.RN);
-----------------------------------------------------------------------------------
------------------------------------SOLUCAO 2--------------------------------------
-----------------------------------------------------------------------------------
/*
Soluçao mais adequada que:
1. filtra as operaçoes efetivas de um talhao
2. descobre a data de inicio e fim de cada operacao
*/
with operacoes_periodo as (
    SELECT
      ddo.CD_UNIDADE,
      ddo.CD_FAZENDA,
      ddo.CD_ZONA,
      ddo.CD_TALHAO,
      ddo.CD_OPERACAO,
      ddo.DESC_OPERACAO,
      REPLACE(co.VL_GRAFICO, '0x', '#') COR,
      TRUNC(ddo.DT_HR_INI_REGNAJORNADA) AS DATA
    FROM DDN_DETALHES_OPERACAO ddo
      JOIN CDT_OPERACAO co ON (ddo.CD_OPERACAO = co.CD_OPERACAO)
    WHERE
      ddo.CD_UNIDADE = :cdUnidade
      AND ddo.CD_FAZENDA = :cdFazenda
      AND ddo.CD_ZONA = :cdZona
      AND ddo.CD_TALHAO = :cdTalhao
      AND co.CD_GRUPO_ATIVIDADE <> 999
      AND co.CD_GRUPO_ATIVIDADE IS NOT NULL
      AND ddo.CD_ESTADO = 'E'
    GROUP BY
      ddo.CD_UNIDADE
      , ddo.CD_FAZENDA
      , ddo.CD_ZONA
      , ddo.CD_TALHAO
      , ddo.CD_OPERACAO
      , ddo.DESC_OPERACAO
      , REPLACE(co.VL_GRAFICO, '0x', '#')
      , TRUNC(ddo.DT_HR_INI_REGNAJORNADA)
    ORDER BY 5, 7)
  , parametros as (
    SELECT CAST(VL_PARAMETRO AS NUMBER) MAX_DELAY FROM CFG_PARAMETROS_GERAIS WHERE CD_ID = 367
  )
  , starts as (
    SELECT
      t.CD_UNIDADE,
      t.CD_FAZENDA,
      t.CD_ZONA,
      t.CD_TALHAO,
      t.CD_OPERACAO,
      t.DESC_OPERACAO,
      t.COR,
      t.DATA,
      ROW_NUMBER()
      OVER (
        ORDER BY CD_OPERACAO, DESC_OPERACAO, DATA ) RN
    FROM (
           SELECT
             op.CD_UNIDADE,
             op.CD_FAZENDA,
             op.CD_ZONA,
             op.CD_TALHAO,
             op.CD_OPERACAO,
             op.DESC_OPERACAO,
             op.COR,
             op.DATA,
             OP.DATA - NVL(LAG(op.DATA)
                           OVER (
                             PARTITION BY OP.CD_OPERACAO
                             ORDER BY OP.CD_OPERACAO, OP.DATA ), OP.DATA) DIFF
           FROM operacoes_periodo op
         ) t
    WHERE t.DIFF = 0 OR t.DIFF > (SELECT MAX_DELAY FROM parametros)
)
  , ends as (
    SELECT
      t.CD_UNIDADE,
      t.CD_FAZENDA,
      t.CD_ZONA,
      t.CD_TALHAO,
      t.CD_OPERACAO,
      t.DESC_OPERACAO,
      t.COR,
      t.DATA,
      ROW_NUMBER()
      OVER (
        ORDER BY CD_OPERACAO, DESC_OPERACAO, DATA ) RN
    FROM (
           SELECT
             op.CD_UNIDADE,
             op.CD_FAZENDA,
             op.CD_ZONA,
             op.CD_TALHAO,
             op.CD_OPERACAO,
             op.DESC_OPERACAO,
             op.COR,
             op.DATA,
             NVL(LEAD(op.DATA)
                 OVER (
                   PARTITION BY OP.CD_OPERACAO
                   ORDER BY OP.CD_OPERACAO, OP.DATA ), OP.DATA) - OP.DATA DIFF
           FROM operacoes_periodo op
         ) t
    WHERE t.DIFF = 0 OR t.DIFF > (SELECT MAX_DELAY FROM parametros)
)
SELECT
  s.CD_UNIDADE cdUnidade,
  s.CD_FAZENDA cdFazenda,
  s.CD_ZONA cdZona,
  s.CD_TALHAO cdTalhao,
  s.CD_OPERACAO cdOperacao,
  s.DESC_OPERACAO descOperacao,
  s.COR cor,
  TO_EPOCH(s.DATA) inicio,
  TO_EPOCH(e.DATA) fim
FROM starts s
  JOIN ends e on (s.RN = e.RN);

-----------------------------------------------------------------------------------
/*
Procura imagens de Ndvi
*/
  SELECT
dnth.CD_UNIDADE cdUnidade
, dnth.CD_FAZENDA cdFazenda
, dnth.CD_TALHAO cdTalhao
, TO_CHAR(dnth.DT_ANALISE, 'RRRR-MM-DD') dtAnalise
, dnth.FONTE fonte
, dnth.ZOOM zoom
, dnth.AREA_DISPONIVEL areaDisponivel
, dnth.PERCENTUAL_NUVENS percentualNuvens
FROM DDN_NDVI_TALHAO_HISTORICO dnth
JOIN CDT_NDVI_TALHAO_POLIGONO cntp ON (
dnth.CD_UNIDADE = cntp.CD_UNIDADE
AND dnth.CD_FAZENDA = cntp.CD_FAZENDA
AND dnth.CD_ZONA = cntp.CD_ZONA
AND dnth.CD_TALHAO = cntp.CD_TALHAO
)
WHERE cntp.POLIGONO = :poligono
AND dnth.FONTE = NVL(:fonte, dnth.FONTE)
AND dnth.DT_ANALISE BETWEEN TO_DATE(:dataIni, 'RRRR-MM-DD HH24-MI-SS') AND TO_DATE(:dataFim, 'RRRR-MM-DD HH24-MI-SS')
AND dnth.PERCENTUAL_NUVENS BETWEEN :nuvensIni AND :nuvensFim
AND dnth.AREA_DISPONIVEL >= 90;

/*
Cruza dados de preciptacao da OpenWeather com dados
de chuva dos pluviometros e estaçoes Solinftec
*/
with localizado as (
    SELECT
      fnc_localizacao_v1(cpp.VL_LATITUDE, cpp.VL_LONGITUDE).CD_FAZENDA CD_FAZENDA,
      fnc_localizacao_v1(cpp.VL_LATITUDE, cpp.VL_LONGITUDE).CD_ZONA    CD_ZONA,
      fnc_localizacao_v1(cpp.VL_LATITUDE, cpp.VL_LONGITUDE).CD_TALHAO  CD_TALHAO,
      cpp.CD_EQUIPAMENTO
    FROM CDT_PARAMETROS_PLUVIOMETRO cpp
),
    filtrado as (
      SELECT *
      FROM localizado
      WHERE CD_FAZENDA = :cdFazenda
            AND CD_ZONA = :cdZona
            AND CD_TALHAO = :cdTalhao
  ),
    cruzado as (
      SELECT
        filtrado.CD_FAZENDA,
        filtrado.CD_ZONA,
        filtrado.CD_TALHAO,
        dm.CD_EQUIPAMENTO,
        TRUNC(dm.DT_HR_LOCAL) DT_HR_LOCAL,
        dm.VL_CHUVA
      FROM DDN_METEOROLOGIA dm
        JOIN filtrado ON filtrado.CD_EQUIPAMENTO = dm.CD_EQUIPAMENTO
                         AND DT_HR_LOCAL BETWEEN TO_DATE(:dataIni, 'RRRR-MM-DD HH24:MI:SS') AND TO_DATE(:dataFim,
                                                                                                        'RRRR-MM-DD HH24:MI:SS')
  ),
    agrupado_equipamento_dia as (
      SELECT
        cruzado.CD_FAZENDA    CD_FAZENDA,
        cruzado.CD_ZONA       CD_ZONA,
        cruzado.CD_TALHAO     CD_TALHAO,
        cruzado.DT_HR_LOCAL,
        cruzado.CD_EQUIPAMENTO,
        SUM(cruzado.VL_CHUVA) chuva_somada
      FROM cruzado
      GROUP BY
        cruzado.CD_FAZENDA
        , cruzado.CD_ZONA
        , cruzado.CD_TALHAO
        , cruzado.DT_HR_LOCAL
        , cruzado.CD_EQUIPAMENTO
  ),
    agrupado_dia as (
      SELECT
        agrupado_equipamento_dia.CD_FAZENDA,
        agrupado_equipamento_dia.CD_ZONA,
        agrupado_equipamento_dia.CD_TALHAO,
        agrupado_equipamento_dia.DT_HR_LOCAL,
        AVG(agrupado_equipamento_dia.chuva_somada) CHUVA_PLUVIOMETRO
      FROM agrupado_equipamento_dia
      GROUP BY
        agrupado_equipamento_dia.CD_FAZENDA
        , agrupado_equipamento_dia.CD_ZONA
        , agrupado_equipamento_dia.CD_TALHAO
        , agrupado_equipamento_dia.DT_HR_LOCAL
  )
  , cruzado_satelite as (
    SELECT
      ad.CD_FAZENDA,
      ad.CD_ZONA,
      ad.CD_TALHAO,
      ad.DT_HR_LOCAL,
      ad.CHUVA_PLUVIOMETRO,
      dpth.CHUVA_ACUMULADA
    FROM agrupado_dia ad
      LEFT JOIN DDN_PRECIPITACAO_TALHAO_HIST dpth
        ON dpth.CD_UNIDADE = '7'
           AND ad.CD_FAZENDA = dpth.CD_FAZENDA
           AND ad.CD_ZONA = dpth.CD_ZONA
           AND ad.CD_TALHAO = dpth.CD_TALHAO
           AND ad.DT_HR_LOCAL = dpth.DT_ANALISE
    ORDER BY DT_HR_LOCAL
)
SELECT *
FROM cruzado_satelite;

