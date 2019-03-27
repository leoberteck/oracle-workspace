WITH detalhado_fino AS (
    SELECT
      dnth.CD_UNIDADE
      , dnth.CD_FAZENDA
      , dnth.CD_ZONA
      , dnth.CD_TALHAO
      , dnth.FONTE
      , dnth.DT_ANALISE
      , dnth.MEDIA
      , FIRST_VALUE(dnth.MEDIA)
      OVER (
        PARTITION BY dnth.CD_UNIDADE
          , dnth.CD_FAZENDA
          , dnth.CD_ZONA
          , dnth.CD_TALHAO
        ORDER BY DT_ANALISE ) AS FIRST_MEDIA
      , dnth.DESVIO_PADRAO
    FROM DDN_NDVI_TALHAO_HISTORICO dnth
    WHERE PERCENTUAL_NUVENS <= :nuvemMaxima
      AND DT_ANALISE BETWEEN TO_DATE(:dataIni, 'RRRR-MM-DD') AND TO_DATE(:dataFim, 'RRRR-MM-DD'))
  , detalhado_agrupado as (
    SELECT
      CD_UNIDADE
      , CD_FAZENDA
      , CD_ZONA
      , CD_TALHAO
      , FONTE
      , TRUNC(DT_ANALISE)  AS DT_ANALISE
      , FIRST_MEDIA
      , AVG(MEDIA)         AS MEDIA
      , AVG(DESVIO_PADRAO) AS DESVIO_PADRAO
    FROM detalhado_fino
    GROUP BY CD_UNIDADE, CD_FAZENDA, CD_ZONA, CD_TALHAO, FONTE, TRUNC(DT_ANALISE), FIRST_MEDIA
)
  , detalhado_calculado as (
    SELECT
      da.CD_UNIDADE
      , da.CD_FAZENDA
      , da.CD_ZONA
      , da.CD_TALHAO
      , da.FONTE
      , da.DT_ANALISE
      , da.DESVIO_PADRAO
      , da.MEDIA
      , NVL(CASE
          WHEN da.MEDIA > da.FIRST_MEDIA
            THEN
              (100 * (da.MEDIA - da.FIRST_MEDIA)) / da.FIRST_MEDIA
          WHEN da.MEDIA < da.FIRST_MEDIA
            THEN
              ((100 * (da.FIRST_MEDIA - da.MEDIA)) / da.FIRST_MEDIA) * -1
          END, 0) AS VL_EVOLUCAO
      , ROW_NUMBER() OVER ( ORDER BY da.CD_UNIDADE, da.CD_FAZENDA, da.CD_ZONA, da.CD_TALHAO, da.DT_ANALISE ) RN
    FROM detalhado_agrupado da
) SELECT
    CD_UNIDADE                        cdUnidade,
    CD_FAZENDA                        cdFazenda,
    CD_ZONA                           cdZona,
    CD_TALHAO                         cdTalhao,
    FONTE                             fonte,
    TO_CHAR(DT_ANALISE, 'RRRR-MM-DD') dtAnalise,
    DESVIO_PADRAO                     desvioPadrao,
    MEDIA                             media,
    VL_EVOLUCAO                       vlEvolucao
  FROM detalhado_calculado
  WHERE RN BETWEEN :start AND :end
  order by RN;

/*
GERA GEOJSON
SELECT dz_json_feature(
           p_geometry => glt.GEOMETRIA,
           p_properties => dz_json_properties_vry(
               dz_json_properties(p_name => 'cdUnidade', p_properties_string => dc.CD_UNIDADE)
               , dz_json_properties(p_name => 'cdFazenda', p_properties_string => dc.CD_FAZENDA)
               , dz_json_properties(p_name => 'cdZona', p_properties_string => dc.CD_ZONA)
               , dz_json_properties(p_name => 'cdTalhao', p_properties_string => dc.CD_TALHAO)
               , dz_json_properties(p_name => 'vlDesvioPadrao', p_properties_number => dc.DESVIO_PADRAO)
               , dz_json_properties(p_name => 'vlMedia', p_properties_number => dc.MEDIA)
               , dz_json_properties(p_name => 'vlEvolucao', p_properties_number => dc.VL_EVOLUCAO)
               , dz_json_properties(p_name => 'dtAnalise', p_properties_string => TO_CHAR(dc.DT_ANALISE, 'YYYY-MM-DD'))
           )
       ).toJSON() as FEATURE
FROM detalhado_calculado dc
  INNER JOIN GEO_LAYER_TALHAO glt ON (
    dc.CD_UNIDADE = glt.CD_UNIDADE
    AND dc.CD_FAZENDA = glt.FAZENDA
    AND dc.CD_ZONA = glt.ZONA
    AND dc.CD_TALHAO = glt.TALHAO);
*/