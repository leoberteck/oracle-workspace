INSERT INTO CFG_PULL_INTEGRATION (IDENTIFICACAO, QUERY)
SELECT TABELA, 'SELECT ' || CAMPO || ' FROM ' || TABELA || ' ' || CONDICIONAL FROM CFG_CONSUMER_API;
COMMIT;