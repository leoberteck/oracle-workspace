/*
Para gerar uma string randomica:
http://www.unit-conversion.info/texttools/random-string-generator/

Com essa string randomica gerar o hash com o comando:
java -jar -De='<string_gerada>' cryptography-1.0-SNAPSHOT.jar
*/
DECLARE
  V_GRUPO NUMBER;
BEGIN
  WITH get_consulta AS (
  SELECT FIRST_VALUE(CD_GRUPO) OVER ( ORDER BY DESC_GRUPO) FROM CDT_GRUPO_USUARIOS
    WHERE DESC_GRUPO LIKE '%CONSULTA%'
  )
  SELECT NVL((SELECT * FROM get_consulta), (SELECT CD_GRUPO FROM CDT_GRUPO_USUARIOS WHERE ROWNUM = 1)) INTO V_GRUPO FROM DUAL;
  INSERT INTO CDT_USUARIO VALUES (seq_cdt_usuarios.nextval, 'solinftec.integracao', :SENHA, 'solinftec integracao', 'A', 'G', V_GRUPO ,'FALSE', 'solinftec.integracao@solinftec.com.br');
  COMMIT;
END;

select * from CDT_USUARIO where CD_USUARIO = 'solinftec.integracao';