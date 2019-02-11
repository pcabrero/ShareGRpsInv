CREATE EXTERNAL TABLE mediaset.configuraciones (
    cod_campanya	STRING COMMENT	"campaña seleccionada por usuario	SFDC	SFDC	NA",
    cod_marca	STRING COMMENT "marca seleccionada por usuario	SFDC	SFDC	NA",
    cod_Anunciante_PE STRING COMMENT "anunciante seleccionado por usuario	SFDC	SFDC	NA",
    cod_Anunciante_Kantar	STRING	COMMENT "anunciante seleccionado por usuario	SFDC	SFDC	NA",
    cod_cadena	STRING	COMMENT "cadena seleccionada por usuario	SFDC	SFDC	NA",
    cod_tipologia	INT	COMMENT "tipologia seleccionada por usuario	SFDC	SFDC	NA",
    accion	INT COMMENT "(1:ACTIVO, 0:INACTIVO)	acción seleccionada por usuario 'filtrar'	SFDC	SFDC	NA",
    FLAG	INT	COMMENT "activo o inactivo 1,0	SFDC	SFDC	NA",
    FECHA_INI	INT	COMMENT "fecha inicio de filtro	SFDC	SFDC	NA",
    FECHA_FIN	INT	COMMENT "fecha fin de filtro	SFDC	SFDC	NA"
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LOCATION '/user/julio/mediaset/configuraciones';
