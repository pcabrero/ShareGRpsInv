package es.pue.mediaset.share

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties

import org.apache.spark.sql.functions.{col, unix_timestamp, from_utc_timestamp}
import org.apache.spark.sql.types._

class Salesforce {

  var login = ""
  var username = ""
  var password = ""

  var timezone = "UTC"

  val query_dim_linea_negocio = "select COD_COMUNICACION__c, COD_TIPOLOGIA__c, cod_tp_categr_km__c, cod_tp_computo_km__c, cod_tp_lineanegocio_km__c, DES_COMUNICACION__c, DES_TIPOLOGIA__c, FECHA_FIN__c, FECHA_INI__c, nom_tp_categr_km__c, nom_tp_computo_km__c, nom_tp_lineanegocio_km__c FROM dim_linea_negocio__c"

//  val query_dim_agrup_cadenas = "select COD_CADENA__c, COD_GRUPO_N0__c, COD_GRUPO_N1__c, COD_GRUPO_N2__c, DES_CADENA__c, DES_GRUPO_N0__c, DES_GRUPO_N1__c, DES_GRUPO_N2__c, FECHA_FIN__c, FECHA_INI__c, COD_FORTA__c, DES_FORTA__c FROM DIM_AGRUP_CADENAS__c"
  val query_dim_agrup_cadenas = "select COD_CADENA__c, COD_GRUPO_N0__c, COD_GRUPO_N1__c, COD_GRUPO_N2__c, DES_CADENA__c, DES_GRUPO_N0__c, DES_GRUPO_N1__c, DES_GRUPO_N2__c, FECHA_FIN__c, FECHA_INI__c, COD_FORTA__c, DES_FORTA__c FROM DIM_AGRUP_CADENAS__c"

  val query_tb_parametros = "select DES_PARAMETRO__c, FECHA_FIN__c, FECHA_INI__c, NOM_PARAM__c, VALOR__c FROM NOM_PARAM__c"
  val query_tb_coeficientes = "select Anyo__c, COEFICIENTE__c, DES_CADENA__c, FECHA_ACT__c, FECHA_FIN__c, FECHA_INI__c, FLAG__c, INDICE__c, MAX_RANGO__c, Mes__c, MIN_RANGO__c FROM TABLA_COEFICIENTE__c"
  val query_tb_configuraciones = "select COD_ACCION__c, COD_ANUNCIANTE_KANTAR__c, COD_ANUNCIANTE_PE__c, COD_CADENA__c, COD_CAMPANA__c, COD_PROGRAMA__c, COD_TIPOLOGIA__c, DES_ACCION__c, DES_ANUNCIANTE_KANTAR__c, DES_ANUNCIANTE_PE__c, DES_CADENA__c, DES_CAMPANA__c, DES_PROGRAMA__c, DES_TIPOLOGIA__c, FECHA_FIN__c, FECHA_INI__c, IIEE2_Formato__c FROM TB_CONFIGURACIONES__c"
  val query_tb_eventos = "select COD_CADENA__c, COD_EVENTO__c, COD_PROGRAMA__c, DES_CADENA__c, DES_EVENTO__c, DES_PROGRAMA__c, FECHA_FIN__c, FECHA_INI__c, FLAG__c FROM TB_EVENTOS__c "
  val query_cat_gr_cadenas_n2 = "select Name, DES_GRUPO_N2__c, FECHA_FIN__c, FECHA_INI__c FROM CAT_GR_CADENAS_N2__c"
  val query_cat_gr_cadenas_n1 = "select Name, DES_GRUPO_N1__c, FECHA_FIN__c, FECHA_INI__c FROM CAT_GR_CADENAS_N1__c"
  val query_cat_gr_cadenas = "select Name, DES_GRUPO_N0__c, FECHA_FIN__c, FECHA_INI__c FROM CAT_GR_CADENAS__c"
  val query_cat_eventos = "select Name, ACTIVO__c, DES_EVENTO__c, FECHA_FIN__c, FECHA_INI__c, INDICE__c FROM CAT_EVENTOS__c"
  val query_cat_coeficientes = "select Name, DES_COEF__c, FECHA_FIN__c, FECHA_INI__c FROM CAT_COEFICIENTES__c"
  val query_cat_nuevas_cadenas = "select Name, DES_CADENA_N__c, FECHA_FIN__c, FECHA_INI__c FROM CAT_NUEVAS_CADENAS__c"


  def setCredentials(cfg : Properties, environment : String) : Unit = {

    // Salesforce credencials
    login = cfg.getProperty("sf.login." + environment)
    username = cfg.getProperty("sf.username." + environment)
    password = cfg.getProperty("sf.password." + environment)
  }

  def setTimeZone(timezone : String) : Unit = {
    this.timezone = timezone
  }

  def getDataFrame(spark: SparkSession, query : String ): DataFrame = {
    spark.read.format("com.springml.salesforce").option("login", login).option("username", username).option("password", password).option("soql", query).option("version", "37.0").load()
  }


  def get_dim_linea_negocio(spark: SparkSession, query: String): DataFrame = {

    val dim_linea_negocio = getDataFrame(spark, query)

    dim_linea_negocio
      .withColumnRenamed("cod_comunicacion__c", "cod_comunicacion")
      .withColumnRenamed("cod_tipologia__c", "cod_tipologia")
      .withColumnRenamed("cod_tp_categr_km__c", "cod_tp_categr_km" )
      .withColumnRenamed("cod_tp_computo_km__c", "cod_tp_computo_km")
      .withColumnRenamed("cod_tp_lineanegocio_km__c", "cod_tp_lineanegocio_km")
      .withColumnRenamed("des_comunicacion__c", "des_comunicacion")
      .withColumnRenamed("des_tipologia__c", "des_tipologia")
      .withColumnRenamed("fecha_fin__c", "fecha_fin")
      .withColumnRenamed("fecha_ini__c", "fecha_ini")
      .withColumnRenamed("nom_tp_categr_km__c", "nom_tp_categr_km")
      .withColumnRenamed("nom_tp_computo_km__c", "nom_tp_computo_km")
      .withColumnRenamed("nom_tp_lineanegocio_km__c", "nom_tp_lineanegocio_km")
      .withColumn("cod_comunicacion", col("cod_comunicacion").cast(IntegerType))
      .withColumn("des_comunicacion", col("des_comunicacion").cast(StringType))
      .withColumn("cod_tipologia", col("cod_tipologia").cast(IntegerType))
      .withColumn("des_tipologia", col("des_tipologia").cast(StringType))
      .withColumn("cod_tp_lineanegocio_km", col("cod_tp_lineanegocio_km").cast(IntegerType))
      .withColumn("nom_tp_lineanegocio_km", col("nom_tp_lineanegocio_km").cast(StringType))
      .withColumn("cod_tp_categr_km", col("cod_tp_categr_km").cast(IntegerType))
      .withColumn("nom_tp_categr_km", col("nom_tp_categr_km").cast(StringType))
      .withColumn("cod_tp_computo_km", col("cod_tp_computo_km").cast(IntegerType))
      .withColumn("nom_tp_computo_km", col("nom_tp_computo_km").cast(StringType))
      .withColumn("cod_comunicacion", col("cod_comunicacion").cast(IntegerType))
      .withColumn("cod_comunicacion", col("cod_comunicacion").cast(IntegerType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))

  }

  def get_dim_agrup_cadenas(spark: SparkSession, query: String): DataFrame = {

    val dim_agrup_cadenas = getDataFrame(spark, query)

    dim_agrup_cadenas
      .withColumnRenamed("des_grupo_n1__c", "des_grupo_n1" )
      .withColumnRenamed("des_grupo_n2__c", "des_grupo_n2")
      .withColumnRenamed("des_grupo_n0__c", "des_grupo_n0")
      .withColumnRenamed("fecha_fin__c", "fecha_fin")
      .withColumnRenamed("cod_forta__c", "cod_forta")
      .withColumnRenamed("des_forta__c", "des_forta")
      .withColumnRenamed("cod_cadena__c", "cod_cadena")
      .withColumnRenamed("cod_grupo_n0__c", "cod_grupo_n0")
      .withColumnRenamed("fecha_ini__c", "fecha_ini")
      .withColumnRenamed("cod_grupo_n2__c", "cod_grupo_n2")
      .withColumnRenamed("cod_grupo_n1__c", "cod_grupo_n1")
      .withColumnRenamed("des_cadena__c", "des_cadena")
      .withColumn("cod_grupo_n0", col("cod_grupo_n0").cast(IntegerType))
      .withColumn("des_grupo_n0", col("des_grupo_n0").cast(StringType))
      .withColumn("cod_grupo_n1", col("cod_grupo_n1").cast(IntegerType))
      .withColumn("des_grupo_n1", col("des_grupo_n1").cast(StringType))
      .withColumn("cod_grupo_n2", col("cod_grupo_n2").cast(IntegerType))
      .withColumn("des_grupo_n2", col("des_grupo_n2").cast(StringType))
      .withColumn("cod_cadena", col("cod_cadena").cast(IntegerType))
      .withColumn("des_cadena", col("des_cadena").cast(StringType))
      .withColumn("cod_forta", col("cod_forta").cast(IntegerType))
      .withColumn("des_forta", col("des_forta").cast(StringType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))

  }

  def get_tb_parametros(spark: SparkSession, query: String): DataFrame = {

    val tb_parametros = getDataFrame(spark, query)
    tb_parametros
      .withColumnRenamed("DES_PARAMETRO__c", "DES_PARAMETRO" )
      .withColumnRenamed("FECHA_FIN__c", "FECHA_FIN")
      .withColumnRenamed("FECHA_INI__c", "FECHA_INI")
      .withColumnRenamed("NOM_PARAM__c", "NOM_PARAM")
      .withColumnRenamed("VALOR__c", "VALOR")
      .withColumn("DES_PARAMETRO", col("DES_PARAMETRO").cast(StringType))
      .withColumn("NOM_PARAM", col("NOM_PARAM").cast(StringType))
      .withColumn("VALOR", col("VALOR").cast(StringType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))

  }

  def get_tb_coeficientes(spark: SparkSession, query: String): DataFrame = {

    val tb_coeficientes = getDataFrame(spark, query)

    tb_coeficientes
      .withColumnRenamed("Anyo__c", "Anyo" )
      .withColumnRenamed("COEFICIENTE__c", "COEFICIENTE")
      .withColumnRenamed("DES_CADENA__c", "DES_CADENA")
      .withColumnRenamed("FECHA_ACT__c", "FECHA_ACT")
      .withColumnRenamed("FECHA_FIN__c", "FECHA_FIN")
      .withColumnRenamed("FECHA_INI__c", "FECHA_INI")
      .withColumnRenamed("FLAG__c", "FLAG")
      .withColumnRenamed("INDICE__c", "INDICE")
      .withColumnRenamed("MAX_RANGO__c", "MAX_RANGO")
      .withColumnRenamed("Mes__c", "Mes")
      .withColumnRenamed("MIN_RANGO__c", "MIN_RANGO")
      .withColumn("Anyo", col("Anyo").cast(StringType))
      .withColumn("COEFICIENTE", col("COEFICIENTE").cast(StringType))
      .withColumn("DES_CADENA", col("DES_CADENA").cast(StringType))
      .withColumn("FECHA_ACT", from_utc_timestamp(unix_timestamp(col("FECHA_ACT"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("FLAG", col("FLAG").cast(IntegerType))
      .withColumn("INDICE", col("INDICE").cast(StringType))
      .withColumn("MAX_RANGO", col("MAX_RANGO").cast(StringType))
      .withColumn("Mes", col("Mes").cast(StringType))
      .withColumn("MIN_RANGO", col("MIN_RANGO").cast(StringType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))

  }

  def get_tb_configuraciones(spark: SparkSession, query: String): DataFrame = {

    val tb_configuraciones = getDataFrame(spark, query)

    tb_configuraciones
      .withColumnRenamed("COD_ACCION__c", "COD_ACCION" )
      .withColumnRenamed("COD_ANUNCIANTE_KANTAR__c", "COD_ANUNCIANTE_KANTAR")
      .withColumnRenamed("COD_ANUNCIANTE_PE__c", "COD_ANUNCIANTE_PE")
      .withColumnRenamed("COD_CADENA__c", "COD_CADENA")
      .withColumnRenamed("COD_CAMPANA__c", "COD_CAMPANA")
      .withColumnRenamed("COD_PROGRAMA__c", "COD_PROGRAMA")
      .withColumnRenamed("COD_TIPOLOGIA__c", "COD_TIPOLOGIA")
      .withColumnRenamed("DES_ACCION__c", "DES_ACCION")
      .withColumnRenamed("DES_ANUNCIANTE_KANTAR__c", "DES_ANUNCIANTE_KANTAR")
      .withColumnRenamed("DES_ANUNCIANTE_PE__c", "DES_ANUNCIANTE_PE")
      .withColumnRenamed("DES_CADENA__c", "DES_CADENA")
      .withColumnRenamed("DES_CAMPANA__c", "DES_CAMPANA")
      .withColumnRenamed("DES_PROGRAMA__c", "DES_PROGRAMA")
      .withColumnRenamed("DES_TIPOLOGIA__c", "DES_TIPOLOGIA")
      .withColumnRenamed("FECHA_FIN__c", "FECHA_FIN")
      .withColumnRenamed("FECHA_INI__c", "FECHA_INI")
      .withColumnRenamed("IIEE2_Formato__c", "IIEE2_Formato")
      .withColumn("COD_ACCION", col("COD_ACCION").cast(IntegerType))
      .withColumn("COD_ANUNCIANTE_KANTAR", col("COD_ANUNCIANTE_KANTAR").cast(IntegerType))
      .withColumn("COD_ANUNCIANTE_PE", col("COD_ANUNCIANTE_PE").cast(IntegerType))
      .withColumn("COD_CADENA", col("COD_CADENA").cast(IntegerType))
      .withColumn("COD_CAMPANA", col("COD_CAMPANA").cast(IntegerType))
      .withColumn("COD_PROGRAMA", col("COD_PROGRAMA").cast(IntegerType))
      .withColumn("COD_TIPOLOGIA", col("COD_TIPOLOGIA").cast(IntegerType))
      .withColumn("DES_ACCION", col("DES_ACCION").cast(StringType))
      .withColumn("DES_ANUNCIANTE_KANTAR", col("DES_ANUNCIANTE_KANTAR").cast(StringType))
      .withColumn("DES_ANUNCIANTE_PE", col("DES_ANUNCIANTE_PE").cast(StringType))
      .withColumn("DES_CADENA", col("DES_CADENA").cast(StringType))
      .withColumn("DES_CAMPANA", col("DES_CAMPANA").cast(StringType))
      .withColumn("DES_PROGRAMA", col("DES_PROGRAMA").cast(StringType))
      .withColumn("DES_TIPOLOGIA", col("DES_TIPOLOGIA").cast(StringType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("IIEE2_Formato", col("IIEE2_Formato").cast(StringType))

  }

  def get_tb_eventos(spark: SparkSession, query: String ): DataFrame = {

    val tb_eventos = getDataFrame(spark, query)

    tb_eventos
      .withColumnRenamed("COD_CADENA__c", "COD_CADENA" )
      .withColumnRenamed("COD_EVENTO__c", "COD_EVENTO")
      .withColumnRenamed("COD_PROGRAMA__c", "COD_PROGRAMA")
      .withColumnRenamed("DES_CADENA__c", "DES_CADENA")
      .withColumnRenamed("DES_EVENTO__c", "DES_EVENTO")
      .withColumnRenamed("DES_PROGRAMA__c", "DES_PROGRAMA")
      .withColumnRenamed("FECHA_FIN__c", "FECHA_FIN")
      .withColumnRenamed("FECHA_INI__c", "FECHA_INI")
      .withColumnRenamed("FLAG__c", "FLAG")
      .withColumn("COD_CADENA", col("COD_CADENA").cast(IntegerType))
      .withColumn("COD_EVENTO", col("COD_EVENTO").cast(IntegerType))
      .withColumn("COD_PROGRAMA", col("COD_PROGRAMA").cast(IntegerType))
      .withColumn("DES_CADENA", col("DES_CADENA").cast(StringType))
      .withColumn("DES_EVENTO", col("DES_EVENTO").cast(StringType))
      .withColumn("DES_PROGRAMA", col("DES_PROGRAMA").cast(StringType))
      .withColumn("FLAG", col("FLAG").cast(IntegerType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))
  }

  def get_cat_gr_cadenas_n2(spark: SparkSession, query: String): DataFrame = {


    val cat_gr_cadenas_n2 = getDataFrame(spark, query)

    cat_gr_cadenas_n2
      .withColumnRenamed("Name", "COD_GRUPO_N2" )
      .withColumnRenamed("DES_GRUPO_N2__c", "DES_GRUPO_N2")
      .withColumnRenamed("FECHA_FIN__c", "FECHA_FIN")
      .withColumnRenamed("FECHA_INI__c", "FECHA_INI")
      .withColumn("COD_GRUPO_N2", col("COD_GRUPO_N2").cast(LongType))
      .withColumn("DES_GRUPO_N2", col("DES_GRUPO_N2").cast(StringType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))

  }

  def get_cat_gr_cadenas_n1(spark: SparkSession, query: String): DataFrame = {

    val cat_gr_cadenas_n1 = getDataFrame(spark, query)

    cat_gr_cadenas_n1
      .withColumnRenamed("Name", "COD_GRUPO_N1" )
      .withColumnRenamed("DES_GRUPO_N1__c", "DES_GRUPO_N1")
      .withColumnRenamed("FECHA_FIN__c", "FECHA_FIN")
      .withColumnRenamed("FECHA_INI__c", "FECHA_INI")
      .withColumn("COD_GRUPO_N1", col("COD_GRUPO_N1").cast(IntegerType))
      .withColumn("DES_GRUPO_N1", col("DES_GRUPO_N1").cast(StringType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))
  }

  def get_cat_gr_cadenas(spark: SparkSession, query: String): DataFrame = {

    val cat_gr_cadenas = getDataFrame(spark, query)

    cat_gr_cadenas
      .withColumnRenamed("Name", "COD_GRUPO_N0" )
      .withColumnRenamed("DES_GRUPO_N0__c", "DES_GRUPO_N0")
      .withColumnRenamed("FECHA_FIN__c", "FECHA_FIN")
      .withColumnRenamed("FECHA_INI__c", "FECHA_INI")
      .withColumn("COD_GRUPO_N0", col("COD_GRUPO_N0").cast(IntegerType))
      .withColumn("DES_GRUPO_N0", col("DES_GRUPO_N0").cast(StringType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))

  }

  def get_cat_eventos(spark: SparkSession, query: String): DataFrame = {

    val cat_eventos = getDataFrame(spark, query)

    cat_eventos
      .withColumnRenamed("Name", "COD_EVENTO" )
      .withColumnRenamed("ACTIVO__c", "ACTIVO")
      .withColumnRenamed("DES_EVENTO__c", "DES_EVENTO")
      .withColumnRenamed("FECHA_FIN__c", "FECHA_FIN")
      .withColumnRenamed("FECHA_INI__c", "FECHA_INI")
      .withColumnRenamed("INDICE__c", "INDICE")
      .withColumn("COD_EVENTO", col("COD_EVENTO").cast(IntegerType))
      .withColumn("ACTIVO", col("ACTIVO").cast(StringType))
      .withColumn("DES_EVENTO", col("DES_EVENTO").cast(StringType))
      .withColumn("INDICE_", col("INDICE").cast(BooleanType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))
  }

  def get_cat_coeficientes(spark: SparkSession, query: String): DataFrame = {

    val cat_coeficientes = getDataFrame(spark, query)

    cat_coeficientes
      .withColumnRenamed("Name", "COD_COEF" )
      .withColumnRenamed("DES_COEF__c", "DES_COEF")
      .withColumnRenamed("FECHA_FIN__c", "FECHA_FIN")
      .withColumnRenamed("FECHA_INI__c", "FECHA_INI")
      .withColumn("COD_COEF", col("COD_COEF").cast(IntegerType))
      .withColumn("DES_COEF", col("DES_COEF").cast(StringType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))

  }

  def get_cat_nuevas_cadenas(spark: SparkSession, query: String): DataFrame = {

    val cat_nuevas_cadenas = getDataFrame(spark, query)

    cat_nuevas_cadenas
      .withColumnRenamed("Name", "COD_CADENA_NUEVA")
      .withColumnRenamed("DES_CADENA_N__c", "DES_CADENA_N")
      .withColumnRenamed("FECHA_FIN__c", "FECHA_FIN")
      .withColumnRenamed("FECHA_INI__c", "FECHA_INI")
      .withColumn("COD_CADENA_NUEVA", col("COD_CADENA_NUEVA").cast(IntegerType))
      .withColumn("DES_CADENA_N", col("DES_CADENA_N").cast(StringType))
      .withColumn("fecha_ini", from_utc_timestamp(unix_timestamp(col("fecha_ini"), "yyyy-MM-dd").cast(TimestampType), timezone))
      .withColumn("fecha_fin", from_utc_timestamp(unix_timestamp(col("fecha_fin"), "yyyy-MM-dd").cast(TimestampType), timezone))

  }

}

