package es.pue.mediaset.share

import java.util.Properties
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object Share {

  //***************************************************
  // Parámetros de las clases auxiliares para utilidades
  private val utils = new Utils
  private var cfg : ConfigArgs = _
  private var parametrizationCfg: Properties = _

  //***************************************************
  // Salesforce
  private var salesforce : Salesforce = _

  //***************************************************
  // Parámetros del proceso en linea de comandos
  private var process_month: String = _
  private var parametrization_filename: String = _

  //***************************************************
  // Parámetros de variables y tablas en .properties

  // Valores por defecto para tablas
  private var timezone: String = _
  private var db_input : String = _
  private var db_output : String = _
  private var format : String = _
  private var compression : String = _
  private var location : String = _

  // Tablas Cloudera
  private var fctd_share_grps : Entity = _ // Tabla de salida del proceso
  private var rel_campania_trgt : Entity = _
  private var fcts_mercado_lineal : Entity = _

  // Tablas Salesforce
  private var dim_agrup_cadenas : Entity = _ // Esta entidad también se persiste como tabla
  private var dim_linea_negocio : Entity = _
  private var tb_parametros : Entity = _
  private var tb_configuraciones : Entity = _
  private var tb_eventos : Entity = _

  //***************************************************

  def main(args : Array[String]) {

   init(args)

   generate()

   save()

   close()

  }

  /**
    * Método que inicia el proceso, se carga el fichero .properties y se instancian
    * las clases de Configuraciones y de Salesforce
    */
  def init(args : Array[String]): Unit = {

    cfg = new ConfigArgs(args)

    process_month = cfg.getMesEnCurso

    parametrization_filename = cfg.getParametrizationFileName

    parametrizationCfg = utils.loadPropertiesFromPath(parametrization_filename)

    setPropertiesParameters(parametrizationCfg)

    salesforce = new Salesforce()
    salesforce.setCredentials(parametrizationCfg, "pro")
    salesforce.setTimeZone(timezone)

  }

  /**
    * Método principal del proceso donde se recuperan los objetos que se van a utilizar de SalesForce y se generan los Broadcast
    * correspondientes. A lo largo de este método se van haciendo llamadas a las funciones que generan las diferentes tablas y
    * columnas.
    */
  def generate(): Unit = {

    val spark = SparkSession.builder.appName("mediaset-share").getOrCreate()
    import spark.implicits._

    utils.showRunningProcess(cfg.getProcess)

    /************************************************************************************************************/
    /**
      * Objetos recuperados desde Salesforce. Se hace una llamada al método correspondiente que recupera la información de
      * Salesforce y se modela a un DataFrame con sus correspondientes cambios en los nombres de las columnas y en su tipo
      */

    val dim_linea_negocio_DF: DataFrame = salesforce.get_dim_linea_negocio(spark, salesforce.query_dim_linea_negocio)
    dim_linea_negocio_DF.createOrReplaceTempView(s"$dim_linea_negocio")

    val dim_agrup_cadenas_DF: DataFrame = salesforce.get_dim_agrup_cadenas(spark, salesforce.query_dim_agrup_cadenas)
    persistAsTable(dim_agrup_cadenas_DF, dim_agrup_cadenas)

    val tb_parametros_DF: DataFrame = salesforce.get_tb_parametros(spark, salesforce.query_tb_parametros)
    tb_parametros_DF.createOrReplaceTempView(s"$tb_parametros")

    val tb_configuraciones_DF: DataFrame = salesforce.get_tb_configuraciones(spark, salesforce.query_tb_configuraciones)
    tb_configuraciones_DF.createOrReplaceTempView(s"$tb_configuraciones")

    val tb_eventos_DF: DataFrame = salesforce.get_tb_eventos(spark, salesforce.query_tb_eventos)
    tb_eventos_DF.createOrReplaceTempView(s"$tb_eventos")

    /************************************************************************************************************/

    // Collecting data from SF
    val duracion_iiee: Int = spark.sql(s"""SELECT valor FROM $tb_parametros WHERE nom_param="DURACION_IIEE" """).map(r => r.getString(0)).collect.toList.head.toInt
    val tipologias_duracion: Array[Int] = spark.sql(s"""SELECT valor FROM $tb_parametros WHERE nom_param="TIPOLOGIAS_DURACION" """).map(r => r.getString(0)).collect.toList.head.split(";").map(x => x.toInt)
    val dim_linea_negocio_list: List[LineaNegocio] = spark.sql(s"SELECT * FROM $dim_linea_negocio").as[LineaNegocio].collect().toList
    val configuraciones_list: List[Configuraciones] = spark.sql(s"""SELECT * FROM $tb_configuraciones""").as[Configuraciones].collect().toList
    val agrupCadenas_list: List[AgrupCadenas] = spark.sql(s"""SELECT * FROM ${dim_agrup_cadenas.getDBTable}""").as[AgrupCadenas].collect().toList
    val rel_campania_trgt_map: Map[(Long, Long), Long] = spark.sql(s"""SELECT * FROM ${rel_campania_trgt.getDBTable}""").as[relCampaniaTrgt].collect().map( o => (o.cod_anuncio, o.cod_cadena ) -> o.cod_target ).toMap
    val eventos_list: List[Eventos] = spark.sql(s"""SELECT * FROM $tb_eventos""").as[Eventos].collect().toList


    /************************************************************************************************************/

    // Creating broadcast objects to work on the nodes
    val BC_param_duracion_iiee: Broadcast[Int] = spark.sparkContext.broadcast(duracion_iiee)
    val BC_param_tipologias_duracion: Broadcast[Array[Int]] = spark.sparkContext.broadcast(tipologias_duracion)
    val BC_dim_linea_negocio_list: Broadcast[List[LineaNegocio]] = spark.sparkContext.broadcast(dim_linea_negocio_list)
    val BC_rel_campania_trgt_map: Broadcast[Map[(Long, Long), Long]] = spark.sparkContext.broadcast(rel_campania_trgt_map)
    val BC_eventos_list: Broadcast[List[Eventos]] = spark.sparkContext.broadcast(eventos_list)
    val BC_configuraciones_list: Broadcast[List[Configuraciones]] = spark.sparkContext.broadcast(configuraciones_list)
    val BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]] = spark.sparkContext.broadcast(agrupCadenas_list)

    /************************************************************************************************************/

    val tmp_fcts_fecha_dia: DataFrame = get_tmp_fcts_fecha_dia(spark, process_month, parametrizationCfg)
    tmp_fcts_fecha_dia.createOrReplaceTempView("tmp_fcts_fecha_dia")

    val mercado_lineal_dia_agregado: DataFrame = get_mercado_lineal_dia_agregado(spark).withColumn("fecha_dia", unix_timestamp(col("fecha_dia"), "yyyy-MM-dd").cast(TimestampType))

    val share_grps_cols_inicial: DataFrame =  mercado_lineal_dia_agregado.persist
    registerShareGRPS(share_grps_cols_inicial)

//    /************************************************************************************************************/

    val codigos_de_cadenas_boing: List[Long] = spark.sql(s"""SELECT DISTINCT cod_anuncio FROM $fctd_share_grps WHERE cod_cadena IN ("5176") AND cod_anuncio IS NOT NULL""").map(r => r.getLong(0)).collect.toList
    val BC_codigos_de_cadenas_boing: Broadcast[List[Long]] = spark.sparkContext.broadcast(codigos_de_cadenas_boing)

    val codigos_de_cadenas_campemimediaset: List[Long] = spark.sql(
      s"""SELECT DISTINCT cod_cadena FROM ${dim_agrup_cadenas.getDBTable} WHERE des_grupo_n1 = "MEDIASET" AND cod_cadena IS NOT NULL
      """.stripMargin).map(r => r.getLong(0)).collect.toList
    val BC_codigos_de_cadenas_campemimediaset: Broadcast[List[Long]] = spark.sparkContext.broadcast(codigos_de_cadenas_campemimediaset)

    val codigos_de_cadenas_autonomicas: List[Long] = spark.sql(
      s"""SELECT DISTINCT cod_cadena FROM  ${dim_agrup_cadenas.getDBTable} WHERE cod_grupo_n2 = 30006 AND cod_cadena IS NOT NULL
       """.stripMargin).map(r => r.getLong(0)).collect.toList
    val BC_codigos_de_cadenas_autonomicas: Broadcast[List[Long]] = spark.sparkContext.broadcast(codigos_de_cadenas_autonomicas)

    val codigos_de_cadenas_forta: List[Long] = spark.sql(
      s"""SELECT DISTINCT cod_cadena FROM ${dim_agrup_cadenas.getDBTable} WHERE cod_forta = 1 AND cod_cadena IS NOT NULL
         """.stripMargin).map(r => r.getLong(0)).collect.toList
    val BC_codigos_de_cadenas_forta: Broadcast[List[Long]] = spark.sparkContext.broadcast(codigos_de_cadenas_forta)

    val cadenas_mediaset_grupo_n1: Set[Long] = spark.sql(s"""SELECT DISTINCT dim.cod_cadena FROM ${dim_agrup_cadenas.getDBTable} as dim, ${fcts_mercado_lineal.getDBTable} as share WHERE dim.cod_grupo_n1 = 20001 AND dim.cod_cadena IS NOT NULL AND share.cod_cadena IS NOT NULL AND dim.cod_cadena = share.cod_cadena AND share.dia_progrmd >= CONCAT(YEAR(dim.fecha_ini), MONTH(dim.fecha_ini)) AND share.dia_progrmd <= CONCAT(YEAR(dim.fecha_fin), MONTH(dim.fecha_fin))
       """.stripMargin).map(x => x.getLong(0)).collect.toSet
    val BC_cadenas_mediaset_grupo_n1: Broadcast[Set[Long]] = spark.sparkContext.broadcast(cadenas_mediaset_grupo_n1)


    val share_grps_cols_modif1 = FN_nom_sus_nomAnunc(share_grps_cols_inicial)
    val share_grps_cols_modif2 = FN_cod_sus_codAnunc(share_grps_cols_modif1)

    /************************************************************************************************************/
    //  Calculo de nuevas columnas


    val share_grps_cols_1: DataFrame = getColumn_cod_tp_lineanegocio_km(share_grps_cols_modif2, BC_dim_linea_negocio_list)
    val share_grps_cols_1_nom: DataFrame = getColumn_nom_tp_lineanegocio_km(share_grps_cols_1, BC_dim_linea_negocio_list)

    /************************************************************************************************************/

    val share_grps_cols_2: DataFrame = getColumn_cod_tp_categr_km(share_grps_cols_1_nom, BC_dim_linea_negocio_list )
    val share_grps_cols_3: DataFrame = getColumn_nom_tp_categr_km(share_grps_cols_2, BC_dim_linea_negocio_list )

    /************************************************************************************************************/

    val share_grps_cols_4: DataFrame = getColumn_cod_fg_autonomica(share_grps_cols_3, BC_agrupCadenas_list, BC_codigos_de_cadenas_autonomicas )
    val share_grps_cols_5: DataFrame = setNomOnColumn(share_grps_cols_4, "cod_fg_autonomica" , "nom_fg_autonomica")

    /************************************************************************************************************/

    val share_grps_cols_6: DataFrame = getColumn_cod_fg_forta(share_grps_cols_5, BC_agrupCadenas_list, BC_codigos_de_cadenas_forta )
    val share_grps_cols_7: DataFrame = setNomOnColumn(share_grps_cols_6, "cod_fg_forta" , "nom_fg_forta")

    /************************************************************************************************************/

    val share_grps_cols_8: DataFrame = getColumn_cod_fg_boing(share_grps_cols_7, BC_agrupCadenas_list, BC_codigos_de_cadenas_boing)
    val share_grps_cols_9: DataFrame = setNomOnColumn(share_grps_cols_8, "cod_fg_boing" , "nom_fg_boing")

    /************************************************************************************************************/

    val share_grps_cols_10: DataFrame = getColumn_cod_identif_franja(share_grps_cols_9, BC_configuraciones_list)
    val share_grps_cols_10_nom: DataFrame = getColumn_nom_identif_franja(share_grps_cols_10, BC_configuraciones_list)

    /************************************************************************************************************/

    val share_grps_cols_11: DataFrame = getColumn_cod_target_compra(share_grps_cols_10_nom, BC_rel_campania_trgt_map)

    /************************************************************************************************************/

    val share_grps_cols_12: DataFrame = getColumn_cod_fg_filtrado(share_grps_cols_11, BC_configuraciones_list)
    val share_grps_cols_13: DataFrame = setNomOnColumn_fg_filtrado(share_grps_cols_12, "cod_fg_filtrado", "nom_fg_filtrado")

    /************************************************************************************************************/

    val share_grps_cols_14: DataFrame = getColumn_cod_fg_campemimediaset(share_grps_cols_13, BC_agrupCadenas_list, BC_codigos_de_cadenas_campemimediaset)
    val share_grps_cols_14_nom: DataFrame = setNomOnColumn(share_grps_cols_14, "cod_fg_campemimediaset" , "nom_fg_campemimediaset")

    /************************************************************************************************************/

    val share_grps_cols_15: DataFrame = getColumn_cod_tp_computo_km(share_grps_cols_14_nom, BC_dim_linea_negocio_list, BC_param_tipologias_duracion, BC_param_duracion_iiee)
    val share_grps_cols_16: DataFrame = getColumn_nom_tp_computo_km(share_grps_cols_15, BC_dim_linea_negocio_list, BC_param_tipologias_duracion, BC_param_duracion_iiee)

    /************************************************************************************************************/

    val share_grps_cols_17: DataFrame = getColumn_cod_eventos(share_grps_cols_16, BC_eventos_list)
    val share_grps_cols_17_nom: DataFrame = getColumn_nom_eventos(share_grps_cols_17, BC_eventos_list)

    /************************************************************************************************************/

    val share_grps_cols_18: DataFrame = getColumn_cod_fg_anuncmediaset(share_grps_cols_17_nom, BC_agrupCadenas_list, BC_codigos_de_cadenas_campemimediaset)
    val share_grps_cols_18_nom: DataFrame  = setNomOnColumn(share_grps_cols_18, "cod_fg_anuncmediaset" , "nom_fg_anuncmediaset")

    /************************************************************************************************************/

    val share_grps_cols_19: DataFrame = getColumn_cod_fg_posicionado(share_grps_cols_18_nom)
    val share_grps_cols_20: DataFrame = getColumn_cod_fg_cadmediaset(share_grps_cols_19, BC_cadenas_mediaset_grupo_n1)


    val share_grps_current_timestamp: DataFrame = setCurrentTimeStamp(share_grps_cols_20, timezone)

    val result = share_grps_current_timestamp

    registerShareGRPS(result)

  }

  /**
    * Siguiente paso en el proceso, guardar el resultado de los calculos y agregaciones como una tabla persistida en Cloudera
    */
  def save(): Unit ={

    persistShareGRPS()

  }

  /**
    * Termina la ejecución del proceso
    */
  def close() : Unit = {

    SparkSession.builder.getOrCreate().stop()

  }

  // Recupera datos parametrizados mediante el fichero .properties y establece valores en distintas variables
  def setPropertiesParameters (parametrizationCfg : Properties) : Unit = {

    setProcessParameters(parametrizationCfg)
    setTableParameters (parametrizationCfg)

  }

  /**
    * Establece los valores por defecto que se utilizarán
    * @param parametrizationCfg: Acceso al fichero .properties
    */
  def setProcessParameters (parametrizationCfg : Properties) : Unit = {

    timezone = parametrizationCfg.getProperty("mediaset.default.timezone")
    db_input = parametrizationCfg.getProperty("mediaset.default.db.input")
    db_output = parametrizationCfg.getProperty("mediaset.default.db.output")
    format = parametrizationCfg.getProperty("mediaset.default.tbl.format")
    compression = parametrizationCfg.getProperty("mediaset.default.tbl.compression")
    location = parametrizationCfg.getProperty("mediaset.default.tbl.location")

  }

  /**
    * Se establecen los parametros para cada tabla que se utiliza, ya provenga de Cloudera o de Salesforce
    * @param parametrizationCfg: Acceso al fichero .properties
    */
  def setTableParameters (parametrizationCfg : Properties) : Unit = {

    // Cloudera
    rel_campania_trgt = new Entity(parametrizationCfg, "rel_campania_trgt" , true)
    fcts_mercado_lineal = new Entity(parametrizationCfg, "fcts_mercado_lineal", true)
    fctd_share_grps = new Entity(parametrizationCfg, "fctd_share_grps")

    // Salesforce
    dim_agrup_cadenas = new Entity(parametrizationCfg, "dim_agrup_cadenas")
    dim_linea_negocio = new Entity(parametrizationCfg, "dim_linea_negocio")
    tb_parametros = new Entity(parametrizationCfg, "tb_parametros")
    tb_configuraciones = new Entity(parametrizationCfg, "tb_configuraciones")
    tb_eventos = new Entity(parametrizationCfg, "tb_eventos")

  }

  /**
    * Se registra un DataFrame como una tabla temporal con el nombre "fctd_share_grps"
    * @param newDF: DataFrame a registrar como tabla temporal
    */
  def registerShareGRPS (newDF: DataFrame) {

    newDF.createOrReplaceTempView(s"$fctd_share_grps")

  }

  /**
    * Se añade una nueva columna en la que se utiliza la fecha para quedarnos con el formato año-mes "2019-01"
    * @param newDF: DataFrame al que se le añade la nueva columna
    * @param parametrizationCfg: Acceso al fichero .properties
    */
  def persistShareGRPS (newDF: DataFrame, parametrizationCfg : Properties) {

    val toPartitionDF = newDF.withColumn("fecha_part", expr("substring(fecha_dia, 1, 7)"))

    persistAsTableShareGrps(toPartitionDF, fctd_share_grps)

  }

  /**
    * Se recupera el estado final del tablón fctd_share_grps almacenado como tabla temporal y se pasa como DataFrame
    * para persistirlo en Cloudera en el siguiente método además se le añade una nueva columna para utilizarla como
    * partición por fecha en formato año-mes "2019-01"
    */
  private def persistShareGRPS () {

    val spark = SparkSession.builder.getOrCreate()

    val newDF = spark.sql(s"SELECT * FROM $fctd_share_grps")

    val toPartitionDF = newDF.withColumn("fecha_part", expr("substring(fecha_dia, 1, 7)"))

    persistAsTableShareGrps(toPartitionDF, fctd_share_grps)

  }

  /**
    * Función que recibe un DataFrame y una entidad que sea una tabla para sobreescribirla si existiera
    * con un formato, compresion, ruta, base de datos y nombre de tabla que recibe por defecto o especificados en
    * el fichero .properties
    * @param newDF: DataFrame que contiene los datos a escribir
    * @param table: Entidad de la que se obtienen los datos especificos de la tabla
    */
  private def persistAsTable (newDF: DataFrame, table: Entity) {

    newDF.write.mode("overwrite").format(table.getFormat).option("compression", table.getCompression).option("path", table.getLocation).saveAsTable(table.getDBTable)

  }

  /**
    * Función que recibe un DataFrame y una entidad que sea una tabla para sobreescribirla si existiera
    * con un formato, compresion, ruta, base de datos y nombre de tabla que recibe por defecto o especificados en
    * el fichero .properties. Ademas añade una partición a la tabla por el campo "fecha-part" que tiene el formato año-mes "2019-01"
    * @param newDF: DataFrame que contiene los datos a escribir
    * @param table: Entidad de la que se obtienen los datos especificos de la tabla
    */
  def persistAsTableShareGrps (newDF: DataFrame, table: Entity) {

    newDF.write.partitionBy("fecha_part").mode("overwrite").format(table.getFormat).option("compression", table.getCompression).option("path", table.getLocation).saveAsTable(table.getDBTable)

  }

  /************************************************************************************************************/
  /**
    * Añade una nueva columna a un DataFrame que almacena la última fecha de ejecución del proceso en función de una
    * zona horaria especificada mediante el fichero .properties por defecto, con objeto de que pueda ser modificada
    * @param originDF: DataFrame de origen al que se le añade la columna
    * @param timezone: Zona horaria
    * @return
    */
  def setCurrentTimeStamp(originDF: DataFrame, timezone: String): DataFrame = {
    originDF.withColumn("fecha_ult_actualiz", from_utc_timestamp(current_timestamp(), timezone))
  }

  /**
    * Función que añade una nueva columna a un DataFrame en función del nombre de la columna que se le
    * especifique y el nombre que tendrá la nueva columna que se va a crear
    * @param originDF: DataFrame de origen al que se le añade la columna
    * @param lookupColName: Columna en la que se fija
    * @param newColumn: Nombre de la nueva columna
    * @return
    */
  def setNomOnColumn(originDF : DataFrame, lookupColName : String, newColumn : String): DataFrame ={
    originDF.withColumn(newColumn, UDF_set_nom_on_column()(col(lookupColName)) )
  }

  /**
    * Función que devuelve un valor de tipo String y que es aplicado a cada registro de la columna
    * @return
    */
  def UDF_set_nom_on_column(): UserDefinedFunction = {
    udf[String, Long]( lookupColName => FN_set_nom_on_column(lookupColName))
  }

  /**
    * Lógica para asignar un "si" o un "no" en función de si el valor de la columna a mirar, es un 1 o un 0
    * @param lookupColValue: Valor de la columna recibida por la UDF
    * @return
    */
  def FN_set_nom_on_column(lookupColValue : java.lang.Long): String = {

    var result = "no"

    if(lookupColValue == 1){
      result = "si"
    }

    result
  }

  def FN_nom_sus_nomAnunc(originDF: DataFrame): DataFrame = {

    originDF.withColumn("nom_anunc", when(col("nom_anunc") === "SIN ESPECIFICAR", col("nom_anunciante_subsidiario"))
      .otherwise(col("nom_anunc"))
    )

  }

  def FN_cod_sus_codAnunc(originDF: DataFrame): DataFrame = {

    originDF.withColumn("cod_anunc", when(col("cod_anunc") === 9999999999L , col("cod_anunciante_subsidiario"))
      .otherwise(col("cod_anunc"))
    )

  }


  // Funciones para calcular y añadir a la tabla las columnas: COD_TP_COMPUTO_KM Y NOM_TP_COMPUTO_KM --------------------------------------------------------------------------------------------------------------------------------------------
  def getColumn_nom_tp_computo_km(originDF : DataFrame, BC_lineaNegocioList: Broadcast[List[LineaNegocio]], BC_param_tipologias_duracion: Broadcast[Array[Int]], BC_param_duracion_iiee:  Broadcast[Int]): DataFrame = {

    originDF.withColumn("nom_tp_computo_km", UDF_nom_tp_computo_km(
      BC_lineaNegocioList, BC_param_tipologias_duracion, BC_param_duracion_iiee )(col("fecha_dia").cast(LongType),
      col("cod_tipologia"),col("cod_comunicacion"),
      col("duracion") ))

  }

  def UDF_nom_tp_computo_km(BC_LineaNegocioList: Broadcast[List[LineaNegocio]], BC_param_tipologias_duracion: Broadcast[Array[Int]], BC_param_duracion_iiee: Broadcast[Int]): UserDefinedFunction = {

    udf[String, Long, Long, Long, Int]( (fecha_dia, cod_tipologia, cod_comunicacion, duracion) => FN_nom_tp_computo_km(BC_LineaNegocioList.value, BC_param_tipologias_duracion.value, BC_param_duracion_iiee.value, fecha_dia, cod_tipologia, cod_comunicacion, duracion ))
  }

  def FN_nom_tp_computo_km(lineaNegocioList: List[LineaNegocio], param_tipologias_duracion: Array[Int],param_duracion_iiee: Int, fecha_dia: Long, cod_tipologia: Long,  cod_comunicacion: Long, duracion: Int): String = {

    var result = "SIN ESPECIFICAR"

    for(elem <- lineaNegocioList){
      if(elem.cod_comunicacion == cod_comunicacion && elem.cod_tipologia == cod_tipologia && fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin ) {

        if(param_tipologias_duracion.contains(cod_tipologia)) {

          if(duracion.toInt < param_duracion_iiee) {
            result = "IIEE 12'"
          } else {
            result = "IIEE 3'"
          }

        } else {
          result = elem.nom_tp_computo_km
        }
      }
    }

    result
  }

  def getColumn_cod_fg_posicionado(originDF : DataFrame): DataFrame = {
    originDF.withColumn("cod_fg_posicionado", UDF_cod_fg_posicionado()(col("cod_posicion_pb2")))
  }

  def UDF_cod_fg_posicionado(): UserDefinedFunction = {

    udf[Long, Long]( cod_posicion_pb2 => FN_cod_fg_posicionado(cod_posicion_pb2))
  }


  def FN_cod_fg_posicionado(cod_posicion_pb2: Long): Long ={

    var result = 0L
    val posicionados : Set[Long] = Set(1,2,3,997,998,999)

    if (posicionados.contains(cod_posicion_pb2)){
      result = 1L
    }
    result

  }


  def getColumn_cod_tp_computo_km(originDF : DataFrame, BC_lineaNegocioList: Broadcast[List[LineaNegocio]], BC_param_tipologias_duracion: Broadcast[Array[Int]], BC_param_duracion_iiee:  Broadcast[Int]  ): DataFrame = {

    originDF.withColumn("cod_tp_computo_km", UDF_cod_tp_computo_km(BC_lineaNegocioList, BC_param_tipologias_duracion, BC_param_duracion_iiee )(col("fecha_dia").cast(LongType), col("cod_tipologia"),col("cod_comunicacion"), col("duracion") ))

  }

  def UDF_cod_tp_computo_km(BC_LineaNegocioList: Broadcast[List[LineaNegocio]], BC_param_tipologias_duracion: Broadcast[Array[Int]], BC_param_duracion_iiee: Broadcast[Int]): UserDefinedFunction = {

    udf[Long, Long, Long, Long, Int]( (fecha_dia, cod_tipologia, cod_comunicacion, duracion) => FN_cod_tp_computo_km(BC_LineaNegocioList.value, BC_param_tipologias_duracion.value, BC_param_duracion_iiee.value, fecha_dia, cod_tipologia, cod_comunicacion, duracion ))
  }

  def FN_cod_tp_computo_km(lineaNegocioList: List[LineaNegocio], param_tipologias_duracion: Array[Int],param_duracion_iiee: Int, fecha_dia: Long, cod_tipologia: Long,  cod_comunicacion: Long, duracion: Int): Long = {

    var result = 3005L

    for(elem <- lineaNegocioList){
      if(elem.cod_comunicacion == cod_comunicacion && elem.cod_tipologia == cod_tipologia && fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin ){

        if(param_tipologias_duracion.contains(cod_tipologia)){

          if(duracion.toInt < param_duracion_iiee){
            result = 3002
          }else{
            result = 3003
          }

        }else{
          result = elem.cod_tp_computo_km
        }

      }
    }

    result
  }

  // Funciones para calcular y añadir a la tabla las columnas: COD_FG_FILTRADO Y NOM_FG_FILTRADO --------------------------------------------------------------------------------------------------------------------------------------------

  def getColumn_cod_fg_filtrado(originDF: DataFrame, BC_configuraciones_list: Broadcast[List[Configuraciones]]): DataFrame = {

    originDF.withColumn("cod_fg_filtrado", UDF_cod_fg_filtrado(BC_configuraciones_list)(col("fecha_dia").cast(LongType), col("cod_anunc"),col("cod_anunciante_subsidiario"),
      col("cod_anuncio"), col("cod_cadena"), col("cod_programa"), col("cod_tipologia") ))

  }

  def UDF_cod_fg_filtrado(BC_configuraciones_list: Broadcast[List[Configuraciones]]): UserDefinedFunction = {

    udf[java.lang.Long, java.lang.Long, java.lang.Long, java.lang.Long, java.lang.Long, java.lang.Long, java.lang.Long, Long]( (fecha_dia, cod_anunc, cod_anunciante_subsidiario, cod_anuncio, cod_cadena, cod_programa, cod_tipologia) =>
      FN_cod_fg_filtrado(BC_configuraciones_list.value, fecha_dia, cod_anunc, cod_anunciante_subsidiario, cod_anuncio, cod_cadena, cod_programa, cod_tipologia))

  }

  def FN_cod_fg_filtrado(configuraciones_list: List[Configuraciones], fecha_dia: java.lang.Long, cod_anunc: java.lang.Long,  cod_anunciante_subsidiario: java.lang.Long,
                         cod_anuncio: java.lang.Long, cod_cadena: java.lang.Long, cod_programa: java.lang.Long, cod_tipologia: java.lang.Long): Long = {

    var result = 0L

    for(elem <- configuraciones_list){
      if(
        (elem.des_accion.equalsIgnoreCase("Filtrar") && elem.cod_programa == null
          &&
          (elem.cod_campana == null
            && (elem.cod_anunciante_pe == cod_anunc || elem.cod_anunciante_kantar == cod_anunciante_subsidiario)
            && (fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin)
            && elem.cod_cadena == cod_cadena
            )
          || (elem.cod_campana != null
          && (elem.cod_campana == cod_anuncio )
          && (elem.cod_anunciante_pe == cod_anunc || elem.cod_anunciante_kantar == cod_anunciante_subsidiario)
          && (fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin)
          && elem.cod_cadena == cod_cadena
          )
          )
          ||
          (elem.des_accion.equalsIgnoreCase("Filtrar") && elem.cod_programa != null
            && (elem.cod_campana == null
            && (elem.cod_anunciante_pe == null || elem.cod_anunciante_kantar == null)
            && elem.cod_cadena == cod_cadena
            && elem.cod_programa == cod_programa
            && elem.cod_tipologia == cod_tipologia
            && (fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin))
            ||
            (elem.cod_campana == null
              && (elem.cod_anunciante_pe == cod_anunc || elem.cod_anunciante_kantar == cod_anunciante_subsidiario)
              && elem.cod_cadena == cod_cadena
              && elem.cod_programa == cod_programa
              && elem.cod_tipologia == cod_tipologia
              && (fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin))
            ||
            (elem.cod_campana == cod_anuncio
              && (elem.cod_anunciante_pe == cod_anunc || elem.cod_anunciante_kantar == cod_anunciante_subsidiario)
              && elem.cod_cadena == cod_cadena
              && elem.cod_programa == cod_programa
              && elem.cod_tipologia == cod_tipologia
              && (fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin)))
      ) {
        result = 1L
      }
    }

    result
  }

  def setNomOnColumn_fg_filtrado(originDF : DataFrame, lookupColumn : String, newColumn : String): DataFrame ={
    originDF.withColumn(newColumn, when(col(lookupColumn) === "1", "si").otherwise("no"))
  }

  // Funciones para calcular y añadir a la tabla las columnas: COD_IDENTIF_FRANJA Y NOM_IDENTIF_FRANJA --------------------------------------------------------------------------------------------------------------------------------------------

  def getColumn_cod_identif_franja(originDF : DataFrame, BC_configuraciones_list: Broadcast[List[Configuraciones]]): DataFrame = {
    originDF.withColumn("cod_identif_franja", UDF_cod_identif_franja(BC_configuraciones_list)(col("fecha_dia").cast(LongType),
      col("cod_programa"), col("cod_cadena")))
  }
  def UDF_cod_identif_franja(BC_configuraciones_list: Broadcast[List[Configuraciones]]): UserDefinedFunction = {
    udf[Long, Long, Long, Long]((fecha_dia, cod_programa, cod_cadena) => FN_cod_identif_franja(BC_configuraciones_list.value, fecha_dia, cod_programa, cod_cadena ))
  }
  def FN_cod_identif_franja(configuraciones_list: List[Configuraciones], fecha_dia: Long, cod_programa: Long, cod_cadena: Long): Long = {
    var result = 0L
    for(elem <- configuraciones_list){
      if(elem.des_accion != "Filtrar") {
        if ( (elem.cod_cadena == cod_cadena && elem.cod_programa == cod_programa)
          && (fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin)) {
          result = elem.cod_accion
        } else {
          result = cod_cadena
        }
      }
    }
    result
  }

  def getColumn_nom_identif_franja(originDF : DataFrame, BC_configuraciones_list: Broadcast[List[Configuraciones]]): DataFrame = {
    originDF.withColumn("nom_identif_franja", UDF_nom_identif_franja(BC_configuraciones_list)(col("fecha_dia").cast(LongType),
      col("cod_programa"),col("cod_cadena"),col("nom_cadena")))
  }

  def UDF_nom_identif_franja(BC_configuraciones_list: Broadcast[List[Configuraciones]]): UserDefinedFunction = {
    udf[String, Long, Long, Long, String]((fecha_dia, cod_programa, cod_cadena, nom_cadena) => FN_nom_identif_franja(BC_configuraciones_list.value, fecha_dia, cod_programa, cod_cadena, nom_cadena ))
  }

  def FN_nom_identif_franja(configuraciones_list: List[Configuraciones], fecha_dia: Long,cod_programa: Long, cod_cadena: Long, nom_cadena: String): String = {
    var result = ""
    for(elem <- configuraciones_list){
      if(elem.des_accion != "Filtrar") {
        if ( (elem.cod_cadena == cod_cadena && elem.cod_programa == cod_programa)
          && (fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin)) {
          result = elem.des_accion
        } else {
          result = nom_cadena
        }
      }
    }
    result
  }

  // Funciones para calcular y añadir a la tabla las columnas: cod_tp_lineanegocio_km Y nom_tp_lineanegocio_km --------------------------------------------------------------------------------------------------------------------------------------------

  def getColumn_cod_tp_lineanegocio_km(originDF : DataFrame, BC_lineaNegocioList: Broadcast[List[LineaNegocio]]): DataFrame = {

    originDF.withColumn("cod_tp_lineanegocio_km", UDF_cod_tp_lineanegocio_km(BC_lineaNegocioList)(col("fecha_dia").cast(LongType), col("cod_tipologia"),col("cod_comunicacion") ))

  }

  def UDF_cod_tp_lineanegocio_km(BC_LineaNegocioList: Broadcast[List[LineaNegocio]]): UserDefinedFunction = {

    udf[Long, Long, Long, Long]( (fecha_dia, cod_tipologia, cod_comunicacion ) => FN_cod_tp_lineanegocio_km(BC_LineaNegocioList.value, fecha_dia, cod_tipologia, cod_comunicacion ))
  }

  def FN_cod_tp_lineanegocio_km(lineaNegocioList: List[LineaNegocio], fecha_dia: Long, cod_tipologia: Long,  cod_comunicacion: Long ): Long = {

  var result = 1003L

    for(elem <- lineaNegocioList) {
      if(elem.cod_comunicacion == cod_comunicacion
        && elem.cod_tipologia == cod_tipologia
        && fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin
        ) {
            result = elem.cod_tp_lineanegocio_km
          }
      }
    result
  }

  def getColumn_nom_tp_lineanegocio_km(originDF : DataFrame, BC_lineaNegocioList: Broadcast[List[LineaNegocio]]): DataFrame = {

    originDF.withColumn("nom_tp_lineanegocio_km", UDF_nom_tp_lineanegocio_km(BC_lineaNegocioList)(col("fecha_dia").cast(LongType), col("cod_tipologia"),col("cod_comunicacion") ))

  }

  def UDF_nom_tp_lineanegocio_km(BC_LineaNegocioList: Broadcast[List[LineaNegocio]]): UserDefinedFunction = {

    udf[String, Long, Long, Long]( (fecha_dia, cod_tipologia, cod_comunicacion ) => FN_nom_tp_lineanegocio_km(BC_LineaNegocioList.value, fecha_dia, cod_tipologia, cod_comunicacion ))
  }

  def FN_nom_tp_lineanegocio_km(lineaNegocioList: List[LineaNegocio], fecha_dia: Long, cod_tipologia: Long,  cod_comunicacion: Long ): String = {

    var result = "SIN ESPECIFICAR"

    for(elem <- lineaNegocioList) {
      if(elem.cod_comunicacion == cod_comunicacion && elem.cod_tipologia == cod_tipologia && fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin ) {
        result = elem.nom_tp_lineanegocio_km
      }
    }

    result
  }

  // Funciones para calcular y añadir a la tabla las columnas: cod_tp_categr_km Y nom_tp_categr_km --------------------------------------------------------------------------------------------------------------------------------------------

  def getColumn_cod_tp_categr_km(originDF : DataFrame, BC_lineaNegocioList: Broadcast[List[LineaNegocio]]): DataFrame = {

    originDF.withColumn("cod_tp_categr_km", UDF_cod_tp_categr_km(BC_lineaNegocioList)(col("fecha_dia").cast(LongType), col("cod_tipologia"),col("cod_comunicacion") ))

  }

  def UDF_cod_tp_categr_km(BC_LineaNegocioList: Broadcast[List[LineaNegocio]]): UserDefinedFunction = {

    udf[Long, Long, Long, Long]( (fecha_dia, cod_tipologia, cod_comunicacion ) => FN_cod_tp_categr_km(BC_LineaNegocioList.value, fecha_dia, cod_tipologia, cod_comunicacion ))
  }

  def FN_cod_tp_categr_km(lineaNegocioList: List[LineaNegocio], fecha_dia: Long, cod_tipologia: Long,  cod_comunicacion: Long ): Long = {

    var result = 2006L

    for(elem <- lineaNegocioList) {
      if(elem.cod_comunicacion == cod_comunicacion && elem.cod_tipologia == cod_tipologia &&
        fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin ) {
        result = elem.cod_tp_categr_km
      }
    }

    result
  }

  def getColumn_nom_tp_categr_km(originDF : DataFrame, BC_lineaNegocioList: Broadcast[List[LineaNegocio]]): DataFrame = {

    originDF.withColumn("nom_tp_categr_km", UDF_nom_tp_categr_km(BC_lineaNegocioList)(col("fecha_dia").cast(LongType), col("cod_tipologia"),col("cod_comunicacion") ))

  }

  def UDF_nom_tp_categr_km(BC_LineaNegocioList: Broadcast[List[LineaNegocio]]): UserDefinedFunction = {

    udf[String, Long, Long, Long]( (fecha_dia, cod_tipologia, cod_comunicacion ) => FN_nom_tp_categr_km(BC_LineaNegocioList.value, fecha_dia, cod_tipologia, cod_comunicacion ))
  }

  def FN_nom_tp_categr_km(lineaNegocioList: List[LineaNegocio], fecha_dia: Long, cod_tipologia: Long,  cod_comunicacion: Long ): String = {

    var result = "SIN ESPECIFICAR"

    for(elem <- lineaNegocioList) {
      if(elem.cod_comunicacion == cod_comunicacion && elem.cod_tipologia == cod_tipologia &&
        fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin ) {

        result = elem.nom_tp_categr_km
      }
    }

    result
  }

  // Funciones para calcular y añadir a la tabla la columna: cod_fg_autonomica  --------------------------------------------------------------------------------------------------------------------------------------------

  def getColumn_cod_fg_autonomica(originDF : DataFrame, BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]], BC_codigos_de_cadenas_autonomicas: Broadcast[List[Long]]): DataFrame = {

    originDF.withColumn("cod_fg_autonomica", UDF_cod_fg_autonomica(BC_agrupCadenas_list, BC_codigos_de_cadenas_autonomicas)(col("fecha_dia").cast(LongType), col("cod_cadena")))

  }

  def UDF_cod_fg_autonomica(BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]], BC_codigos_de_cadenas_autonomicas: Broadcast[List[Long]]): UserDefinedFunction = {

    udf[Int, Long, Long]( (fecha_dia, cod_cadena ) => FN_cod_fg_autonomica(BC_agrupCadenas_list.value, BC_codigos_de_cadenas_autonomicas.value, fecha_dia, cod_cadena ))
  }

  def FN_cod_fg_autonomica(agrupCadenas_list: List[AgrupCadenas], codigosDeCadenasAutonomicas: List[Long], fecha_dia: Long, cod_cadena: Long ): Int = {

    var result = 0

    for(elem <- agrupCadenas_list) {
      if (codigosDeCadenasAutonomicas.contains(cod_cadena) && fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin ) {

          result = 1
        }
        else {
          result = 0
        }
      }
    result
  }

  // Funciones para calcular y añadir a la tabla la columna: cod_fg_forta  --------------------------------------------------------------------------------------------------------------------------------------------

  def getColumn_cod_fg_forta(originDF : DataFrame, BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]], BC_codigos_de_cadenas_forta: Broadcast[List[Long]]): DataFrame = {

    originDF.withColumn("cod_fg_forta", UDF_cod_fg_forta(BC_agrupCadenas_list, BC_codigos_de_cadenas_forta)(col("fecha_dia").cast(LongType), col("cod_cadena")))

  }

  def UDF_cod_fg_forta(BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]], BC_codigos_de_cadenas_forta: Broadcast[List[Long]]): UserDefinedFunction = {

    udf[Int, Long, Long]( (fecha_dia, cod_cadena ) => FN_cod_fg_forta(BC_agrupCadenas_list.value, BC_codigos_de_cadenas_forta.value, fecha_dia, cod_cadena ))
  }

  def FN_cod_fg_forta(agrupCadenas_list: List[AgrupCadenas], codigosDeCadenasForta: List[Long], fecha_dia: Long, cod_cadena: Long ): Int = {

    var result = 0

    for(elem <- agrupCadenas_list) {
      if (codigosDeCadenasForta.contains(cod_cadena) && fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin ) {
          result = 1
        } else {

          result = 0
        }
      }

    result
  }

  // Funciones para calcular y añadir a la tabla la columna: cod_fg_boing  --------------------------------------------------------------------------------------------------------------------------------------------

  def getColumn_cod_fg_boing(originDF : DataFrame, BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]], BC_codigos_de_cadenas_boing: Broadcast[List[Long]]): DataFrame = {

    originDF.withColumn("cod_fg_boing", UDF_cod_fg_boing(BC_agrupCadenas_list, BC_codigos_de_cadenas_boing)(col("fecha_dia").cast(LongType), col("cod_cadena"), col("cod_anuncio")))

  }

  def UDF_cod_fg_boing(BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]], BC_codigos_de_cadenas_boing: Broadcast[List[Long]]): UserDefinedFunction = {

    udf[Int, Long, Long, Long]( (fecha_dia, cod_cadena, cod_anuncio ) => FN_cod_fg_boing(BC_agrupCadenas_list.value, BC_codigos_de_cadenas_boing.value, fecha_dia, cod_cadena, cod_anuncio ))
  }

  def FN_cod_fg_boing(agrupCadenas_list: List[AgrupCadenas], codigos_de_cadenas_boing_list: List[Long], fecha_dia: Long, cod_cadena: Long, cod_anuncio: Long ): Int = {


    var result = 0

    for(elem <- agrupCadenas_list) {
      if (codigos_de_cadenas_boing_list.contains(cod_anuncio) && fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin) {

        result = 1
      } else {
        result = 0
      }
    }
    result
  }

  // Funciones para calcular y añadir a la tabla la columna: cod_target_compra  --------------------------------------------------------------------------------------------------------------------------------------------

  def getColumn_cod_target_compra(originDF : DataFrame, BC_rel_campania_trgt_list: Broadcast[scala.collection.immutable.Map[(Long,Long),Long]]): DataFrame = {

    originDF.withColumn("cod_target_compra", UDF_cod_target_compra(BC_rel_campania_trgt_list)(col("cod_anuncio"),col("cod_cadena") ))

  }

  def UDF_cod_target_compra(BC_rel_campania_trgt_list: Broadcast[scala.collection.immutable.Map[(Long,Long),Long]]): UserDefinedFunction = {

    udf[Long, Long, Long]( (cod_anuncio, cod_cadena ) => FN_cod_target_compra(BC_rel_campania_trgt_list.value, cod_anuncio, cod_cadena ))
  }

  def FN_cod_target_compra(rel_campania_trgt_map: scala.collection.immutable.Map[(Long,Long),Long], cod_anuncio: Long, cod_cadena: Long ): Long = {

    rel_campania_trgt_map.getOrElse((cod_anuncio,cod_cadena),0L)

  }

  // Funciones para calcular y añadir a la tabla la columna: cod_fg_campemimediaset  --------------------------------------------------------------------------------------------------------------------------------------------

  def getColumn_cod_fg_campemimediaset(originDF : DataFrame, BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]], BC_codigos_de_cadenas_campemimediaset: Broadcast[List[Long]]): DataFrame = {

    originDF.withColumn("cod_fg_campemimediaset", UDF_cod_fg_campemimediaset(BC_agrupCadenas_list, BC_codigos_de_cadenas_campemimediaset )(col("fecha_dia").cast(LongType), col("cod_anuncio"),col("cod_cadena") ))

  }

  def UDF_cod_fg_campemimediaset(BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]], BC_codigos_de_cadenas_campemimediaset: Broadcast[List[Long]]): UserDefinedFunction = {

    udf[Int, Long, Long, Long]( (fecha_dia, cod_anuncio, cod_cadena ) => FN_cod_fg_campemimediaset(BC_agrupCadenas_list.value, BC_codigos_de_cadenas_campemimediaset.value, fecha_dia, cod_anuncio, cod_cadena ))
  }

  def FN_cod_fg_campemimediaset(agrupCadenas_list: List[AgrupCadenas], codigos_de_cadenas_campemimediaset: List[Long], fecha_dia: Long, cod_anuncio: Long, cod_cadena: Long ): Int = {

    var result = -1

    for(elem <- agrupCadenas_list) {
      if(codigos_de_cadenas_campemimediaset.contains(cod_cadena) && fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin) {

        result = 1

      } else {
        result = 0
      }
    }

    result
  }






  // Funciones para calcular y añadir a la tabla las columnas: cod_fg_evento, nom_fg_evento  --------------------------------------------------------------------------------------------------------------------------------------------

  def getColumn_cod_eventos(originDF : DataFrame, BC_eventos_list: Broadcast[List[Eventos]]): DataFrame = {

    originDF.withColumn("cod_eventos", UDF_cod_eventos(BC_eventos_list)(col("fecha_dia").cast(LongType), col("cod_cadena"),col("cod_programa") ))

  }

  def UDF_cod_eventos(BC_eventos_list: Broadcast[List[Eventos]]): UserDefinedFunction = {

    udf[Long, Long, Long, Long]( (fecha_dia, cod_cadena, cod_programa ) => FN_cod_eventos(BC_eventos_list.value, fecha_dia, cod_cadena, cod_programa ))
  }

  def FN_cod_eventos(eventos_list: List[Eventos], fecha_dia: Long, cod_cadena: Long, cod_programa: Long ): Long = {

    var result = 0L

    for(elem <- eventos_list) {
      if(cod_cadena == elem.cod_cadena && cod_programa == elem.cod_programa
        && fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin) {

        result = elem.cod_evento

      } else {
        result = 0
      }
    }

    result
  }

  def getColumn_nom_eventos(originDF : DataFrame, BC_eventos_list: Broadcast[List[Eventos]]): DataFrame = {

    originDF.withColumn("nom_eventos", UDF_nom_eventos(BC_eventos_list)(col("fecha_dia").cast(LongType), col("cod_cadena"),col("cod_programa") ))

  }

  def UDF_nom_eventos(BC_eventos_list: Broadcast[List[Eventos]]): UserDefinedFunction = {

    udf[String, Long, Long, Long]( (fecha_dia, cod_cadena, cod_programa ) => FN_nom_eventos(BC_eventos_list.value, fecha_dia, cod_cadena, cod_programa ))
  }

  def FN_nom_eventos(eventos_list: List[Eventos], fecha_dia: Long, cod_cadena: Long, cod_programa: Long ): String = {

    var result = ""

    for(elem <- eventos_list) {
      if(cod_cadena == elem.cod_cadena && cod_programa == elem.cod_programa
        && fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin) {

        result = elem.des_evento

      } else {
        result = ""
      }
    }

    result
  }

  // Funciones para calcular y añadir a la tabla la columna: cod_fg_anuncmediaset  --------------------------------------------------------------------------------------------------------------------------------------------

  def getColumn_cod_fg_anuncmediaset(originDF : DataFrame, BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]], BC_codigos_de_cadenas_campemimediaset: Broadcast[List[Long]]): DataFrame = {

    originDF.withColumn("cod_fg_anuncmediaset", UDF_cod_fg_anuncmediaset(BC_agrupCadenas_list, BC_codigos_de_cadenas_campemimediaset )(col("fecha_dia").cast(LongType), col("cod_anunciante_subsidiario"),col("cod_cadena") ))

  }

  def UDF_cod_fg_anuncmediaset(BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]], BC_codigos_de_cadenas_campemimediaset: Broadcast[List[Long]]): UserDefinedFunction = {

    udf[Int, Long, Long, Long]( (fecha_dia, cod_anunciante_subsidiario, cod_cadena ) => FN_cod_fg_anuncmediaset(BC_agrupCadenas_list.value, BC_codigos_de_cadenas_campemimediaset.value, fecha_dia, cod_anunciante_subsidiario, cod_cadena ))
  }

  def FN_cod_fg_anuncmediaset(agrupCadenas_list: List[AgrupCadenas], codigos_de_cadenas_campemimediaset: List[Long], fecha_dia: Long, cod_anunciante_subsidiario: Long, cod_cadena: Long ): Int = {

    var result = -1

    for(elem <- agrupCadenas_list) {
      if(codigos_de_cadenas_campemimediaset.contains(cod_cadena) && fecha_dia >= elem.fecha_ini && fecha_dia <= elem.fecha_fin) {

        result = 1

      } else {
        result = 0
      }
    }

    result
  }

  def getColumn_cod_fg_cadmediaset(originDF: DataFrame, BC_cadenas_mediaset_grupo_n1: Broadcast[Set[Long]]): DataFrame = {
    originDF.withColumn("cod_fg_cadmediaset", UDF_cod_fg_cadmediaset(BC_cadenas_mediaset_grupo_n1)(col("cod_cadena")))
  }

  def UDF_cod_fg_cadmediaset(BC_cadenas_mediaset_grupo_n1: Broadcast[Set[Long]]): UserDefinedFunction = {

    udf[Int, Long]( cod_cadena => FN_cod_fg_cadmediaset(BC_cadenas_mediaset_grupo_n1.value, cod_cadena))

  }

  def FN_cod_fg_cadmediaset(cadenasMediasetGrupo_n1: Set[Long], cod_cadena: Long): Int =  {

    var result = 0
    if( cadenasMediasetGrupo_n1.contains(cod_cadena)) {
      result = 1
    }
    result
  }












  /************************************************************************************************************/

  // Case class con las diferentes tablas que se recuperan de SalesForce. Se especifican sus columnas y sus tipos de datos.

  case class LineaNegocio(cod_tp_categr_km: java.lang.Long, cod_tipologia: java.lang.Long, cod_tp_lineanegocio_km: java.lang.Long, fecha_fin: Long,
                          nom_tp_lineanegocio_km: String, des_comunicacion: String, des_tipologia: String,
                          nom_tp_categr_km: String, fecha_ini: Long, cod_comunicacion: java.lang.Long, nom_tp_computo_km: String, cod_tp_computo_km: java.lang.Long )

  case class AgrupCadenas(des_grupo_n1 : String, des_grupo_n2: String, des_grupo_n0: String, fecha_fin: Long, cod_forta: java.lang.Long, des_forta: String,
                          cod_cadena: java.lang.Long, cod_grupo_n0: java.lang.Long, fecha_ini: Long,
                          cod_grupo_n2: java.lang.Long, cod_grupo_n1: java.lang.Long, des_cadena: String)

  case class relCampaniaTrgt(cod_anuncio: Long, nom_anuncio: String, cod_cadena: Long, nom_cadena: String, cod_target: Long, cod_target_gen_may: java.lang.Long,
                             nom_target_gen_may: String, fecha_hora: java.lang.Long, origen_datos: String)

  case class Configuraciones(cod_accion : java.lang.Long, cod_anunciante_kantar: java.lang.Long, cod_anunciante_pe: java.lang.Long, cod_cadena: java.lang.Long,
                             cod_campana: java.lang.Long, cod_programa: java.lang.Long, cod_tipologia: java.lang.Long, des_accion: String, des_anunciante_kantar: String,
                             des_anunciante_pe: String, des_cadena: String, des_campana: String, des_programa: String, des_tipologia: String, fecha_fin: Long, fecha_ini: Long, iiee2_formato: String)

  case class Parametros(des_parametro : String, fecha_fin: Long, fecha_ini: Long, nom_param: String, valor: String)

  case class Coeficientes(Anyo : String, coeficiente: String, des_cadena: String, fecha_act: Long, fecha_fin: Long, fecha_ini: Long, flag: java.lang.Long, INDICE: String, MAX_RANGO: String,
                          Mes: String, MIN_RANGO: String)

  case class Eventos(cod_cadena : java.lang.Long, cod_evento: java.lang.Long, cod_programa: java.lang.Long, des_cadena: String, des_evento: String,
                     des_programa: String, fecha_fin: Long, fecha_ini: Long, flag: java.lang.Long)

  case class Cadenas_n2(cod_grupo_n2: java.lang.Long, des_grupo_n2: String, fecha_fin: Long, fecha_ini: Long)

  case class Cadenas_n1(cod_grupo_n1: java.lang.Long, des_grupo_n1: String, fecha_fin: Long, fecha_ini: Long)

  case class Cadenas(cod_grupo_n0: java.lang.Long, des_grupo_n0: String, fecha_fin: Long, fecha_ini: Long)

  case class Cat_eventos(cod_evento: java.lang.Long, activo: String, des_evento: Long, fecha_fin: Long, fecha_ini: Long, indice: Boolean)

  case class Cat_coeficientes(cod_coef: java.lang.Long, des_coef: String, fecha_fin: Long, fecha_ini: Long)

  case class Cat_nuevas_cadenas(cod_cadena_nueva: java.lang.Long, des_cadena_n: String, fecha_fin: Long, fecha_ini: Long)

  /************************************************************************************************************/


  /**
    * Query inicial que selecciona los datos de la tabla fcts_mercado_lineal. También se realizan filtros para acotar la recuperación de datos
    * @param spark: Instanciación del objeto spark para poder acceder a sus métodos de SQL
    * @return DataFrame obtenido al realizar la query sobre la tabla: mediaset.fcts_mercado_lineal
    */
  def get_tmp_fcts_fecha_dia(spark: SparkSession, process_month: String, parametrizationCfg: Properties): DataFrame = {

    spark.sql(s"""
      SELECT
      dia_progrmd AS fecha_dia,
      dia_progrmd,
      cod_mes_com,
      nom_mes_com,
      aniomes,
      COD_PERIODO,
      NOM_PERIODO,
      COD_DAY_PART,
      DES_DAY_PART,
      COD_CADENA,
      NOM_CADENA,
      COD_ANUNCIO,
      NOM_ANUNCIO,
      cod_anunciante_subsidiario,
      nom_anunciante_subsidiario,
      COD_SECT_GEOG,
      NOM_SECT_GEOG,
      COD_TIPOLOGIA,
      NOM_TIPOLOGIA,
      COD_COMUNICACION,
      NOM_COMUNICACION,
      COD_PRODUCTO,
      NOM_PRODUCTO,
      COD_GRUPO,
      NOM_GRUPO,
      COD_SECTOR,
      NOM_SECTOR,
      DURACION,
      COD_POS_PB2,
      COD_POS_PB3,
      NUM_SPOTS_PB2,
      NUM_SPOTS_PB3,
      COD_POSICION_PB2,
      NOM_POSICION_PB2,
      COD_POSICION_PB3,
      NOM_POSICION_PB3,
      COD_CUALITATIVO,
      NOM_CUALITATIVO,
      COD_CUALITATIVO_PE,
      NOM_CUALITATIVO_PE,
      COD_EJECUTIVO,
      NOM_EJECUTIVO,
      COD_SUBDIVISION,
      NOM_SUBDIVISION,
      COD_DIVISION,
      NOM_DIVISION,
      COD_AREA,
      NOM_AREA,
      cod_dir_comercial,
      NOM_DIR_COMERCIAL,
      COD_DIR_GENERAL,
      NOM_DIR_GENERAL,
      COD_EMPRESA,
      NOM_EMPRESA,
      COD_MARCA,
      NOM_MARCA,
      COD_ANUNC,
      NOM_ANUNC,
      COD_HOLDING,
      NOM_HOLDING,
      COD_CENTRAL,
      NOM_CENTRAL,
      COD_GRUPO_CENT,
      NOM_GRUPO_CENT,
      COD_MARCA_H,
      NOM_MARCA_H,
      COD_ANUNC_H,
      NOM_ANUNC_H,
      COD_TARGET,
      NOM_TARGET,
      COD_FG_ANUN_LOCAL,
      NOM_FG_ANUN_LOCAL,
      COD_FG_TELEVENTA,
      NOM_FG_TELEVENTA,
      COD_FG_PUB_COMPARTIDA,
      NOM_FG_PUB_COMPARTIDA,
      COD_FG_AUTOPROMO,
      NOM_FG_AUTOPROMO,
      NOM_PROGRAMA,
      COD_PROGRAMA,
      -- COLUMNAS SOBRE LAS QUE SE AGREGARÁ
        CANT_PASES,
      GRPS_BRUTOS,
      GRPS_20,
      GRPS_VOSDAL,
      GRPS_20_VOSDAL,
      GRPS_BRUTO_INV,
      GRPS_20_INV,
      GRPS_VOSDAL_INV,
      GRPS_20_VOSDAL_INV,
      GRPS_TIMESHIFT,
      GRPS_20_TIMESHIFT,
      GRPS_TIMESHIFT_INV,
      GRPS_20_TIMESHIFT_INV,
      GRPS_TOTALES,
      GRPS_20_TOTALES
        FROM ${fcts_mercado_lineal.getDBTable}
        WHERE nom_sect_geog != "PBC"
          AND cod_target IN (5, 3, 2)
          --AND substr(dia_progrmd, 0, 10) >= "${utils.getFechaActual}-01" AND substr(dia_progrmd, 0, 10) <= "${utils.getFechaActual}-31"
          AND substr(dia_progrmd, 0, 10) >= "1999-01-01" AND substr(dia_progrmd, 0, 10) <= "2099-01-31"
    """)
  }


  /**
    * Query que se lanza sobre la tabla temporal tmp_fcts_fecha_dia para agrupar sus columnas con el objetivo de realizar el cálculo o suma sobre varios campos
    * @param spark: Instanciación del objeto spark para poder acceder a sus métodos de SQL
    * @return
    */
  def get_mercado_lineal_dia_agregado(spark: SparkSession): DataFrame = {
    spark.sql("""
                            SELECT
                               fecha_dia,
                               dia_progrmd,
                               cod_mes_com,
                               nom_mes_com,
                               aniomes,
                               COD_PERIODO,
                               NOM_PERIODO,
                               COD_DAY_PART,
                               DES_DAY_PART,
                               COD_CADENA,
                               NOM_CADENA,
                               COD_ANUNCIO,
                               NOM_ANUNCIO,
                               cod_anunciante_subsidiario,
                               nom_anunciante_subsidiario,
                               COD_SECT_GEOG,
                               NOM_SECT_GEOG,
                               COD_TIPOLOGIA,
                               NOM_TIPOLOGIA,
                               COD_COMUNICACION,
                               NOM_COMUNICACION,
                               COD_PRODUCTO,
                               NOM_PRODUCTO,
                               COD_GRUPO,
                               NOM_GRUPO,
                               COD_SECTOR,
                               NOM_SECTOR,
                               DURACION,
                               COD_POS_PB2,
                               COD_POS_PB3,
                               NUM_SPOTS_PB2,
                               NUM_SPOTS_PB3,
                               COD_POSICION_PB2,
                               NOM_POSICION_PB2,
                               COD_POSICION_PB3,
                               NOM_POSICION_PB3,
                               COD_CUALITATIVO,
                               NOM_CUALITATIVO,
                               COD_CUALITATIVO_PE,
                               NOM_CUALITATIVO_PE,
                               COD_EJECUTIVO,
                               NOM_EJECUTIVO,
                               COD_SUBDIVISION,
                               NOM_SUBDIVISION,
                               COD_DIVISION,
                               NOM_DIVISION,
                               COD_AREA,
                               NOM_AREA,
                               cod_dir_comercial,
                               NOM_DIR_COMERCIAL,
                               COD_DIR_GENERAL,
                               NOM_DIR_GENERAL,
                               COD_EMPRESA,
                               NOM_EMPRESA,
                               COD_MARCA,
                               NOM_MARCA,
                               COD_ANUNC,
                               NOM_ANUNC,
                               COD_HOLDING,
                               NOM_HOLDING,
                               COD_CENTRAL,
                               NOM_CENTRAL,
                               COD_GRUPO_CENT,
                               NOM_GRUPO_CENT,
                               COD_MARCA_H,
                               NOM_MARCA_H,
                               COD_ANUNC_H,
                               NOM_ANUNC_H,
                               COD_TARGET,
                               NOM_TARGET,
                               COD_FG_ANUN_LOCAL,
                               NOM_FG_ANUN_LOCAL,
                               COD_FG_TELEVENTA,
                               NOM_FG_TELEVENTA,
                               COD_FG_PUB_COMPARTIDA,
                               NOM_FG_PUB_COMPARTIDA,
                               COD_FG_AUTOPROMO,
                               NOM_FG_AUTOPROMO,
                               NOM_PROGRAMA,
                               COD_PROGRAMA,
                               SUM(CANT_PASES) AS CANT_PASES,
                               SUM(GRPS_BRUTOS) AS GRPS_BRUTOS,
                               SUM(GRPS_20) AS GRPS_20,
                               SUM(GRPS_VOSDAL) AS GRPS_VOSDAL,
                               SUM(GRPS_20_VOSDAL) AS GRPS_20_VOSDAL,
                               SUM(GRPS_BRUTO_INV) AS GRPS_BRUTO_INV,
                               SUM(GRPS_20_INV) AS GRPS_20_INV,
                               SUM(GRPS_VOSDAL_INV) AS GRPS_VOSDAL_INV,
                               SUM(GRPS_20_VOSDAL_INV) AS GRPS_20_VOSDAL_INV,
                               SUM(GRPS_TIMESHIFT) AS GRPS_TIMESHIFT,
                               SUM(GRPS_20_TIMESHIFT) AS GRPS_20_TIMESHIFT,
                               SUM(GRPS_TIMESHIFT_INV) AS GRPS_TIMESHIFT_INV,
                               SUM(GRPS_20_TIMESHIFT_INV) AS GRPS_20_TIMESHIFT_INV,
                               SUM(GRPS_TOTALES) AS GRPS_TOTALES,
                               SUM(GRPS_20_TOTALES) AS GRPS_20_TOTALES

                             FROM tmp_fcts_fecha_dia
                             GROUP BY
                               fecha_dia,
                               dia_progrmd,
                               cod_mes_com,
                               nom_mes_com,
                               aniomes,
                               COD_PERIODO,
                               NOM_PERIODO,
                               COD_MES_COM,
                               NOM_MES_COM,
                               COD_DAY_PART,
                               DES_DAY_PART,
                               COD_CADENA,
                               NOM_CADENA,
                               COD_ANUNCIO,
                               NOM_ANUNCIO,
                               cod_anunciante_subsidiario,
                               nom_anunciante_subsidiario,
                               COD_SECT_GEOG,
                               NOM_SECT_GEOG,
                               COD_TIPOLOGIA,
                               NOM_TIPOLOGIA,
                               COD_COMUNICACION,
                               NOM_COMUNICACION,
                               COD_PRODUCTO,
                               NOM_PRODUCTO,
                               COD_GRUPO,
                               NOM_GRUPO,
                               COD_SECTOR,
                               NOM_SECTOR,
                               DURACION,
                               COD_POS_PB2,
                               COD_POS_PB3,
                               NUM_SPOTS_PB2,
                               NUM_SPOTS_PB3,
                               COD_POSICION_PB2,
                               NOM_POSICION_PB2,
                               COD_POSICION_PB3,
                               NOM_POSICION_PB3,
                               COD_CUALITATIVO,
                               NOM_CUALITATIVO,
                               COD_EJECUTIVO,
                               NOM_EJECUTIVO,
                               COD_SUBDIVISION,
                               NOM_SUBDIVISION,
                               COD_DIVISION,
                               NOM_DIVISION,
                               COD_AREA,
                               NOM_AREA,
                               cod_dir_comercial,
                               NOM_DIR_COMERCIAL,
                               COD_DIR_GENERAL,
                               NOM_DIR_GENERAL,
                               COD_EMPRESA,
                               NOM_EMPRESA,
                               COD_MARCA,
                               NOM_MARCA,
                               COD_ANUNC,
                               NOM_ANUNC,
                               COD_HOLDING,
                               NOM_HOLDING,
                               COD_CENTRAL,
                               NOM_CENTRAL,
                               COD_MARCA_H,
                               NOM_MARCA_H,
                               COD_ANUNC_H,
                               NOM_ANUNC_H,
                               COD_TARGET,
                               NOM_TARGET,
                               COD_FG_ANUN_LOCAL,
                               NOM_FG_ANUN_LOCAL,
                               COD_FG_TELEVENTA,
                               NOM_FG_TELEVENTA,
                               COD_FG_PUB_COMPARTIDA,
                               NOM_FG_PUB_COMPARTIDA,
                               COD_FG_AUTOPROMO,
                               NOM_FG_AUTOPROMO,
                               NOM_PROGRAMA,
                               COD_PROGRAMA,
                               COD_GRUPO_CENT,
                               NOM_GRUPO_CENT,
                               COD_CUALITATIVO_PE,
                               NOM_CUALITATIVO_PE
                             """)
  }


}
