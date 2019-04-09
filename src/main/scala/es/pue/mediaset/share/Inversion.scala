package es.pue.mediaset.share

import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map

object Inversion {

  //***********************************************************
  // Parámetros de las clases auxiliares para utilidades
  private val utils = new Utils
  private var cfg : ConfigArgs = _
  private var parametrizationCfg: Properties = _

  //***********************************************************
  // Salesforce
  private var salesforce: Salesforce = _

  //***********************************************************
  // Parámetros del proceso en linea de comando
  private var initial_process_month: String = _
  private var end_process_month: String = _
  private var parametrization_filename: String = _

  //***********************************************************
  // Parámetros de variables y tablas en .properties

  // Valores por defecto para tablas
  private var timezone: String = _
  private var db_input : String = _
  private var db_output : String = _
  private var format : String = _
  private var compression : String = _
  private var location : String = _

  //**********************************************************

  def main(args : Array[String]): Unit = {

    init(args)

    generate(generateTables())

    save(generateTables())

    close()

  }

  /**
    * Método que inicia el proceso, se carga el fichero .properties y se instancian
    * las clases de Configuraciones y de Salesforce
    */
  def init(args: Array[String]): Unit = {

    cfg = new ConfigArgs(args)

    initial_process_month = cfg.getInitialMonth

    end_process_month = cfg.getEndMonth

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
  def generateTables(): InversionSQLstrs = {
    val tableCollection = new InversionSQLstrs(parametrizationCfg, initial_process_month, end_process_month)
    tableCollection
  }

  def generate(tableCollection: InversionSQLstrs): Unit = {

    val spark = SparkSession.builder.appName("mediaset-inv").getOrCreate()
    import spark.implicits._
    /************************************************************************************************************/
    /**
      * Objetos recuperados desde Salesforce. Se hace una llamada al método correspondiente que recupera la información de
      * Salesforce y se modela a un DataFrame con sus correspondientes cambios en los nombres de las columnas y en su tipo
      */

    val dim_agrup_cadenas_DF: DataFrame = salesforce.get_dim_agrup_cadenas(spark, salesforce.query_dim_agrup_cadenas)
    persistAsTable(dim_agrup_cadenas_DF, tableCollection.dim_agrup_cadenas)

    val tb_coeficientes_DF: DataFrame = salesforce.get_tb_coeficientes(spark, salesforce.query_tb_coeficientes)
    UtilsInversion.registerTemporalTable(tb_coeficientes_DF, tableCollection.tb_coeficientes.getName)

    val cat_eventos_DF: DataFrame = salesforce.get_cat_eventos(spark, salesforce.query_cat_eventos)
    UtilsInversion.registerTemporalTable(cat_eventos_DF, tableCollection.cat_eventos.getName)

    val cat_coeficientes_DF: DataFrame = salesforce.get_cat_coeficientes(spark, salesforce.query_cat_coeficientes)
    UtilsInversion.registerTemporalTable(cat_coeficientes_DF, tableCollection.cat_coeficientes.getName)

    val tb_parametros_DF: DataFrame = salesforce.get_tb_parametros(spark, salesforce.query_tb_parametros)
    UtilsInversion.registerTemporalTable(tb_parametros_DF, tableCollection.tb_parametros.getName)

    val tb_inversion_agregada_DF: DataFrame = salesforce.get_tb_inversion_agregada(spark, salesforce.query_inversionagregada)
    UtilsInversion.registerTemporalTable(tb_inversion_agregada_DF, tableCollection.tb_inversion_agregada.getName)


    /************************************************************************************************************/

    //Carga inicial GRPS como inversion

    val initial_inversion = getTablon_fctm_share_inv(spark, tableCollection)

    //Obtencion del listado de productos comercializados dentro de mediaset

    val producto_mediaset : List[Long] = spark.sql(tableCollection.producto_mediaset_sqlString.stripMargin).map(x => x.getLong(0)).collect.toList

    val fctm_share_inv_col_0_1: DataFrame = InversionColumnFunctions.getColumn_cod_fg_prodmediaset(initial_inversion, producto_mediaset )

    //Obtencion del listado de grupos comercializados en mediasetp

    val grupo_mediaset: List[Long] = spark.sql(tableCollection.grupo_mediaset_sqlString.stripMargin).map(x => x.getLong(0)).collect.toList

    val fctm_share_inv_col_0_2: DataFrame = InversionColumnFunctions.getColumn_cod_fg_grupomediaset(fctm_share_inv_col_0_1, grupo_mediaset)

    val fctm_share_inv_col_1 = InversionColumnFunctions.getColumn_importe_pase(fctm_share_inv_col_0_2, spark.sql(tableCollection.importe_pase_sqlString))

    UtilsInversion.registerTemporalTable(fctm_share_inv_col_1, tableCollection.fctm_share_inv.getName)


    /************************************************************************************************************/

    val costebase_marca = spark.sql(tableCollection.costebase_marca_sqlString.stripMargin)

    val costebase_anunc = spark.sql(tableCollection.costebase_anunc_sqlString.stripMargin)

    val costebase_producto = spark.sql(tableCollection.costebase_producto_sqlString.stripMargin)

    val costebase_grupo = spark.sql(tableCollection.costebase_grupo_sqlString.stripMargin)

    val costebase_else = spark.sql(tableCollection.costebase_else_sqlString.stripMargin)

    /************************************************************************************************************/

    val cod_cadena_disney: List[Long] = spark.sql(tableCollection.cod_cadena_disney_sqlString.stripMargin).map(x => x(0).asInstanceOf[Long]).collect.toList

    val tb_coeficientes_list = spark.sql(s"SELECT * FROM "+ tableCollection.tb_coeficientes.getName).as[Coeficientes].collect().toList

    /************************************************************************************************************/

    // Columna COEF_FORTA
    val coef_forta_param_valor: Int = spark.sql(tableCollection.coef_forta_param_valor_sqlString.stripMargin).map(x => x.getString(0)).collect.toList.head.toInt

    val BC_coef_forta_param_valor: Broadcast[Int] = spark.sparkContext.broadcast(coef_forta_param_valor)

    val indice_coeficiente_forta: Double = spark.sql(tableCollection.indice_coeficiente_forta_sqlString.stripMargin).map(x => x.getDouble(0)).collect.toList.head

    val BC_indice_coeficiente_forta: Broadcast[Double] = spark.sparkContext.broadcast(indice_coeficiente_forta)

    /************************************************************************************************************/
    /*
    val coef_cadena_calc_1: Map[Long, Double] = spark.sql(tableCollection.coef_cadena_calc_1_sqlString.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap


    val BC_coef_cadena_calc_1: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(coef_cadena_calc_1)
    // Columna Coef Cadena
    val coef_cadena_calc_2: Map[Long, Double] = spark.sql(tableCollection.coef_cadena_calc_2_sqlString.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_coef_cadena_calc_2: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(coef_cadena_calc_2)
    */

    /************************************************************************************************************/

    // Columna COEF_ANUNCIANTET

    //val coef_anunciante_calc: Map[Long, Double] = spark.sql(tableCollection.coef_anunciante_calc_sqlString.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    //val BC_coef_anunciante_calc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(coef_anunciante_calc)

    /************************************************************************************************************/

    val fctm_share_inv_col_2: DataFrame = InversionColumnFunctions.getColumn_costebase(spark, fctm_share_inv_col_1, costebase_marca, costebase_anunc, costebase_producto, costebase_grupo, costebase_else)


    /************************************************************************************************************/

    val fctm_share_inv_col_3: DataFrame = InversionColumnFunctions.getColumn_coef_evento(spark, fctm_share_inv_col_2, spark.sql(tableCollection.coef_evento_calc_sqlString))


    /************************************************************************************************************/

    val fctm_share_inv_col_4: DataFrame = InversionColumnFunctions.getColumn_por_pt_mediaset(spark, fctm_share_inv_col_3, spark.sql(tableCollection.por_pt_mediaset_calc_sqlString))


    /************************************************************************************************************/

    val fctm_share_inv_col_5: DataFrame = InversionColumnFunctions.getColumn_por_pt_grupocadena(spark, fctm_share_inv_col_4, spark.sql(tableCollection.por_pt_grupocadena_calc_sqlString))


    /************************************************************************************************************/

    val fctm_share_inv_col_6: DataFrame = InversionColumnFunctions.getColumn_dif_por_primetime( fctm_share_inv_col_5)


    /************************************************************************************************************/

    val fctd_share_inv_col_7: DataFrame = InversionColumnFunctions.getColumn_coef_pt( fctm_share_inv_col_6, tb_coeficientes_list, cod_cadena_disney)


    /************************************************************************************************************/

    val fctd_share_inv_col_8: DataFrame = InversionColumnFunctions.getColumn_por_cualmediaset( spark, fctd_share_inv_col_7, spark.sql(tableCollection.por_cualmediaset_calc_sqlString))


    /************************************************************************************************************/

    val fctd_share_inv_col_9: DataFrame = InversionColumnFunctions.getColumn_por_cualgrupocadena( spark, fctd_share_inv_col_8, spark.sql(tableCollection.por_cualgrupocadena_calc_sqlString))


    /************************************************************************************************************/

    val fctd_share_inv_col_10: DataFrame = InversionColumnFunctions.getColumn_dif_por_cualitativos( fctd_share_inv_col_9)


    /************************************************************************************************************/

    val fctd_share_inv_col_11: DataFrame = InversionColumnFunctions.getColumn_coef_cual( fctd_share_inv_col_10, tb_coeficientes_list)


    /************************************************************************************************************/

    val fctd_share_inv_col_12: DataFrame = InversionColumnFunctions.getColumn_por_posmediaset( spark, fctd_share_inv_col_11, spark.sql(tableCollection.por_posmediaset_calc_sqlString))


    /************************************************************************************************************/

    val fctd_share_inv_col_13: DataFrame = InversionColumnFunctions.getColumn_por_posGrupoCadena_n1( spark, fctd_share_inv_col_12, spark.sql(tableCollection.por_posgrupocadena_n1_calc_sqlString))


    /************************************************************************************************************/

    val fctd_share_inv_col_14: DataFrame = InversionColumnFunctions.getColumn_dif_por_posicionamiento( fctd_share_inv_col_13)


    /************************************************************************************************************/

    val fctd_share_inv_col_15: DataFrame = InversionColumnFunctions.getColumn_coef_posic( fctd_share_inv_col_14, tb_coeficientes_list)


    /************************************************************************************************************/

    val fctd_share_inv_col_16: DataFrame = InversionColumnFunctions.getColumn_cuota_por_grupo(spark, fctd_share_inv_col_15, spark.sql(tableCollection.cuota_por_grupo_calc_sqlString))


    /************************************************************************************************************/

    val fctd_share_inv_col_19: DataFrame = InversionColumnFunctions.getColumn_coef_cuota( fctd_share_inv_col_16, tb_coeficientes_list)


    /************************************************************************************************************/

    val fctd_share_inv_col_20: DataFrame = InversionColumnFunctions.getColumn_coef_cadena(spark, fctd_share_inv_col_19, spark.sql(tableCollection.coef_cadena_calc_1_sqlString.stripMargin), spark.sql(tableCollection.coef_cadena_calc_2_sqlString.stripMargin))



    /************************************************************************************************************/

    val fctd_share_inv_col_21: DataFrame = InversionColumnFunctions.getColumn_num_cadenas_forta_emitido( fctd_share_inv_col_20, spark.sql(tableCollection.num_cadenas_forta_calc_sqlString))



    /************************************************************************************************************/

    val fctd_share_inv_col_22: DataFrame = InversionColumnFunctions.getColumn_coef_forta( fctd_share_inv_col_21, BC_coef_forta_param_valor, BC_indice_coeficiente_forta)


    /************************************************************************************************************/

    val fctd_share_inv_col_23: DataFrame = InversionColumnFunctions.getColumn_coef_anunciante(spark, fctd_share_inv_col_22, spark.sql(tableCollection.coef_anunciante_calc_sqlString.stripMargin))


    /************************************************************************************************************/

    val fctd_share_inv_col_24: DataFrame = InversionColumnFunctions.getColumn_inv_pre(spark, fctd_share_inv_col_23)
    UtilsInversion.registerTemporalTable(fctd_share_inv_col_24, tableCollection.fctm_share_inv.getName)


    /************************************************************************************************************/

    val inv_est_pe_calc: Map[String, java.lang.Double] = spark.sql(tableCollection.inv_est_pe_calc_sqlString.stripMargin).collect().map(x => x(0).asInstanceOf[String] -> x(1).asInstanceOf[java.lang.Double]).toMap
    val BC_inv_est_pe_calc: Broadcast[Map[String, java.lang.Double]] = spark.sparkContext.broadcast(inv_est_pe_calc)

    createTable_tb_inv_agregada(spark,tableCollection, BC_inv_est_pe_calc)

    val coef_corrector_DF: DataFrame = spark.sql(tableCollection.coef_corrector_DF_sqlString.stripMargin)

    val joinDF = fctd_share_inv_col_24.join(coef_corrector_DF, fctd_share_inv_col_24("cod_cadena") === coef_corrector_DF("cod_cadena1")
      && fctd_share_inv_col_24("aniomes") === coef_corrector_DF("aniomes1"), "left")

    val fctd_share_inv_col_25: DataFrame = joinDF.drop("cod_cadena1","aniomes1")

    val fctd_share_inv_col_26: DataFrame = InversionColumnFunctions.getColumn_inv_final(fctd_share_inv_col_25)

    val result = fctd_share_inv_col_26

    UtilsInversion.registerTemporalTable(result, tableCollection.fctm_share_inv.getName)

  }

  /**
    * Siguiente paso en el proceso, guardar el resultado de los calculos y agregaciones como una tabla persistida en Cloudera
    */
  def save(tableCollection: InversionSQLstrs): Unit = {

    persistShareINV(tableCollection)

  }

  /**
    * Termina la ejecución del proceso
    */
  def close(): Unit = {

    SparkSession.builder.getOrCreate().stop()

  }

  /**
    * Creación de la tabla tb_inv_agregada
    * @param spark: Instanciación del objeto Spark
    */
  def createTable_tb_inv_agregada(spark: SparkSession,tableCollection: InversionSQLstrs,  BC_inv_est_pe_calc: Broadcast[Map[String, java.lang.Double]]) : Unit = {


    val tb_inv_agregada_1: DataFrame = spark.sql(
      "SELECT grupo_cadena AS gr_n1_infoadex, inv_est_infoadex, mes, anho FROM " + tableCollection.tb_inversion_agregada.getName)

    val tb_inv_agregada_2: DataFrame = InversionColumnFunctions.getColumn_inv_est_pe(tb_inv_agregada_1, spark.sql(tableCollection.inv_est_pe_calc_sqlString))

    val tb_inv_agregada_3: DataFrame = InversionColumnFunctions.getColumn_coef_corrector_inv_agregada(tb_inv_agregada_2)

    val tb_inv_agregada_fecha_carga: DataFrame = UtilsInversion.setCurrentTimeStamp(tb_inv_agregada_3, timezone)

    persistAsTable(tb_inv_agregada_fecha_carga, tableCollection.tb_inv_agregada)


  }

  /************************************************************************************************************/

  // Recupera datos parametrizados mediante el fichero .properties y establece valores en distintas variables
  def setPropertiesParameters (parametrizationCfg : Properties) : Unit = {

    setProcessParameters(parametrizationCfg)

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

  /************************************************************************************************************/


  /**
    * Se recupera el estado final del tablón fctm_share_inv almacenado como tabla temporal y se pasa como DataFrame
    * para persistirlo en Cloudera en el siguiente método
    */
  private def persistShareINV(tableCollection: InversionSQLstrs): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val newDF = spark.sql("SELECT * FROM " + tableCollection.fctm_share_inv.getName)

    val toPartitionDF = newDF.withColumn("fecha_part", expr("substring(fecha_dia, 1, 7)"))

    UtilsInversion.persistAsTableShareINV(toPartitionDF, tableCollection.fctm_share_inv)

  }

  /**
    * Función que recibe un DataFrame y una entidad que sea una tabla para sobreescribirla si existiera
    * con un formato, compresion, ruta, base de datos y nombre de tabla que recibe por defecto o especificados en
    * el fichero .properties
    * @param newDF: DataFrame que contiene los datos a escribir
    * @param table: Entidad de la que se obtienen los datos especificos de la tabla
    */
  def persistAsTable (newDF: DataFrame, table: Entity): Unit = {

    newDF.write.mode("overwrite").format(table.getFormat).option("compression", table.getCompression).option("path", table.getLocation).saveAsTable(table.getDBTable)
  }



  /************************************************************************************************************/

  /************************************************************************************************************/

  // Case class con las diferentes tablas que se recuperan de SalesForce. Se especifican sus columnas y sus tipos de datos.

  case class Coeficientes(Anyo : String, coeficiente: String, des_cadena: String, cod_cadena: java.lang.Long, fecha_act: java.lang.Long, fecha_fin: java.lang.Long, fecha_ini: java.lang.Long, flag: java.lang.Long, INDICE: java.lang.Double, MAX_RANGO: java.lang.Double,
                          Mes: String, MIN_RANGO: java.lang.Double)

  /************************************************************************************************************/

  /**
    * Query de agregación sobre los campos de la tabla temporal filtrada fctm_share_inv
    * @param spark: Instanciación del objeto spark para poder acceder a sus métodos de SQL
    * @return
    */

  def getTablon_fctm_share_inv(spark: SparkSession, tableCollection: InversionSQLstrs): DataFrame = {

    val grpsTable = spark.sql(tableCollection.getSharedGrps_sqlString)
    UtilsInversion.registerTemporalTable(grpsTable, tableCollection.fctm_share_inv.getName)

    val agrup_by_mediaset = spark.sql(tableCollection.getTablon_fctm_share_inv_sqlString)

    val agrup_by_Nomediaset = spark.sql(tableCollection.agrup_by_Nomediaset_sqlString)

    val inversion_DF = agrup_by_mediaset.union(agrup_by_Nomediaset).toDF()
    UtilsInversion.registerTemporalTable(inversion_DF, tableCollection.fctm_share_inv.getName)
    inversion_DF
  }

}


