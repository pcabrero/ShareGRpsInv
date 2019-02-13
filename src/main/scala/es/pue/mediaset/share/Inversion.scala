package es.pue.mediaset.share

import java.util.Properties
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.immutable.Map
import org.apache.spark.sql.UDFRegistration

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
  private var process_month: String = _
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

  // Tablas Cloudera
  private var fctd_share_grps : Entity = _
  private var fctm_share_inv : Entity = _  // Tabla de salida del proceso
  private var rel_campania_trgt : Entity = _
  private var fcts_mercado_lineal : Entity = _
  private var ordenado : Entity = _

  // Tablas Salesforce
  private var dim_agrup_cadenas : Entity = _    // Esta entidad también se persiste como tabla
  private var dim_linea_negocio : Entity = _
  private var tb_parametros : Entity = _
  private var tb_configuraciones : Entity = _
  private var tb_eventos : Entity = _
  private var r_sectores_econ : Entity = _
  private var tb_inv_agregada : Entity = _
  private var tb_coeficientes: Entity = _
  private var cat_eventos: Entity = _
  private var cat_coeficientes: Entity = _
  private var tb_inversion_agregada: Entity = _

  //**********************************************************

  def main(args : Array[String]): Unit = {

    init(args)

    generate()

    save()

    close()

  }

  /**
    * Método que inicia el proceso, se carga el fichero .properties y se instancian
    * las clases de Configuraciones y de Salesforce
    */
  def init(args: Array[String]): Unit = {

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

    val spark = SparkSession.builder.appName("mediaset-inv").getOrCreate()
    import spark.implicits._

    /************************************************************************************************************/
    /**
      * Objetos recuperados desde Salesforce. Se hace una llamada al método correspondiente que recupera la información de
      * Salesforce y se modela a un DataFrame con sus correspondientes cambios en los nombres de las columnas y en su tipo
      */

    val dim_agrup_cadenas_DF: DataFrame = salesforce.get_dim_agrup_cadenas(spark, salesforce.query_dim_agrup_cadenas)
    persistAsTable(dim_agrup_cadenas_DF, dim_agrup_cadenas)

    val tb_coeficientes_DF: DataFrame = salesforce.get_tb_coeficientes(spark, salesforce.query_tb_coeficientes)
    registerTemporalTable(tb_coeficientes_DF, s"$tb_coeficientes")

    val cat_eventos_DF: DataFrame = salesforce.get_cat_eventos(spark, salesforce.query_cat_eventos)
    registerTemporalTable(cat_eventos_DF, s"$cat_eventos")

    val cat_coeficientes_DF: DataFrame = salesforce.get_cat_coeficientes(spark, salesforce.query_cat_coeficientes)
    registerTemporalTable(cat_coeficientes_DF, s"$cat_coeficientes")

    val tb_parametros_DF: DataFrame = salesforce.get_tb_parametros(spark, salesforce.query_tb_parametros)
    registerTemporalTable(tb_parametros_DF, s"$tb_parametros")

    val tb_inversion_agregada_DF: DataFrame = salesforce.get_tb_inversion_agregada(spark, salesforce.query_inversionagregada)
    registerTemporalTable(tb_inversion_agregada_DF, s"$tb_inversion_agregada")

    /************************************************************************************************************/

    // Collecting data from SF
    // Creating broadcast objects to work on the nodes
    // TODO Revisar si es necesario recorrer la tabla entera o se puede optimizar se está utilizando en 4 casos
    val tb_coeficientes_list = spark.sql(s"SELECT * FROM $tb_coeficientes").as[Coeficientes].collect().toList
    val BC_tb_coeficientes_list = spark.sparkContext.broadcast(tb_coeficientes_list)

    /************************************************************************************************************/

    val por_pt_mediaset_calc: Map[Long, Double] = spark.sql(
      s"""SELECT in_mediaset.cod_cadena, (in_pt.sum_campana_grps_in_pt / in_mediaset.sum_campana_grps_in_mediaset) AS por_pt_mediaset
         |FROM (SELECT cod_cadena, SUM(grps_20) AS sum_campana_grps_in_pt FROM ${fctd_share_grps.getDBTable} WHERE des_day_part = "PT"
         | AND $fctd_share_grps.cod_cadena IN (SELECT $dim_agrup_cadenas.cod_cadena FROM ${dim_agrup_cadenas.getDBTable} WHERE
         | cod_grupo_n1 = "20001")
         | GROUP BY cod_cadena) AS in_pt
         | JOIN
         | (SELECT $fctd_share_grps.cod_cadena AS cod_cadena, SUM($fctd_share_grps.grps_20) AS sum_campana_grps_in_mediaset
         | FROM ${fctd_share_grps.getDBTable} WHERE $fctd_share_grps.cod_cadena IN (SELECT $dim_agrup_cadenas.cod_cadena FROM
         | ${dim_agrup_cadenas.getDBTable} WHERE cod_grupo_n1 = "20001")
         | GROUP BY $fctd_share_grps.cod_cadena) AS in_mediaset
         | ON in_pt.cod_cadena = in_mediaset.cod_cadena
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_por_pt_mediaset_calc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(por_pt_mediaset_calc)

    /************************************************************************************************************/

    val por_cualmediaset_calc: Map[Long, Double] = spark.sql(
      s"""SELECT in_mediaset.cod_cadena, (in_cual.sum_campana_grps_in_cual / in_mediaset.sum_campana_grps_in_mediaset)
         |AS por_cual_mediaset FROM (SELECT cod_cadena, SUM(grps_20) AS sum_campana_grps_in_cual FROM ${fctd_share_grps.getDBTable} WHERE cod_cualitativo = 1
         |GROUP BY cod_cadena) AS in_cual
         |JOIN
         |(SELECT $fctd_share_grps.cod_cadena AS cod_cadena, SUM($fctd_share_grps.grps_20) AS sum_campana_grps_in_mediaset
         |FROM ${fctd_share_grps.getDBTable}
         WHERE $fctd_share_grps.cod_cadena IN (SELECT $dim_agrup_cadenas.cod_cadena FROM ${dim_agrup_cadenas.getDBTable} WHERE
         cod_grupo_n1 = "20001")
         GROUP BY $fctd_share_grps.cod_cadena) AS in_mediaset
         ON (in_cual.cod_cadena = in_mediaset.cod_cadena)
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_por_cualmediaset_calc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(por_cualmediaset_calc)

    /************************************************************************************************************/

    val por_pt_grupocadena_calc: Map[Long, Double] = spark.sql(
      s"""SELECT DISTINCT a.cod_cadena, (a.grps_20 / b.sum_grps_20) AS por_pt_grupocadena FROM
         |(SELECT $fctd_share_grps.cod_cadena, sum(grps_20) as grps_20
         |FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.cod_day_part = 1
         |AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini,
         |$dim_agrup_cadenas.fecha_fin) AS a
         |JOIN
         |(SELECT $fctd_share_grps.cod_cadena, sum(grps_20) AS sum_grps_20
         |FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini,
         |$dim_agrup_cadenas.fecha_fin) AS b
         |ON (a.cod_cadena = b.cod_cadena)
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_por_pt_grupocadena_calc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(por_pt_grupocadena_calc)

    /************************************************************************************************************/

    val por_cualgrupocadena_calc: Map[Long, Double] = spark.sql(
      s"""SELECT DISTINCT a.cod_cadena, (a.grps_20 / b.sum_grps_20) AS por_pt_grupocadena FROM
         |(SELECT $fctd_share_grps.cod_cadena, sum(grps_20) as grps_20
         |FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.cod_cualitativo = 1
         |AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini,
         |$dim_agrup_cadenas.fecha_fin) AS a
         |JOIN
         |(SELECT $fctd_share_grps.cod_cadena, sum(grps_20) AS sum_grps_20
         |FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2) AS b
         |ON (a.cod_cadena = b.cod_cadena)
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_por_cualgrupocadena_calc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(por_cualgrupocadena_calc)

    /************************************************************************************************************/

    val cuota_por_grupo_calc: Map[Long, Double] = spark.sql(
      s"""SELECT a.cod_cadena, (a.grps_20 / b.sum_grps_20) AS cuota
         |FROM
         |(SELECT $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin,
         |SUM(grps_20) AS grps_20 FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable} WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS a
         |JOIN
         |(SELECT $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n0, SUM(grps_20) AS sum_grps_20 FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $dim_agrup_cadenas.des_grupo_n0 = "TTV"
         |AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctd_share_grps.cod_cadena,
         |$dim_agrup_cadenas.cod_grupo_n0) AS b
         |ON (a.cod_cadena = b.cod_cadena)
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_cuota_por_grupo_calc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(cuota_por_grupo_calc)

    /************************************************************************************************************/
/*
    val cadenas_mediaset_grupo_n1: Set[Long] = spark.sql(
      s"""SELECT DISTINCT cod_cadena FROM ${dim_agrup_cadenas.getDBTable} WHERE cod_grupo_n1 = 20001 AND cod_cadena IS NOT NULL
       """.stripMargin).map(x => x.getInt(0).toLong).collect.toSet
*/
    val cadenas_mediaset_grupo_n1: Set[Long] = spark.sql(
      s"""SELECT DISTINCT cod_cadena FROM ${dim_agrup_cadenas.getDBTable} WHERE cod_grupo_n1 = 20001 AND cod_cadena IS NOT NULL
       """.stripMargin).map(x => x.getLong(0)).collect.toSet

    val BC_cadenas_mediaset_grupo_n1: Broadcast[Set[Long]] = spark.sparkContext.broadcast(cadenas_mediaset_grupo_n1)

    /************************************************************************************************************/

    val producto_mediaset : List[Long] = spark.sql(
      s"""SELECT DISTINCT cod_producto FROM ${r_sectores_econ.getDBTable} WHERE cod_producto IS NOT NULL
       """.stripMargin).map(x => x.getLong(0)).collect.toList

    val BC_producto_mediaset: Broadcast[List[Long]] = spark.sparkContext.broadcast(producto_mediaset)

    /************************************************************************************************************/

    val grupo_mediaset: List[Long] = spark.sql(
      s"""SELECT DISTINCT cod_grupo FROM ${r_sectores_econ.getDBTable} WHERE cod_grupo IS NOT NULL
       """.stripMargin).map(x => x.getLong(0)).collect.toList

    val BC_grupo_mediaset: Broadcast[List[Long]] = spark.sparkContext.broadcast(grupo_mediaset)

    /************************************************************************************************************/

    val costebase_marca: Map[Long, Double] = spark.sql(
      s"""SELECT $fctd_share_grps.cod_marca, SUM($ordenado.importe_pase) / SUM($fctd_share_grps.grps_20)
         |FROM ${ordenado.getDBTable}, ${fctd_share_grps.getDBTable} WHERE $ordenado.cod_marca
         |= $fctd_share_grps.cod_marca GROUP BY $fctd_share_grps.cod_marca
           """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_costebase_marca: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(costebase_marca)

    val costebase_anunc: Map[Long, Double] = spark.sql(
          s"""SELECT $fctd_share_grps.cod_anunc, SUM($ordenado.importe_pase) / SUM($fctd_share_grps.grps_20)
             |FROM ${ordenado.getDBTable}, ${fctd_share_grps.getDBTable} WHERE $ordenado.cod_anunc
             |= $fctd_share_grps.cod_anunc GROUP BY $fctd_share_grps.cod_anunc
           """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_costebase_anunc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(costebase_anunc)

    val costebase_producto: Map[Long, Double] = spark.sql(
      s"""SELECT $fctd_share_grps.cod_producto, SUM($ordenado.importe_pase) / SUM($fctd_share_grps.grps_20)
         |FROM ${ordenado.getDBTable}, ${fctd_share_grps.getDBTable} WHERE $ordenado.cod_producto
         |= $fctd_share_grps.cod_producto GROUP BY $fctd_share_grps.cod_producto
           """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_costebase_producto: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(costebase_producto)

    val costebase_grupo: Map[Long, Double] = spark.sql(
      s"""SELECT $fctd_share_grps.cod_grupo, SUM($ordenado.importe_pase) / SUM($fctd_share_grps.grps_20)
         |FROM ${ordenado.getDBTable}, ${fctd_share_grps.getDBTable} WHERE $ordenado.cod_grupo
         |= $fctd_share_grps.cod_grupo GROUP BY $fctd_share_grps.cod_grupo
           """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_costebase_grupo: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(costebase_grupo)


    /************************************************************************************************************/

    val cod_posicion_pb2_in_posicionado: List[Long] = spark.sql(
      s"""SELECT cod_posicion_pb2 FROM ${fctd_share_grps.getDBTable}
         |WHERE cod_posicion_pb2 IN (1, 2, 3, 997, 998, 999)
       """.stripMargin).map(x => x.getLong(0)).collect.toList

    val BC_cod_posicion_pb2_in_posicion: Broadcast[List[Long]] = spark.sparkContext.broadcast(cod_posicion_pb2_in_posicionado)

    /************************************************************************************************************/

    val num_cadenas_forta_calc: List[(Long, Long)] = spark.sql(
      s"""
         |SELECT cod_anuncio, COUNT(DISTINCT cod_cadena) AS cads_forta
         |FROM ${fctd_share_grps.getDBTable}
         |WHERE cod_fg_forta = 1
         |GROUP BY cod_anuncio
       """.stripMargin).map(x => (x.getLong(0), x.getLong(1))).collect.toList

    val BC_num_cadenas_forta_calc: Broadcast[List[(Long, Long)]] = spark.sparkContext.broadcast(num_cadenas_forta_calc)

    /************************************************************************************************************/

    // Columna COEF_FORTA
    val coef_forta_param_valor: Int = spark.sql(
      s"""SELECT valor FROM tb_parametros
         |WHERE nom_param = "FORTA" LIMIT 1
       """.stripMargin).map(x => x.getString(0)).collect.toList.head.toInt

    val BC_coef_forta_param_valor: Broadcast[Int] = spark.sparkContext.broadcast(coef_forta_param_valor)

    val indice_coeficiente_forta: Double = spark.sql(
      s"""SELECT indice FROM tb_coeficientes
         |WHERE coeficiente = "FORTA" LIMIT 1
       """.stripMargin).map(x => x.getDouble(0)).collect.toList.head

    val BC_indice_coeficiente_forta: Broadcast[Double] = spark.sparkContext.broadcast(indice_coeficiente_forta)

    /************************************************************************************************************/
/*
    // Columna Coef Cadena
    val coef_cadena_calc: Map[Long, Double] = spark.sql(
      s"""
         |SELECT $dim_agrup_cadenas.cod_cadena, tb_coeficientes.indice
         |FROM tb_coeficientes, ${dim_agrup_cadenas.getDBTable}
         |WHERE tb_coeficientes.des_cadena = $dim_agrup_cadenas.des_grupo_n2
         |AND tb_coeficientes.coeficiente = "CADENA_N2"
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Int].toLong -> x(1).asInstanceOf[Double]).toMap
*/

    // Columna Coef Cadena
    val coef_cadena_calc: Map[Long, Double] = spark.sql(
      s"""
         |SELECT $dim_agrup_cadenas.cod_cadena, tb_coeficientes.indice
         |FROM tb_coeficientes, ${dim_agrup_cadenas.getDBTable}
         |WHERE tb_coeficientes.des_cadena = $dim_agrup_cadenas.des_grupo_n2
         |AND tb_coeficientes.coeficiente = "CADENA_N2"
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_coef_cadena_calc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(coef_cadena_calc)

    /************************************************************************************************************/

    // Columna COEF_ANUNCIANTET

    val coef_anunciante_calc: Map[Long, Double] = spark.sql(
      s"""
         |SELECT cod_cadena, indice
         |FROM tb_coeficientes
         |WHERE coeficiente = "ANUNCIANTE"
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_coef_anunciante_calc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(coef_anunciante_calc)

    /************************************************************************************************************/

    // Columna coef_evento

    val coef_evento_calc = spark.sql(
      s"""SELECT DISTINCT $fctd_share_grps.cod_eventos, $cat_eventos.indice
         |FROM ${fctd_share_grps.getDBTable}, $cat_eventos
         |WHERE $fctd_share_grps.cod_eventos = $cat_eventos.cod_evento AND $fctd_share_grps.cod_eventos IS NOT NULL
         |AND $cat_eventos.cod_evento IS NOT NULL""".stripMargin).collect().map(x => x(0).asInstanceOf[java.lang.Long] -> x(1).asInstanceOf[java.lang.Double]).toMap

    val BC_coef_evento_calc: Broadcast[Map[java.lang.Long, java.lang.Double]] = spark.sparkContext.broadcast(coef_evento_calc)

    /************************************************************************************************************/

    val fctd_importe_pase: DataFrame = getColumn_importe_pase(spark)
    registerTemporalTable(fctd_importe_pase, s"$fctm_share_inv")
    /***********************************************************************************************************/

    val filtro_fctm_share_inv: DataFrame = getTablon_filtro_fctm_share_inv(spark)
    registerTemporalTable(filtro_fctm_share_inv, s"$fctm_share_inv")

    /************************************************************************************************************/

    val tablon_fctm_share_inv: DataFrame = getTablon_fctm_share_inv(spark) // TODO revisar query

    /************************************************************************************************************/

    val fctm_share_inv_col_0_1: DataFrame = getColumn_cod_fg_cadmediaset(tablon_fctm_share_inv, BC_cadenas_mediaset_grupo_n1)

    /************************************************************************************************************/

    val fctm_share_inv_col_0_2: DataFrame = getColumn_cod_fg_prodmediaset(fctm_share_inv_col_0_1, BC_producto_mediaset )

    /************************************************************************************************************/

    val fctm_share_inv_col_0_3: DataFrame = getColumn_cod_fg_grupomediaset(fctm_share_inv_col_0_2, BC_grupo_mediaset)

    /************************************************************************************************************/

    val fctm_share_inv_col_2: DataFrame = getColumn_costebase(spark, fctm_share_inv_col_0_3, BC_costebase_marca, BC_costebase_anunc, BC_costebase_producto, BC_costebase_grupo )

    /************************************************************************************************************/

    val fctm_share_inv_col_3: DataFrame = getColumn_coef_evento( fctm_share_inv_col_2, BC_coef_evento_calc)

    /************************************************************************************************************/

    val fctm_share_inv_col_4: DataFrame = getColumn_por_pt_mediaset( fctm_share_inv_col_3, BC_por_pt_mediaset_calc)

    /************************************************************************************************************/

    val fctm_share_inv_col_5: DataFrame = getColumn_por_pt_grupocadena( fctm_share_inv_col_4, BC_por_pt_grupocadena_calc)

    /************************************************************************************************************/

    val fctm_share_inv_col_6: DataFrame = getColumn_dif_por_primetime( fctm_share_inv_col_5)

    /************************************************************************************************************/

    val fctd_share_inv_col_7: DataFrame = getColumn_coef_pt( fctm_share_inv_col_6, BC_tb_coeficientes_list)

    /************************************************************************************************************/

    val fctd_share_inv_col_8: DataFrame = getColumn_por_cualmediaset( fctd_share_inv_col_7, BC_por_cualmediaset_calc)

    /************************************************************************************************************/

    val fctd_share_inv_col_9: DataFrame = getColumn_por_cualgrupocadena( fctd_share_inv_col_8, BC_por_cualgrupocadena_calc)

    /************************************************************************************************************/

    val fctd_share_inv_col_10: DataFrame = getColumn_dif_por_cualitativos( fctd_share_inv_col_9)

    /************************************************************************************************************/

    val fctd_share_inv_col_11: DataFrame = getColumn_coef_cual( fctd_share_inv_col_10, BC_tb_coeficientes_list)

    /************************************************************************************************************/

    val fctd_share_inv_col_12: DataFrame = getColumn_cod_posicionado( fctd_share_inv_col_11)

    /************************************************************************************************************/

    val fctd_share_inv_col_13: DataFrame = setNomOnColumn(fctd_share_inv_col_12, "cod_posicionado", "nom_posicionado")
    registerTemporalTable(fctd_share_inv_col_13, s"$fctm_share_inv")

    /************************************************************************************************************/

    val por_posmediaset_calc: Map[Long, Double] = spark.sql(
      s"""SELECT in_pos.cod_cadena, (in_pos.sum_campana_grps_in_pos / in_mediaset.sum_campana_grps_in_mediaset)
         |AS por_posmediaset
         |FROM
         |(SELECT cod_cadena, SUM(grps_20) AS sum_campana_grps_in_pos FROM $fctm_share_inv
         |WHERE cod_posicionado = 1 AND $fctm_share_inv.cod_cadena IN
         |(SELECT $dim_agrup_cadenas.cod_cadena FROM ${dim_agrup_cadenas.getDBTable} WHERE cod_grupo_n1 = 20001)
         |GROUP BY cod_cadena) AS in_pos
         |JOIN
         |(SELECT $fctm_share_inv.cod_cadena AS cod_cadena, SUM($fctm_share_inv.grps_20) AS sum_campana_grps_in_mediaset
         |FROM $fctm_share_inv
         |WHERE $fctm_share_inv.cod_cadena IN
         |(SELECT $dim_agrup_cadenas.cod_cadena FROM ${dim_agrup_cadenas.getDBTable} WHERE cod_grupo_n1 = 20001)
         |GROUP BY $fctm_share_inv.cod_cadena) AS in_mediaset
         |ON (in_pos.cod_cadena = in_mediaset.cod_cadena)
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_por_posmediaset_calc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(por_posmediaset_calc)

    val fctd_share_inv_col_14: DataFrame = getColumn_por_posmediaset( fctd_share_inv_col_13, BC_por_posmediaset_calc)
    registerTemporalTable(fctd_share_inv_col_14, s"$fctm_share_inv")

    /************************************************************************************************************/

    val por_posgrupocadena_n2_calc = spark.sql(
      s"""SELECT a.cod_cadena, (a.grps_20 / b.sum_grps_20) AS por_pos_grupocadena
         |FROM
         |(SELECT $fctm_share_inv.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2, $fctm_share_inv.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, SUM(grps_20) AS grps_20
         |FROM $fctm_share_inv, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctm_share_inv.cod_cadena = $dim_agrup_cadenas.cod_cadena AND $fctm_share_inv.cod_posicionado = 1
         |AND $fctm_share_inv.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctm_share_inv.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2, $fctm_share_inv.fecha_dia,
         |$dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS a
         |JOIN
         |(SELECT $fctm_share_inv.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2, SUM(grps_20) AS sum_grps_20
         |FROM $fctm_share_inv, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctm_share_inv.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctm_share_inv.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctm_share_inv.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2) AS b
         |ON (a.cod_cadena = b.cod_cadena)
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_por_posgrupocadena_n2_calc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(por_posgrupocadena_n2_calc)

    val fctd_share_inv_col_15: DataFrame = getColumn_por_posGrupoCadena_n2( fctd_share_inv_col_14, BC_por_posgrupocadena_n2_calc)

    /************************************************************************************************************/

    val fctd_share_inv_col_16: DataFrame = getColumn_dif_por_posicionamiento( fctd_share_inv_col_15)

    /************************************************************************************************************/

    val fctd_share_inv_col_17: DataFrame = getColumn_coef_posic( fctd_share_inv_col_16, BC_tb_coeficientes_list)

    /************************************************************************************************************/

    val fctd_share_inv_col_18: DataFrame = getColumn_cuota_por_grupo( fctd_share_inv_col_17, BC_cuota_por_grupo_calc)

    /************************************************************************************************************/

    val fctd_share_inv_col_19: DataFrame = getColumn_coef_cuota( fctd_share_inv_col_18, BC_tb_coeficientes_list)

    /************************************************************************************************************/

    val fctd_share_inv_col_20: DataFrame = getColumn_coef_cadena( fctd_share_inv_col_19, BC_coef_cadena_calc)

    /************************************************************************************************************/

    val fctd_share_inv_col_21: DataFrame = getColumn_num_cadenas_forta_emitido( fctd_share_inv_col_20, BC_num_cadenas_forta_calc)

    /************************************************************************************************************/

    val fctd_share_inv_col_22: DataFrame = getColumn_coef_forta( fctd_share_inv_col_21, BC_coef_forta_param_valor, BC_indice_coeficiente_forta)

    /************************************************************************************************************/

    val fctd_share_inv_col_23: DataFrame = getColumn_coef_anunciante( fctd_share_inv_col_22, BC_coef_anunciante_calc)

    /************************************************************************************************************/

    val fctd_share_inv_col_24: DataFrame = getColumn_inv_pre(spark, fctd_share_inv_col_23)
    registerTemporalTable(fctd_share_inv_col_24, s"$fctm_share_inv")
    // TODO debug
    //    createTable_tb_inv_agregada(spark)

    // TODO debug
    /*
    val coef_corrector_DF : DataFrame = spark.sql(
      s"""
         |SELECT CAST($dim_agrup_cadenas.cod_cadena AS BIGINT) as cod_cadena1, $tb_inv_agregada.coef_corrector, CAST(CONCAT($tb_inv_agregada.anho, $tb_inv_agregada.mes) AS BIGINT) AS aniomes1
         |--SELECT tb_inv_agregada.coef_corrector
         |FROM ${tb_inv_agregada.getDBTable}, ${dim_agrup_cadenas.getDBTable}
         |WHERE $tb_inv_agregada.gr_n1_infoadex = $dim_agrup_cadenas.des_grupo_n1
         |AND CONCAT($tb_inv_agregada.anho, $tb_inv_agregada.mes) >= CONCAT(YEAR(fecha_ini), MONTH(fecha_ini))
         |AND CONCAT($tb_inv_agregada.anho, $tb_inv_agregada.mes) <= CONCAT(YEAR(fecha_fin), MONTH(fecha_fin))
       """.stripMargin)
    val joinDF = fctd_share_inv_col_24.join(coef_corrector_DF, fctd_share_inv_col_24("cod_cadena") === coef_corrector_DF("cod_cadena1")
      && fctd_share_inv_col_24("aniomes") === coef_corrector_DF("aniomes1"), "left")

    val fctd_share_inv_col_25: DataFrame = joinDF.drop("cod_cadena1","aniomes1")

    val fctd_share_inv_col_26: DataFrame = getColumn_inv_final(fctd_share_inv_col_25)
    */

    val result = fctd_share_inv_col_24

    registerTemporalTable(result, s"$fctm_share_inv")

  }

  /**
    * Siguiente paso en el proceso, guardar el resultado de los calculos y agregaciones como una tabla persistida en Cloudera
    */
  def save(): Unit = {

    persistShareINV()

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
  def createTable_tb_inv_agregada(spark: SparkSession) : Unit = {

    /************************************************************************************************************/
    // TODO Ahora está puesto con fecha_dia pero hay que ver si se va a particionar fctm_share_inversion y hacerlo por el campo fecha_part
    val inv_est_pe_calc: Map[String, Double] = spark.sql(
      s"""
         |SELECT
         |N1_INFOADEX.DES_GRUPO_N1 AS des_grupo_n1,
         |GRPS_N2.SUMAINVN2 AS inv_est_pe
         |-- SUBSTRING(GRPS_N2.fecha_dia, 1, 7) AS fecha_dia
         |FROM
         |(SELECT
         |$dim_agrup_cadenas.DES_GRUPO_N2,
         |sum($fctm_share_inv.inv_pre) AS SUMAINVN2,
         |CAST(CONCAT(SUBSTRING($fctm_share_inv.fecha_dia, 1, 7), '-01') AS TIMESTAMP) as fecha_dia
         |FROM $fctm_share_inv, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctm_share_inv.COD_CADENA = $dim_agrup_cadenas.COD_CADENA
         |AND CAST(CONCAT(SUBSTRING($fctm_share_inv.fecha_dia, 1, 7), '-01') AS TIMESTAMP) >= CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_ini, 1, 7), '-01') AS TIMESTAMP)
         |AND CAST(CONCAT(SUBSTRING($fctm_share_inv.fecha_dia, 1, 7), '-01') AS TIMESTAMP) <= CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_fin, 1, 7), '-31') AS TIMESTAMP)
         |AND DES_GRUPO_N0="TTV"
         |GROUP BY $dim_agrup_cadenas.DES_GRUPO_N2, $fctm_share_inv.fecha_dia) AS GRPS_N2
         |RIGHT JOIN
         |(SELECT DES_GRUPO_N1, DES_GRUPO_N2, CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_ini, 1, 7), '-01') AS TIMESTAMP) as mesanioini,
         |CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_fin, 1, 7), '-31') AS TIMESTAMP) as mesaniofin
         |FROM ${dim_agrup_cadenas.getDBTable}
         |WHERE DES_GRUPO_N0 LIKE "%INFOADEX%") AS N1_INFOADEX
         |WHERE GRPS_N2.DES_GRUPO_N2 = N1_INFOADEX.DES_GRUPO_N2
         |AND GRPS_N2.fecha_dia >= N1_INFOADEX.mesanioini
         |AND GRPS_N2.fecha_dia <= N1_INFOADEX.mesaniofin
       """.stripMargin).collect().map(x => x(0).asInstanceOf[String] -> x(1).asInstanceOf[Double]).toMap
    val BC_inv_est_pe_calc: Broadcast[Map[String, Double]] = spark.sparkContext.broadcast(inv_est_pe_calc)

    val tb_inv_agregada_1: DataFrame = spark.sql(
      s"""
         |SELECT grupo_cadena AS gr_n1_infoadex, inv_est_infoadex, mes, anho
         |FROM $tb_inversion_agregada
       """.stripMargin)

    val tb_inv_agregada_2: DataFrame = getColumn_inv_est_pe(tb_inv_agregada_1, BC_inv_est_pe_calc)

    val tb_inv_agregada_3: DataFrame = getColumn_coef_corrector_inv_agregada(tb_inv_agregada_2)

    val tb_inv_agregada_fecha_carga: DataFrame = setCurrentTimeStamp(tb_inv_agregada_3, timezone)

    persistAsTable(tb_inv_agregada_fecha_carga, tb_inv_agregada)

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
    originDF.withColumn("fecha_carga", from_utc_timestamp(current_timestamp(), timezone))
  }

  def getColumn_inv_est_pe(originDF: DataFrame, BC_inv_est_pe_calc: Broadcast[Map[String, Double]]): DataFrame = {
    originDF.withColumn("inv_est_pe", UDF_inv_est_pe(BC_inv_est_pe_calc)(col("gr_n1_infoadex")))
  }

  def UDF_inv_est_pe(BC_inv_est_pe_calc: Broadcast[Map[String, Double]]): UserDefinedFunction = {

    udf[Double, String]( gr_n1_infoadex => FN_inv_est_pe(BC_inv_est_pe_calc.value, gr_n1_infoadex))

  }

  def FN_inv_est_pe(invEstPe_calc: Map[String, Double], gr_n1_infoadex: String): Double =  {

    invEstPe_calc.getOrElse(gr_n1_infoadex, 0D)

  }

  def getColumn_coef_corrector_inv_agregada(originDF: DataFrame): DataFrame = {

    originDF.withColumn("coef_corrector", UDF_coef_corrector_inv_agregada()(col("inv_est_infoadex"), col("inv_est_pe")))

  }

  def UDF_coef_corrector_inv_agregada(): UserDefinedFunction = {
    udf[Double, Double, Double] ( (inv_est_infoadex, inv_est_pe) => FN_coef_corrector_inv_agregada(inv_est_infoadex, inv_est_pe))
  }

  def FN_coef_corrector_inv_agregada(inv_est_infoadex: Double, inv_est_pe: Double): Double = {

    ((inv_est_infoadex - inv_est_pe) / inv_est_pe) + 1
  }

  /************************************************************************************************************/

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
    fctd_share_grps = new Entity(parametrizationCfg, "fctd_share_grps", true)
    ordenado = new Entity(parametrizationCfg, "ordenado", true)

    tb_inv_agregada = new Entity(parametrizationCfg, "tb_inv_agregada")
    fctm_share_inv = new Entity(parametrizationCfg, "fctm_share_inv")

    // Salesforce
    dim_agrup_cadenas = new Entity(parametrizationCfg, "dim_agrup_cadenas")
    dim_linea_negocio = new Entity(parametrizationCfg, "dim_linea_negocio")
    tb_parametros = new Entity(parametrizationCfg, "tb_parametros")
    tb_configuraciones = new Entity(parametrizationCfg, "tb_configuraciones")
    tb_eventos = new Entity(parametrizationCfg, "tb_eventos")
    r_sectores_econ = new Entity(parametrizationCfg, "r_sectores_econ")
    tb_coeficientes = new Entity(parametrizationCfg, "tb_coeficientes")
    cat_eventos = new Entity(parametrizationCfg, "cat_eventos")
    cat_coeficientes = new Entity(parametrizationCfg, "cat_coeficientes")
    tb_inversion_agregada = new Entity(parametrizationCfg, "tb_inversion_agregada")
  }

  /************************************************************************************************************/

  /**
    * Se registra un DataFrame como una tabla temporal especificandole un nombre
    * @param newDF: DataFrame a registrar como tabla temporal
    * @param tmpTableName: Nombre de la tabla temporal
    */
  def registerTemporalTable(newDF: DataFrame, tmpTableName: String): Unit =  {
    newDF.createOrReplaceTempView(tmpTableName)
  }

  /**
    * Se recupera el estado final del tablón fctm_share_inv almacenado como tabla temporal y se pasa como DataFrame
    * para persistirlo en Cloudera en el siguiente método
    */
  private def persistShareINV(): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    val newDF = spark.sql(s"""SELECT * FROM $fctm_share_inv""")

    val toPartitionDF = newDF.withColumn("fecha_part", expr("substring(fecha_dia, 1, 7)"))

    persistAsTableShareINV(toPartitionDF, fctm_share_inv)

  }

  /**
    * Función que recibe un DataFrame y una entidad que sea una tabla para sobreescribirla si existiera
    * con un formato, compresion, ruta, base de datos y nombre de tabla que recibe por defecto o especificados en
    * el fichero .properties
    * @param newDF: DataFrame que contiene los datos a escribir
    * @param table: Entidad de la que se obtienen los datos especificos de la tabla
    */
  private def persistAsTable (newDF: DataFrame, table: Entity): Unit = {

    newDF.write.mode("overwrite").format(table.getFormat).option("compression", table.getCompression).option("path", table.getLocation).saveAsTable(table.getDBTable)
  }

  /**
    * Guarda un dataframe como tabla con los parametros especificados mediante un fichero .properties
    * @param newDF: DataFrame a persisir como tabla
    * @param table: Nombre de la tabla de salida
    */
  def persistAsTableShareINV(newDF: DataFrame, table: Entity): Unit = {

    newDF.write.partitionBy("fecha_part").mode("overwrite").format(table.getFormat).option("compression", table.getCompression).option("path", table.getLocation).saveAsTable(table.getDBTable)
  }


  /************************************************************************************************************/

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

  /************************************************************************************************************/

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

  def getColumn_cod_fg_prodmediaset(originDF: DataFrame, BC_producto_mediaset: Broadcast[List[Long]]): DataFrame = {
    originDF.withColumn("cod_fg_prodmediaset", UDF_cod_fg_prodmediaset(BC_producto_mediaset)(col("cod_producto")))
  }

  def UDF_cod_fg_prodmediaset(BC_producto_mediaset: Broadcast[List[Long]]): UserDefinedFunction = {

    udf[Int, Long]( cod_producto => FN_cod_fg_prodmediaset(BC_producto_mediaset.value, cod_producto))
  }

  def FN_cod_fg_prodmediaset(productoMediaset_list: List[Long], cod_producto: Long): Int = {

    var result = 0
      if ( productoMediaset_list.contains(cod_producto)) {
        result = 1
      }
    result
  }

  /************************************************************************************************************/

  def getColumn_cod_fg_grupomediaset(originDF: DataFrame, BC_grupo_mediaset: Broadcast[List[Long]]): DataFrame = {
    originDF.withColumn("cod_fg_grupomediaset", UDF_cod_fg_grupomediaset(BC_grupo_mediaset)(col("cod_grupo")))
  }

  def UDF_cod_fg_grupomediaset(BC_grupo_mediaset: Broadcast[List[Long]]): UserDefinedFunction = {
    udf[Int, Long](cod_grupo => FN_cod_fg_grupomediaset(BC_grupo_mediaset.value, cod_grupo))
  }

  def FN_cod_fg_grupomediaset(grupoMediaset_list: List[Long], cod_grupo: Long): Int = {

    var result = 0
      if( grupoMediaset_list.contains(cod_grupo)) {
        result = 1
      }
    result
  }

  /************************************************************************************************************/

  def getColumn_costebase(spark: SparkSession, originDF: DataFrame, BC_costebase_marca: Broadcast[Map[Long, Double]], BC_costebase_anunc: Broadcast[Map[Long, Double]],
                          BC_costebase_producto: Broadcast[Map[Long, Double]], BC_costebase_grupo: Broadcast[Map[Long, Double]]): DataFrame = {

    import spark.implicits._

    originDF.withColumn("costebase", when($"cod_fg_cadmediaset" =!= 1,
      UDF_costebase(BC_costebase_marca, BC_costebase_anunc,
      BC_costebase_producto, BC_costebase_grupo)(col("cod_fg_cadmediaset"), col("cod_fg_campemimediaset"), col("cod_fg_anuncmediaset"),
      col("cod_fg_prodmediaset"), col("cod_fg_grupomediaset"), col("cod_marca"), col("cod_anunc"),
      col("cod_producto"), col("cod_grupo"))).otherwise(when($"cod_fg_cadmediaset" === 1, $"importe_pase" / $"grps_20")))

  }

  def UDF_costebase(BC_costebase_marca: Broadcast[Map[Long, Double]], BC_costebase_anunc: Broadcast[Map[Long, Double]],
                    BC_costebase_producto: Broadcast[Map[Long, Double]], BC_costebase_grupo: Broadcast[Map[Long, Double]]): UserDefinedFunction = {
    udf[Double, Int, Int, Int, Int, Int, Long, Long, Long, Long]((cod_fg_cadmediaset, cod_fg_campemimediaset, cod_fg_anuncmediaset, cod_fg_prodmediaset, cod_fg_grupomediaset, cod_marca, cod_anunc, cod_producto, cod_grupo) => FN_costebase(BC_costebase_marca.value,
      BC_costebase_anunc.value, BC_costebase_producto.value, BC_costebase_grupo.value, cod_fg_cadmediaset, cod_fg_campemimediaset, cod_fg_anuncmediaset, cod_fg_prodmediaset, cod_fg_grupomediaset, cod_marca, cod_anunc, cod_producto, cod_grupo))
  }

  def FN_costebase(costebase_marca: Map[Long, Double], costebase_anunc: Map[Long, Double], costebase_producto: Map[Long, Double], costebase_grupo: Map[Long, Double], cod_fg_cadmediaset: Int,
                   cod_fg_campemimediaset: Int, cod_fg_anuncmediaset: Int, cod_fg_prodmediaset: Int, cod_fg_grupomediaset: Int, cod_marca: Long, cod_anunc: Long, cod_producto: Long, cod_grupo: Long): Double = {

    var result = 0D

    if(cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 1) {
      result = costebase_marca.getOrElse(cod_marca, 0D)
    } else if (cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 0 && cod_fg_anuncmediaset == 1) {
      result = costebase_anunc.getOrElse(cod_anunc, 0D)
    } else if (cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 0 && cod_fg_anuncmediaset == 0 && cod_fg_prodmediaset == 1) {
      result = costebase_producto.getOrElse(cod_producto, 0D)
    } else if (cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 0 && cod_fg_anuncmediaset == 0 && cod_fg_prodmediaset == 0 && cod_fg_grupomediaset == 1) {
      result = costebase_grupo.getOrElse(cod_grupo, 0D)
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_coef_evento( originDF: DataFrame, BC_coef_evento_calc: Broadcast[Map[java.lang.Long, java.lang.Double]]): DataFrame = {

    originDF.withColumn("coef_evento", UDF_coef_evento(BC_coef_evento_calc)(col("cod_eventos")))

  }

  def UDF_coef_evento(BC_coef_evento_calc: Broadcast[Map[java.lang.Long, java.lang.Double]]): UserDefinedFunction = {

    udf[java.lang.Double, java.lang.Long](cod_eventos => FN_coef_evento(BC_coef_evento_calc.value, cod_eventos))
  }

  def FN_coef_evento(coefEvento_calc: Map[java.lang.Long, java.lang.Double], cod_eventos: java.lang.Long): java.lang.Double = {

    coefEvento_calc.getOrElse(cod_eventos, 1D)
  }

  /************************************************************************************************************/

  def getColumn_por_pt_mediaset( originDF: DataFrame, BC_por_pt_mediaset_calc: Broadcast[Map[Long, Double]]): DataFrame = {

    originDF.withColumn("por_pt_mediaset", UDF_por_pt_mediaset(BC_por_pt_mediaset_calc)(col("cod_cadena")))
  }

  def UDF_por_pt_mediaset(BC_por_pt_mediaset_calc: Broadcast[Map[Long, Double]]): UserDefinedFunction = {

    udf[Double, Long]( cod_cadena => FN_por_pt_mediaset(BC_por_pt_mediaset_calc.value, cod_cadena ))
  }

  def FN_por_pt_mediaset(por_pt_mediaset_calc: Map[Long, Double], cod_cadena: Long): Double = {

    por_pt_mediaset_calc.getOrElse(cod_cadena, 0D)

  }

  /************************************************************************************************************/

  def getColumn_por_pt_grupocadena( originDF: DataFrame, BC_por_pt_grupocadena_calc: Broadcast[Map[Long, Double]]): DataFrame = {
    originDF.withColumn("por_pt_grupocadena", UDF_por_pt_grupocadena(BC_por_pt_grupocadena_calc)(col("cod_cadena")))
  }

  def UDF_por_pt_grupocadena(BC_por_pt_grupocadena_calc:  Broadcast[Map[Long, Double]]): UserDefinedFunction = {
    udf[Double, Long ](cod_cadena => FN_por_pt_grupocadena(BC_por_pt_grupocadena_calc.value, cod_cadena))
  }

  def FN_por_pt_grupocadena(porPtGrupoCadena_calc: Map[Long, Double], cod_cadena: Long): Double = {
    porPtGrupoCadena_calc.getOrElse(cod_cadena, 0D)
  }

  /************************************************************************************************************/

  def getColumn_dif_por_primetime( originDF: DataFrame) : DataFrame = {
    originDF.withColumn("dif_por_primetime", UDF_dif_por_primetime()(col("por_pt_grupocadena"), col("por_pt_mediaset")))
  }


  def UDF_dif_por_primetime() : UserDefinedFunction = {

    udf[Double, Double, Double]((por_pt_grupocadena, por_pt_mediaset) => FN_dif_por_primetime(por_pt_grupocadena, por_pt_mediaset))
  }

  def FN_dif_por_primetime(por_pt_grupocadena: Double, por_pt_mediaset: Double): Double = {
    por_pt_grupocadena - por_pt_mediaset
  }

  /************************************************************************************************************/

  def getColumn_coef_pt( originDF: DataFrame, BC_tb_coeficientes_list: Broadcast[List[Coeficientes]]): DataFrame = {
    originDF.withColumn("coef_pt", UDF_coef_pt(BC_tb_coeficientes_list)(col("dif_por_primetime")))
  }

  def UDF_coef_pt(BC_tb_coeficientes_list: Broadcast[List[Coeficientes]]): UserDefinedFunction = {

    udf[Double, Double]( dif_por_primetime  => FN_coef_pt( BC_tb_coeficientes_list.value, dif_por_primetime) )

  }

  def FN_coef_pt(Coeficientes_list: List[Coeficientes], dif_por_primetime: Double ): Double = {

    var result = 1D
    for (elem <- Coeficientes_list) {
      if ( elem.coeficiente.equalsIgnoreCase("PRIMETIME")
        && dif_por_primetime > elem.MIN_RANGO
        && dif_por_primetime <= elem.MAX_RANGO) {
        result = elem.INDICE
      }
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_por_cualmediaset( originDF: DataFrame, BC_por_cualmediaset_calc: Broadcast[Map[Long, Double]]): DataFrame = {

    originDF.withColumn("por_cualmediaset", UDF_por_cualmediaset(BC_por_cualmediaset_calc)(col("cod_cadena")))
  }

  def UDF_por_cualmediaset(BC_por_cualmediaset_calc: Broadcast[Map[Long, Double]]): UserDefinedFunction = {

    udf[Double, Long ]( cod_cadena  => FN_por_cualmediaset(BC_por_cualmediaset_calc.value, cod_cadena))
  }

  def FN_por_cualmediaset(porCualmediaset_calc: Map[Long, Double], cod_cadena: Long): Double = {

    porCualmediaset_calc.getOrElse(cod_cadena, 0D)
  }

  /************************************************************************************************************/

  def getColumn_por_cualgrupocadena( originDF: DataFrame, BC_por_cualgrupocadena_calc: Broadcast[Map[Long, Double]]): DataFrame = {

    originDF.withColumn("por_cualgrupocadena", UDF_por_cualgrupocadena(BC_por_cualgrupocadena_calc)(col("cod_cadena")))
  }

  def UDF_por_cualgrupocadena(BC_por_cualgrupocadena_calc: Broadcast[Map[Long, Double]]): UserDefinedFunction = {

    udf[Double, Long]( cod_cadena => FN_por_cualgrupocadena(BC_por_cualgrupocadena_calc.value, cod_cadena))
  }

  def FN_por_cualgrupocadena(porCualGrupoCadena_calc: Map[Long, Double], cod_cadena: Long): Double = {

    porCualGrupoCadena_calc.getOrElse(cod_cadena, 0D)

  }

  /************************************************************************************************************/

  def getColumn_dif_por_cualitativos( originDF: DataFrame ) : DataFrame = {

    originDF.withColumn("dif_por_cualitativos", UDF_dif_por_cualitativos()(col("por_cualgrupocadena"), col("por_cualmediaset")))
  }

  def UDF_dif_por_cualitativos(): UserDefinedFunction = {

    udf[Double, Double, Double]( (por_cualgrupocadena, por_cualmediaset) => FN_dif_por_cualitativos(por_cualgrupocadena, por_cualmediaset))
  }

  def FN_dif_por_cualitativos(por_cualgrupocadena: Double, por_cualmediaset: Double) : Double = {

    por_cualgrupocadena - por_cualmediaset
  }

  /************************************************************************************************************/

  def getColumn_coef_cual( originDF: DataFrame, BC_tb_coeficientes_list: Broadcast[List[Coeficientes]]) : DataFrame = {

    originDF.withColumn("coef_cual", UDF_coef_cual(BC_tb_coeficientes_list)(col("dif_por_cualitativos")))

  }

  def UDF_coef_cual(BC_tb_coeficientes_list: Broadcast[List[Coeficientes]]): UserDefinedFunction = {

    udf[Double, Double]( dif_por_cualitativos  => FN_coef_cual( BC_tb_coeficientes_list.value, dif_por_cualitativos) )
  }

  def FN_coef_cual( Coeficientes_list: List[Coeficientes], dif_por_cualitativos: Double ): Double = {

    var result = 1D
    for (elem <- Coeficientes_list) {
      if ( elem.coeficiente.equalsIgnoreCase("Cualitativos")
        && dif_por_cualitativos > elem.MIN_RANGO
      && dif_por_cualitativos <= elem.MAX_RANGO) {
        result = elem.INDICE
      }
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_cod_posicionado( originDF: DataFrame): DataFrame = {

    originDF.withColumn("cod_posicionado", UDF_cod_posicionado()(col("cod_cualitativo"), col("cod_posicion_pb2")))
  }

  def UDF_cod_posicionado() : UserDefinedFunction = {
    udf[Long, Long, Long]( (cod_cualitativo, cod_posicion_pb2) => FN_cod_posicionado(cod_cualitativo, cod_posicion_pb2))
  }

  def FN_cod_posicionado(cod_cualitativo: Long, cod_posicion_pb2: Long): Long = {

    var result = 0L

    val posicionados : Set[Long] = Set(1,2,3,997,998,999)

    if(cod_cualitativo != 0 && posicionados.contains(cod_posicion_pb2)){
      result = 1L
    }

    result
  }

  /************************************************************************************************************/

  def getColumn_por_posmediaset( originDF: DataFrame, BC_por_posmediaset_calc: Broadcast[Map[Long, Double]]): DataFrame = {

    originDF.withColumn("por_posmediaset", UDF_por_posmediaset(BC_por_posmediaset_calc)(col("cod_cadena")))
  }

  def UDF_por_posmediaset(BC_por_posmediaset_calc: Broadcast[Map[Long, Double]]): UserDefinedFunction = {
    udf[Double, Long] ( cod_cadena => FN_por_posmediaset(BC_por_posmediaset_calc.value, cod_cadena))
  }

  def FN_por_posmediaset(porPosMediaset_calc: Map[Long, Double], cod_cadena: Long): Double = {

    porPosMediaset_calc.getOrElse(cod_cadena, 0D)

  }

  /************************************************************************************************************/

  def getColumn_por_posGrupoCadena_n2( originDF: DataFrame, BC_por_posgrupocadena_n2_calc: Broadcast[Map[Long, Double]]): DataFrame = {

    originDF.withColumn("por_posGrupoCadenaN2", UDF_por_posGrupoCadenaN2(BC_por_posgrupocadena_n2_calc)(col("cod_cadena")))
  }

  def UDF_por_posGrupoCadenaN2(BC_por_posgrupocadena_n2_calc: Broadcast[Map[Long, Double]]): UserDefinedFunction = {
    udf[Double, Long] ( cod_cadena => FN_por_posGrupoCadenaN2(BC_por_posgrupocadena_n2_calc.value, cod_cadena))
  }

  def FN_por_posGrupoCadenaN2(porPosGrupoCadena_n2_calc: Map[Long, Double], cod_cadena: Long): Double = {

    porPosGrupoCadena_n2_calc.getOrElse(cod_cadena, 0D)

  }

  /************************************************************************************************************/

  def getColumn_dif_por_posicionamiento( originDF: DataFrame): DataFrame = {

    originDF.withColumn("dif_por_posicionamiento", UDF_dif_por_posicionamiento()(col("por_posGrupoCadenaN2"), col("por_posmediaset")))
  }

  def UDF_dif_por_posicionamiento(): UserDefinedFunction = {
    udf[Double, Double, Double] ( (por_posGrupoCadenaN2, por_posmediaset) => FN_dif_por_posicionamiento(por_posGrupoCadenaN2, por_posmediaset))
  }

  def FN_dif_por_posicionamiento(por_posGrupoCadenaN2: Double, por_posmediaset: Double): Double = {

    por_posGrupoCadenaN2 - por_posmediaset
  }

  /************************************************************************************************************/

  def getColumn_coef_posic( originDF: DataFrame,  BC_tb_coeficientes_list: Broadcast[List[Coeficientes]]): DataFrame = {

    originDF.withColumn("coef_posic", UDF_coef_posic(BC_tb_coeficientes_list)(col("dif_por_posicionamiento")))

  }

  def UDF_coef_posic(BC_tb_coeficientes_list: Broadcast[List[Coeficientes]]): UserDefinedFunction = {

    udf[Double, Double]( dif_por_posicionamiento  => FN_coef_posic( BC_tb_coeficientes_list.value, dif_por_posicionamiento) )

  }

  def FN_coef_posic(Coeficientes_list: List[Coeficientes], dif_por_posicionamiento: Double): Double = {

    var result = 1D
    for (elem <- Coeficientes_list) {
      if ( elem.coeficiente.equalsIgnoreCase("Posicionado")
        && dif_por_posicionamiento > elem.MIN_RANGO
        && dif_por_posicionamiento <= elem.MAX_RANGO) {
        result = elem.INDICE
      }
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_cuota_por_grupo( originDF: DataFrame, BC_cuota_por_grupo_calc: Broadcast[Map[Long, Double]]): DataFrame = {
    originDF.withColumn("cuota_por_grupo", UDF_cuota_por_grupo(BC_cuota_por_grupo_calc)(col("cod_cadena")))
  }

  def UDF_cuota_por_grupo(BC_cuota_por_grupo_calc: Broadcast[Map[Long, Double]]): UserDefinedFunction = {
    udf[Double, Long](cod_cadena => FN_cuota_por_grupo(BC_cuota_por_grupo_calc.value, cod_cadena))
  }

  def FN_cuota_por_grupo(cuotaPorGrupo_calc: Map[Long, Double], cod_cadena: Long): Double = {
    cuotaPorGrupo_calc.getOrElse(cod_cadena, 0D)
  }

  /************************************************************************************************************/

  def getColumn_coef_cuota( originDF: DataFrame, BC_tb_coeficientes_list: Broadcast[List[Coeficientes]]): DataFrame = {

    originDF.withColumn("coef_cuota", UDF_coef_cuota(BC_tb_coeficientes_list)(col("cuota_por_grupo")))

  }

  def UDF_coef_cuota(BC_tb_coeficientes_list: Broadcast[List[Coeficientes]]): UserDefinedFunction = {

    udf[Double, Double]( cuota_por_grupo  => FN_coef_cuota( BC_tb_coeficientes_list.value, cuota_por_grupo) )

  }

  def FN_coef_cuota(Coeficientes_list: List[Coeficientes], cuota_por_grupo: Double): Double = {

    var result = 1D
    for (elem <- Coeficientes_list) {
      if ( elem.coeficiente.equalsIgnoreCase("Cuota")
        && cuota_por_grupo > elem.MIN_RANGO
        && cuota_por_grupo <= elem.MAX_RANGO) {
        result = elem.INDICE
      }
    }
    result
  }

  /************************************************************************************************************/


  def getColumn_coef_cadena( originDF: DataFrame, BC_coef_cadena_cacl: Broadcast[Map[Long, Double]]): DataFrame = {

    originDF.withColumn("coef_cadena", UDF_coef_cadena(BC_coef_cadena_cacl)(col("cod_cadena")))
  }

  def UDF_coef_cadena(BC_coef_cadena_cacl: Broadcast[Map[Long, Double]]): UserDefinedFunction = {

    udf[Double, Long]( cod_cadena => FN_coef_cadena(BC_coef_cadena_cacl.value, cod_cadena))
  }

  def FN_coef_cadena(coefCadena_cacl: Map[Long, Double], cod_cadena: Long): Double = {

    coefCadena_cacl.getOrElse(cod_cadena, 0D)

  }

  /************************************************************************************************************/
  /**
    * Añade la columna "num_cadenas_forta_emitido" al DataFrame de origen
    * @param BC_num_cadenas_forta_calc: Lista que contiene el numero de cadenas forta en las que ha sido emitido un determinado anuncio
    * @return
    */
  def getColumn_num_cadenas_forta_emitido( originDF: DataFrame, BC_num_cadenas_forta_calc: Broadcast[List[(Long, Long)]]): DataFrame = {

    originDF.withColumn("num_cadenas_forta_emitido", UDF_num_cadenas_forta_emitido(BC_num_cadenas_forta_calc)(col("cod_anuncio")))
  }

  def UDF_num_cadenas_forta_emitido(BC_num_cadenas_forta_calc: Broadcast[List[(Long, Long)]]): UserDefinedFunction = {

    udf[Long, Long](cod_anuncio => FN_num_cadenas_forta_emitido(BC_num_cadenas_forta_calc.value, cod_anuncio))
  }

  def FN_num_cadenas_forta_emitido(numCadenasFortaEmitidas: List[(Long, Long)], cod_anuncio: Long): Long = {

    var result = 0L
    for (x <- numCadenasFortaEmitidas)
      if (cod_anuncio == x._1) {
        result = x._2
      }
    result
  }

  /************************************************************************************************************/

  def getColumn_coef_forta( originDF: DataFrame, BC_coef_forta_param_valor: Broadcast[Int], BC_indice_coeficiente_forta: Broadcast[Double]): DataFrame = {
    originDF.withColumn("coef_forta", UDF_coef_forta(BC_coef_forta_param_valor, BC_indice_coeficiente_forta)(col("cod_fg_autonomica"), col("cod_fg_forta"), col("num_cadenas_forta_emitido")))
  }

  def UDF_coef_forta(BC_coef_forta_param_valor: Broadcast[Int], BC_indice_coeficiente_forta: Broadcast[Double]): UserDefinedFunction = {
    udf[Double, Int, Int, Long]( (cod_fg_autonomica, cod_fg_forta, num_cadenas_forta_emitido) => FN_coef_forta(BC_coef_forta_param_valor.value, BC_indice_coeficiente_forta.value, cod_fg_autonomica, cod_fg_forta, num_cadenas_forta_emitido))
  }

  def FN_coef_forta(coefFortaParam_valor: Int, indiceCoeficiente_forta: Double, cod_fg_autonomica: Int, cod_fg_forta: Int, num_cadenas_forta_emitido: Long): Double = {

    var result = 1D
    if (cod_fg_autonomica == 1
    && cod_fg_forta == 1
    && num_cadenas_forta_emitido >= coefFortaParam_valor ) {
      result = indiceCoeficiente_forta
    }
    result

  }

  /************************************************************************************************************/

  def getColumn_coef_anunciante(originDF: DataFrame, BC_coef_anunciante_calc: Broadcast[Map[Long, Double]]) : DataFrame = {
    originDF.withColumn("coef_anunciante", UDF_coef_anunciante(BC_coef_anunciante_calc)(col("cod_anunc")))
  }

  def UDF_coef_anunciante(BC_coef_anunciante_calc: Broadcast[Map[Long, Double]]): UserDefinedFunction = {
    udf[Double, Long]( cod_anunc => FN_coef_anunciante(BC_coef_anunciante_calc.value, cod_anunc))
  }

  def FN_coef_anunciante(coefAnunciante_calc: Map[Long, Double], cod_anunc: Long): Double = {

    coefAnunciante_calc.getOrElse(cod_anunc, 1D)

  }

  /************************************************************************************************************/

  def getColumn_inv_pre(spark: SparkSession, originDF: DataFrame) : DataFrame = {
    import spark.implicits._

    originDF.withColumn("inv_pre", when($"cod_fg_cadmediaset" === 0,
      ($"costebase" / $"grps_20") * $"coef_evento" * $"coef_pt" * $"coef_cual" * $"coef_posic" * $"coef_cuota" * $"coef_cadena" * $"coef_forta" * $"coef_anunciante")
      .otherwise($"costebase" / $"grps_20"))

  }

  /************************************************************************************************************/

  // UDF's INV_FINAL ----------------------------------------------------------------------------------------------

  def getColumn_inv_final( originDF: DataFrame): DataFrame = {
    originDF.withColumn("inv_final", UDF_inv_final()(col("coef_corrector"), col("inv_pre")))
  }

  def UDF_inv_final(): UserDefinedFunction = {
    udf[Double, Double, Double]( (coef_corrector, inv_pre) => FN_inv_final(coef_corrector, inv_pre))
  }

  def FN_inv_final(coef_corrector: Double, inv_pre: Double): Double = {

    coef_corrector * inv_pre
  }

  /************************************************************************************************************/

  // Case class con las diferentes tablas que se recuperan de SalesForce. Se especifican sus columnas y sus tipos de datos.

  case class Coeficientes(Anyo : String, coeficiente: String, des_cadena: String, cod_cadena: java.lang.Long, fecha_act: java.lang.Long, fecha_fin: java.lang.Long, fecha_ini: java.lang.Long, flag: java.lang.Long, INDICE: java.lang.Double, MAX_RANGO: java.lang.Double,
                          Mes: String, MIN_RANGO: java.lang.Double)

  /************************************************************************************************************/

  /**
    * Query inicial que recupera todas las columnas de la tabla fctd_share_grps y la columna importe_pase de la tabla ordenado
    * @param spark: Instanciación del objeto spark para poder acceder a sus métodos de SQL
    * @return: Devuelve el primer DataFrame a partir del cual se iran agregando más columnas
    */
  def getColumn_importe_pase(spark: SparkSession): DataFrame = {
    spark.sql(
      s"""SELECT $fctd_share_grps.*, $ordenado.importe_pase AS importe_pase
         |FROM ${fctd_share_grps.getDBTable} LEFT JOIN ${ordenado.getDBTable}
         |ON ($ordenado.cod_target_venta = $fctd_share_grps.cod_target_compra AND $fctd_share_grps.cod_cadena = $ordenado.cod_canal
         |AND $fctd_share_grps.cod_marca = $ordenado.cod_marca AND $fctd_share_grps.dia_progrmd = $ordenado.dia_progrmd
         |AND $fctd_share_grps.cod_day_part = $ordenado.cod_day_part)
       """.stripMargin)

  }

  /**
    * Query que selecciona diferentes campos de la tabla temporal con los datos de fctd_share_grps y la columna importe_pase y además se
    * realizan unos filtros para acotar los datos
    * @param spark: Instanciación del objeto spark para poder acceder a sus métodos de SQL
    * @return
    */
  def getTablon_filtro_fctm_share_inv(spark: SparkSession): DataFrame = {

  spark.sql(s"""
                            SELECT
                               fecha_dia,
                               dia_progrmd,
                               aniomes,
                               grps_20,
                               importe_pase,
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
                               COD_FG_ANUN_LOCAL,
                               NOM_FG_ANUN_LOCAL,
                               COD_FG_TELEVENTA,
                               NOM_FG_TELEVENTA,
                               COD_FG_PUB_COMPARTIDA,
                               NOM_FG_PUB_COMPARTIDA,
                               COD_FG_AUTOPROMO,
                               NOM_FG_AUTOPROMO,
                               cant_pases,
                               cod_tp_lineanegocio_km,
                               nom_tp_lineanegocio_km,
                               cod_tp_categr_km,
                               nom_tp_categr_km,
                               cod_fg_autonomica,
                               cod_fg_forta,
                               cod_fg_boing,
                               cod_identif_franja,
                               nom_identif_franja,
                               cod_target_compra,
                               cod_fg_filtrado,
                               cod_fg_campemimediaset,
                               cod_tp_computo_km,
                               nom_tp_computo_km,
                               cod_eventos,
                               nom_eventos,
                               cod_fg_anuncmediaset
                             FROM $fctm_share_inv
                              WHERE cod_target_compra = cod_target
                              AND substr(dia_progrmd, 0, 10) >= "1999-01-01" AND substr(dia_progrmd, 0, 10) <= "2099-01-31"
    """)
    // TODO añadir filtro de fecha con los nuevos parametros
}

  /**
    * Query de agregación sobre los campos de la tabla temporal filtrada fctm_share_inv
    * @param spark: Instanciación del objeto spark para poder acceder a sus métodos de SQL
    * @return
    */
  def getTablon_fctm_share_inv(spark: SparkSession): DataFrame = {

    spark.sql(s"""
                            SELECT
                               fecha_dia,
                               dia_progrmd,
                               aniomes,
                               SUM(grps_20) AS grps_20,
                               importe_pase,
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
                               COD_FG_ANUN_LOCAL,
                               NOM_FG_ANUN_LOCAL,
                               COD_FG_TELEVENTA,
                               NOM_FG_TELEVENTA,
                               COD_FG_PUB_COMPARTIDA,
                               NOM_FG_PUB_COMPARTIDA,
                               COD_FG_AUTOPROMO,
                               NOM_FG_AUTOPROMO,
                               cant_pases,
                               cod_tp_lineanegocio_km,
                               nom_tp_lineanegocio_km,
                               cod_tp_categr_km,
                               nom_tp_categr_km,
                               MAX(cod_fg_autonomica) AS cod_fg_autonomica, --Añadir función MAX ver doc
                               MAX(cod_fg_forta) AS cod_fg_forta,
                               MAX(cod_fg_boing) AS cod_fg_boing,
                               cod_identif_franja,
                               nom_identif_franja,
                               cod_target_compra,
                               MAX(cod_fg_filtrado) AS cod_fg_filtrado,
                               MAX(cod_fg_campemimediaset) AS cod_fg_campemimediaset,
                               cod_tp_computo_km,
                               nom_tp_computo_km,
                               cod_eventos,
                               nom_eventos,
                               MAX(cod_fg_anuncmediaset) AS cod_fg_anuncmediaset
                               FROM $fctm_share_inv
                               GROUP BY
                               fecha_dia,
                               dia_progrmd,
                               aniomes,
                               importe_pase,
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
                                COD_FG_ANUN_LOCAL,
                                NOM_FG_ANUN_LOCAL,
                                COD_FG_TELEVENTA,
                                NOM_FG_TELEVENTA,
                                COD_FG_PUB_COMPARTIDA,
                                NOM_FG_PUB_COMPARTIDA,
                                COD_FG_AUTOPROMO,
                                NOM_FG_AUTOPROMO,
                                cant_pases,
                                cod_tp_lineanegocio_km,
                                nom_tp_lineanegocio_km,
                                cod_tp_categr_km,
                                nom_tp_categr_km,
                                cod_identif_franja,
                                nom_identif_franja,
                                cod_target_compra,
                                cod_tp_computo_km,
                                nom_tp_computo_km,
                                cod_eventos,
                                nom_eventos
                             """)
  }

}
