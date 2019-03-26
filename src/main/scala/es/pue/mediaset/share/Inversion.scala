package es.pue.mediaset.share

import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map
import org.apache.spark.sql.UDFRegistration
import org.joda.time.DateTime

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
  private var rel_camp_trgt_may : Entity = _
  private var fcts_mercado_lineal : Entity = _
  private var ordenado : Entity = _
  private var dim_conf_canales : Entity = _


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

    //val dim_conf_canales_DF: DataFrame = salesforce.get_tb_dim_conf_canales(spark, salesforce.query_dim_conf_canales)
    //registerTemporalTable(dim_conf_canales_DF, s"$dim_conf_canales")

    /************************************************************************************************************/

    // Collecting data from SF
    // Creating broadcast objects to work on the nodes
    // TODO Revisar si es necesario recorrer la tabla entera o se puede optimizar se está utilizando en 4 casos
    val tb_coeficientes_list = spark.sql(s"SELECT * FROM $tb_coeficientes").as[Coeficientes].collect().toList
    val BC_tb_coeficientes_list = spark.sparkContext.broadcast(tb_coeficientes_list)

    /************************************************************************************************************/

    val por_pt_mediaset_calc: Map[(Long,Long,Long,Long), Double] = spark.sql(
      s"""SELECT in_pt.aniomes, in_pt.cod_anuncio, in_pt.cod_anunc, in_pt.cod_cadena, (in_pt.sum_campana_grps_in_pt / in_mediaset.sum_campana_grps_in_mediaset) AS por_pt_mediaset
         |FROM (SELECT aniomes, cod_anuncio, cod_anunc, cod_cadena, SUM(grps_20_totales) AS sum_campana_grps_in_pt FROM ${fctd_share_grps.getDBTable} WHERE des_day_part = "PT"
         | AND $fctd_share_grps.cod_cadena IN (SELECT $dim_agrup_cadenas.cod_cadena FROM ${dim_agrup_cadenas.getDBTable} WHERE
         | cod_grupo_n1 = 20001)
         | AND cod_day_part = 1
         | AND nom_sect_geog = "PYB"
         | AND $fctd_share_grps.cod_fg_filtrado = 0
         | AND cod_target_compra = cod_target
         | GROUP BY aniomes,cod_anuncio,cod_anunc,cod_cadena) AS in_pt
         | JOIN
         | (SELECT aniomes, cod_anuncio, cod_anunc, $fctd_share_grps.cod_cadena AS cod_cadena, SUM($fctd_share_grps.grps_20_totales) AS sum_campana_grps_in_mediaset
         | FROM ${fctd_share_grps.getDBTable} WHERE $fctd_share_grps.cod_cadena IN (SELECT $dim_agrup_cadenas.cod_cadena FROM
         | ${dim_agrup_cadenas.getDBTable} WHERE cod_grupo_n1 = 20001)
         | AND $fctd_share_grps.nom_sect_geog = "PYB"
         | AND $fctd_share_grps.cod_fg_filtrado = 0
         | AND cod_target_compra = cod_target
         | GROUP BY $fctd_share_grps.aniomes, $fctd_share_grps.cod_anuncio, $fctd_share_grps.cod_anunc, $fctd_share_grps.cod_cadena) AS in_mediaset
         | ON in_pt.cod_cadena = in_mediaset.cod_cadena
         | AND in_pt.aniomes = in_mediaset.aniomes
         | AND in_pt.cod_anunc = in_mediaset.cod_anunc
         | AND in_pt.cod_anuncio = in_mediaset.cod_anuncio
       """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long],x(2).asInstanceOf[Long],x(3).asInstanceOf[Long]) -> x(4).asInstanceOf[Double]).toMap

    val BC_por_pt_mediaset_calc: Broadcast[Map[(Long,Long,Long,Long), Double]] = spark.sparkContext.broadcast(por_pt_mediaset_calc)

    /************************************************************************************************************/

    val por_cualmediaset_calc: Map[(Long,Long,Long,Double), Double] = spark.sql(
      s"""SELECT a.cod_cadena,a.fecha_dia,a.cod_grupo_n1,b.grps_20_totales,(a.grps_20_totales/b.grps_20_totales) AS por_cual_mediaset FROM
         |(SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, SUM(grps_20_totales)
         |AS grps_20_totales FROM ${fctd_share_grps.getDBTable},${dim_agrup_cadenas.getDBTable} WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena AND $fctd_share_grps.cod_cualitativo = 1 AND
         |$fctd_share_grps.cod_sect_geog = "PYB" AND $fctd_share_grps.cod_fg_filtrado = 0 AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target AND $dim_agrup_cadenas.cod_grupo_n1 = 20001 AND
         |$fctd_share_grps.fecha_dia BETWEEN ($dim_agrup_cadenas.fecha_ini;$dim_agrup_cadenas.fecha_fin) GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini,$dim_agrup_cadenas.fecha_fin) AS a
         |JOIN (SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $fctd_share_grps.fecha_dia,$fctd_share_grps.cod_grupo_n1,$fctd_share_grps.fecha_ini,$fctd_share_grps.fecha_fin, SUM(grps_20_totales) AS sum_grps_20_totales
         |FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable} WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena AND $fctd_share_grps.fecha_dia BETWEEN ($dim_agrup_cadenas.fecha_ini;$dim_agrup_cadenas.fecha_fin)
         |AND $fctd_share_grps.cod_sect_geog = "PYB" AND $fctd_share_grps.cod_fg_filtrado = 0 AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target AND $dim_agrup_cadenas.cod_grupo_n1 = 20001 GROUP BY $fctd_share_grps.cod_cadena,
         |$dim_agrup_cadenas.cod_grupo_n2) AS b
         | ON a.cod_cadena = b.cod_cadena
         | AND a.aniomes = b.aniomes
         | AND a.cod_grupo_n1 = b.cod_grupo_n1
         | AND a.fecha_dia = b.fecha_dia
         | AND a.fecha_ini = b.fecha_ini
         | AND a.fecha_fin = b.fecha_fin
         | """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long],x(2).asInstanceOf[Long],x(3).asInstanceOf[Double]) -> x(4).asInstanceOf[Double]).toMap

    val BC_por_cualmediaset_calc: Broadcast[Map[(Long,Long,Long,Double), Double]] = spark.sparkContext.broadcast(por_cualmediaset_calc)

    /************************************************************************************************************/

    val por_pt_grupocadena_calc: Map[(Long,Long,Long,Long), Double] = spark.sql(
      s"""SELECT DISTINCT a.cod_cadena, a.aniomes,a.cod_grupo_n1,a.cod_anuncio,(a.grps_20_totales / b.sum_grps_20_totales) AS por_pt_grupocadena FROM
         |(SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $fctd_share_grps.cod_anuncio, $fctd_share_grps.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, sum(grps_20_totales) as grps_20_totales
         |FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.cod_day_part = 1
         |AND $fctd_share_grps.nom_sect_geo = "PYB"
         |AND $fctd_share_grps.cod_fg_filtrado = 0
         |AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
         |AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini,
         |$dim_agrup_cadenas.fecha_fin, $fctd_share_grps.aniomes) AS a
         |JOIN
         |(SELECT $fctd_share_grps.cod_cadena,$fctd_share_grps.aniomes, $fctd_share_grps.cod_anuncio, $fctd_share_grps.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, sum(grps_20_totales) AS sum_grps_20_totales
         |FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.nom_sect_geo = "PYB"
         |AND $fctd_share_grps.cod_fg_filtrado = 0
         |AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
         |AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini,
         |$dim_agrup_cadenas.fecha_fin, $fctd_share_grps.aniomes) AS b
         |ON (a.cod_cadena = b.cod_cadena)
         | AND a.aniomes = b.aniomes
         | AND a.cod_grupo_n1 = b.cod_grupo_n1
         | AND a.fecha_dia = b.fecha_dia
         | AND a.fecha_ini = b.fecha_ini
         | AND a.fecha_fin = b.fecha_fin
       """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long],x(2).asInstanceOf[Long],x(3).asInstanceOf[Long]) -> x(4).asInstanceOf[Double]).toMap

    val BC_por_pt_grupocadena_calc: Broadcast[Map[(Long,Long,Long,Long), Double]] = spark.sparkContext.broadcast(por_pt_grupocadena_calc)

    /************************************************************************************************************/

    val por_cualgrupocadena_calc: Map[(Long,Long,Long,Double), Double] = spark.sql(
      s"""SELECT a.cod_cadena,a.fecha_dia,a.cod_grupo_n2,b.grps_20_totales,(a.grps_20_totales/b.grps_20_totales) AS POR_cual_grupoN2
         |FROM
         |(SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n2, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin,
         |SUM(grps_20_totales) AS grps_20_totales
         |FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.cod_cualitativo = 1
         |AND  $fctd_share_grps.cod_sect_geog = "PYB"
         |AND  $fctd_share_grps.cod_target_compra =  $fctd_share_grps.cod_target
         |AND  $fctd_share_grps.fecha_dia BETWEEN ($dim_agrup_cadenas.fecha_ini;$dim_agrup_cadenas.fecha_fin)
         |GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2,$fctd_share_grps.fecha_dia,$dim_agrup_cadenas.fecha_ini,$dim_agrup_cadenas.fecha_fin)
         |AS a JOIN
         |(SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $fctd_share_grps.fecha_dia,$fctd_share_grps.cod_grupo_n2,$fctd_share_grps.fecha_ini,$fctd_share_grps.fecha_fin,
         |SUM(grps_20_totales) AS sum_grps_20_totales
         |FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
         | WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND  $fctd_share_grps.fecha_dia BETWEEN ($dim_agrup_cadenas.fecha_ini;$dim_agrup_cadenas.fecha_fin)
         |AND  $fctd_share_grps.cod_sect_geog = "PYB"
         |AND  $fctd_share_grps.cod_fg_filtrado = 0
         |AND  $fctd_share_grps.cod_target_compra =  $fctd_share_grps.cod_target
         |GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2) AS b
         | ON a.cod_cadena = b.cod_cadena
         | AND a.aniomes = b.aniomes
         | AND a.cod_grupo_n2 = b.cod_grupo_n2
         | AND a.fecha_dia = b.fecha_dia
         | AND a.fecha_ini = b.fecha_ini
         | AND a.fecha_fin = b.fecha_fin
       """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long],x(2).asInstanceOf[Long],x(3).asInstanceOf[Double]) -> x(4).asInstanceOf[Double]).toMap

    val BC_por_cualgrupocadena_calc: Broadcast[Map[(Long,Long,Long,Double), Double]] = spark.sparkContext.broadcast(por_cualgrupocadena_calc)

    /************************************************************************************************************/

    val cuota_por_grupo_calc: Map[(Long,Long,Long,Long,Long), Double] = spark.sql(
      s"""SELECT a.aniomes,a.cod_grupo_n1,a.fecha_dia,a.fecha_ini,a.fecha_fin,(a.grps_20_totales / b.sum_grps_20_totales) AS cuota
         |FROM SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin,
         |SUM(grps_20_totales) AS grps_20_totales FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable} WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS a
         |JOIN
         |(SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n0, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin,  SUM(grps_20_totales) AS sum_grps_20_totales FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $dim_agrup_cadenas.cod_grupo_n0 = 10001
         |AND $fctd_share_grps.cod_sect_geog = "PYB"
         |AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
         |GROUP BY $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes
         |$dim_agrup_cadenas.cod_grupo_n0) AS b
         |ON (a.cod_cadena = b.cod_cadena)
         | AND a.aniomes = b.aniomes
         | AND a.cod_grupo_n1 = b.cod_grupo_n0
         | AND a.fecha_dia = b.fecha.dia
         | AND a.fecha_ini = b.fecha_ini
         | AND a.fecha_fin = b.fecha_fin
       """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long],x(2).asInstanceOf[Long],x(3).asInstanceOf[Long],x(4).asInstanceOf[Long]) -> x(5).asInstanceOf[Double]).toMap

    val BC_cuota_por_grupo_calc: Broadcast[Map[(Long,Long,Long,Long,Long), Double]] = spark.sparkContext.broadcast(cuota_por_grupo_calc)

    /************************************************************************************************************/
    /*
        val cadenas_mediaset_grupo_n1: Set[Long] = spark.sql(
          s"""SELECT DISTINCT cod_cadena FROM ${dim_agrup_cadenas.getDBTable} WHERE cod_grupo_n1 = 20001 AND cod_cadena IS NOT NULL
           """.stripMargin).map(x => x.getInt(0).toLong).collect.toSet
    */
    val cadenas_mediaset_grupo_n1: Set[Long] = spark.sql(
      s"""SELECT DISTINCT cod_cadena FROM ${dim_agrup_cadenas.getDBTable}  WHERE cod_grupo_n1 = 20001 AND cod_cadena IS NOT NULL AND $fctd_share_grps.aniomes >= CONCAT(YEAR(fecha_ini), MONTH(fecha_ini)) AND $fctd_share_grps.aniomes <= CONCAT(YEAR(fecha_fin), MONTH(fecha_fin))
       """.stripMargin).map(x => x.getLong(0)).collect.toSet

    val BC_cadenas_mediaset_grupo_n1: Broadcast[Set[Long]] = spark.sparkContext.broadcast(cadenas_mediaset_grupo_n1)

    /************************************************************************************************************/

    val producto_mediaset : List[Long] = spark.sql(
      s"""SELECT  sh.cod_producto FROM ${dim_agrup_cadenas.getDBTable} AS ag ${fctd_share_grps.getDBTable} AS sh
         | WHERE cod_producto IS NOT NULL
         | AND ag.cod_cadena = sh.cod_cadena
         | AND cod_grupo_n1__c = 20001
         | AND sh.aniomes >= concat(year(ag.fechaini);month(ag.fechaini)
         | AND sh.aniomes <= concat(year(ag.fechafin);month(ag.fechafin)
       """.stripMargin).map(x => x.getLong(0)).collect.toList

    val BC_producto_mediaset: Broadcast[List[Long]] = spark.sparkContext.broadcast(producto_mediaset)

    /************************************************************************************************************/

    val grupo_mediaset: List[Long] = spark.sql(
      s"""SELECT  sh.cod_grupo FROM ${dim_agrup_cadenas.getDBTable} AS ag ${fctd_share_grps.getDBTable} AS sh
         | WHERE cod_producto IS NOT NULL
         | AND ag.cod_cadena = sh.cod_cadena
         | AND cod_grupo_n1__c = 20001
         | AND sh.aniomes >= concat(year(ag.fechaini);month(ag.fechaini)
         | AND sh.aniomes <= concat(year(ag.fechafin);month(ag.fechafin)
       """.stripMargin).map(x => x.getLong(0)).collect.toList

    val BC_grupo_mediaset: Broadcast[List[Long]] = spark.sparkContext.broadcast(grupo_mediaset)

    /************************************************************************************************************/

    val importe_pase: Map[(Long,String,Long,Long,Long,Long,Long,Double), Double] = spark.sql(
      s"""SELECT share.aniomes, share.nom_cadena, share.cod_target_compra, share.cod_anunc, share.cod_anunciante_subsidiario, share.cod_anuncio, share.cod_marca, share.grps, IF(sum(ord.importe_pase) IS NULL, 0, sum(ord.importe_pase)) importe
         |FROM
         |(SELECT sh.dia_progrmd,sh.aniomes, sh.nom_cadena, sh.cod_cadena, sh.cod_target_compra, sh.cod_anunc, sh.cod_anunciante_subsidiario, sh.cod_anuncio, sh.cod_marca, SUM(sh.grps_20_totales) grps
         |from ${fctd_share_grps.getDBTable} AS sh
         |where sh.nom_sect_geog in ("PYB")
         |and sh.cod_fg_filtrado = 0
         |and sh.cod_target=sh.cod_target_compra
         |GROUP BY sh.dia_progrmd, sh.aniomes, sh.nom_Cadena, sh.cod_cadena, sh.cod_target_compra, sh.cod_anunc, sh.cod_anunciante_subsidiario, sh.cod_anuncio, sh.cod_marca
         |) share
         |left join
         |(SELECT orden.dia_progrmd, orden.cod_marca, orden.nom_marca, orden.cod_anunc, orden.nom_anunc,orden.cod_grptarget_venta_gen, orden.nom_grptarget_venta_gen, orden.cod_canal,orden.nom_canal, orden.cod_conexion, orden.nom_conexion, orden.cod_anuncio, orden.nom_anuncio, orden.importe_pase, can.cod_cadena, can.nom_cadena
         |FROM ${ordenado.getDBTable} orden left join ${dim_conf_canales.getDBTable} can
         |on orden.cod_conexion = can.cod_conexion
         |WHERE orden.cod_conexion <> 81
         |AND fg_prod_placement <> 1
         |AND cod_est_linea IN (2,3,4)) ord
         |ON ord.cod_grptarget_venta_gen = share.cod_target_compra
         |AND  share.cod_marca = ord.cod_marca
         |AND  share.cod_anuncio = ord.cod_anuncio
         |AND share.cod_anunc = ord.cod_anunc
         |AND share.dia_progrmd = ord.dia_progrmd
         |and ord.cod_cadena = share.cod_cadena
         |group by share.aniomes, share.nom_cadena,share.cod_target_compra, share.cod_anunc, share.cod_anunciante_subsidiario, share.cod_anuncio, share.cod_marca, share.grps
    """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[String],x(2).asInstanceOf[Long],x(3).asInstanceOf[Long],x(4).asInstanceOf[Long],x(5).asInstanceOf[Long],x(6).asInstanceOf[Long],x(7).asInstanceOf[Double]) -> x(8).asInstanceOf[Double]).toMap

    val BC_importe_pase: Broadcast[Map[(Long,String,Long,Long,Long,Long,Long,Double), Double]] = spark.sparkContext.broadcast(importe_pase)

    /************************************************************************************************************/

    val costebase_marca: Map[(Long,Long), Double] = spark.sql(
      s"""SELECT $fctm_share_inv.mesanio, $fctm_share_inv.cod_anuncio, SUM($fctm_share_inv.importe_pase) / SUM($fctm_share_inv.grps_20_totales)
         |FROM ${fctm_share_inv.getDBTable}, ${dim_agrup_cadenas.getDBTable} WHERE $fctm_share_inv.cod_cadena
         |= $dim_agrup_cadenas.cod_cadena AND $dim_agrup_cadenas.cod_grupo_n1 = 20001  GROUP BY $fctm_share_inv.mesanio, $fctd_share_grps.cod_anuncio
           """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long]) -> x(2).asInstanceOf[Double]).toMap

    val BC_costebase_marca: Broadcast[Map[(Long,Long), Double]] = spark.sparkContext.broadcast(costebase_marca)

    val costebase_anunc: Map[(Long,Long), Double] = spark.sql(
      s"""SELECT $fctm_share_inv.mesanio, $fctd_share_grps.cod_anunc, SUM($fctm_share_inv.importe_pase) / SUM($fctm_share_inv.grps_20_totales)
         |FROM ${fctm_share_inv.getDBTable}, ${dim_agrup_cadenas.getDBTable}  WHERE $fctm_share_inv.cod_cadena
         |= $dim_agrup_cadenas.cod_cadena AND $dim_agrup_cadenas.cod_grupo_n1 = 20001  GROUP BY $fctm_share_inv.mesanio, $fctd_share_grps.cod_anunc,
           """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long]) -> x(2).asInstanceOf[Double]).toMap

    val BC_costebase_anunc: Broadcast[Map[(Long,Long), Double]] = spark.sparkContext.broadcast(costebase_anunc)

    val costebase_producto: Map[(Long,Long), Double] = spark.sql(
      s"""SELECT $fctm_share_inv.mesanio,$fctd_share_grps.cod_producto, SUM($fctm_share_inv.importe_pase) / SUM($fctm_share_inv.grps_20_totales)
         |FROM ${fctm_share_inv.getDBTable}, ${dim_agrup_cadenas.getDBTable} WHERE $fctm_share_inv.cod_cadena
         |= $dim_agrup_cadenas.cod_cadena AND $dim_agrup_cadenas.cod_grupo_n1 = 20001 GROUP BY $fctm_share_inv.mesanio,$fctm_share_inv.cod_producto,
           """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long]) -> x(2).asInstanceOf[Double]).toMap

    val BC_costebase_producto: Broadcast[Map[(Long,Long), Double]] = spark.sparkContext.broadcast(costebase_producto)

    val costebase_grupo: Map[(Long,Long), Double] = spark.sql(
      s"""SELECT $fctm_share_inv.mesanio,$fctm_share_inv.cod_grupo, SUM($fctm_share_inv.importe_pase) / SUM($fctm_share_inv.grps_20_totales)
         |FROM ${fctm_share_inv.getDBTable}, ${dim_agrup_cadenas.getDBTable} WHERE $fctm_share_inv.cod_cadena
         |= $dim_agrup_cadenas.cod_cadena AND $dim_agrup_cadenas.cod_grupo_n1 = 20001 GROUP BY $fctm_share_inv.mesanio,$fctm_share_inv.cod_grupo,
           """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long]) -> x(2).asInstanceOf[Double]).toMap

    val BC_costebase_grupo: Broadcast[Map[(Long,Long), Double]] = spark.sparkContext.broadcast(costebase_grupo)


    val costebase_else: Map[(Long,Long),Double] = spark.sql(
      s"""SELECT $fctm_share_inv.mesanio,$fctm_share_inv.cod_anuncio,SUM($fctm_share_inv.importe_pase) / SUM($fctm_share_inv.grps_20_totales)
         |FROM ${fctm_share_inv.getDBTable}, ${dim_agrup_cadenas.getDBTable} WHERE $fctm_share_inv.cod_cadena
         |= $dim_agrup_cadenas.cod_cadena AND $dim_agrup_cadenas.cod_grupo_n1 = 20001 GROUP BY $fctm_share_inv.mesanio,$fctm_share_inv.cod_anuncio
           """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long]) -> x(2).asInstanceOf[Double]).toMap

    val BC_costebase_else: Broadcast[Map[(Long,Long),Double]] = spark.sparkContext.broadcast(costebase_else)
    /************************************************************************************************************/

    val cod_cadena_disney: List[Long] = spark.sql(
      s"""SELECT cod_cadena FROM ${dim_agrup_cadenas.getDBTable}
         |WHERE cod_grupo_n1 = 20003
       """.stripMargin).map(x => x.getLong(0)).collect.toList
    val BC_cod_cadena_disney:Broadcast[List[Long]] = spark.sparkContext.broadcast(cod_cadena_disney)


    /************************************************************************************************************/

    val cod_posicion_pb2_in_posicionado: List[Long] = spark.sql(
      s"""SELECT cod_posicion_pb2 FROM ${fctd_share_grps.getDBTable}
         |WHERE cod_posicion_pb2 IN (1, 2, 3, 997, 998, 999)
       """.stripMargin).map(x => x.getLong(0)).collect.toList

    val BC_cod_posicion_pb2_in_posicion: Broadcast[List[Long]] = spark.sparkContext.broadcast(cod_posicion_pb2_in_posicionado)

    /************************************************************************************************************/

    val num_cadenas_forta_calc: Map[(Long,Long),Long] = spark.sql(
      s"""
         |SELECT aniomes, cod_anuncio, COUNT(DISTINCT cod_cadena) AS cads_forta
         |FROM ${fctd_share_grps.getDBTable}
         |WHERE cod_fg_forta = 1
         |GROUP BY aniomes, cod_anuncio
       """.stripMargin).collect.map(x => (x(0).asInstanceOf[Long], x(1).asInstanceOf[Long]) -> x(2).asInstanceOf[Long]).toMap

    val BC_num_cadenas_forta_calc: Broadcast[Map[(Long,Long),Long]] = spark.sparkContext.broadcast(num_cadenas_forta_calc)

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

    val coef_cadena_calc_1: Map[Long, Double] = spark.sql(
      s"""
         |SELECT tb_coeficientes.cod_cadena, tb_coeficientes.indice
         |FROM tb_coeficientes, $fctm_share_inv
         |AND tb_coeficientes.coeficiente = "CADENA_N2"
         |AND tb_coeficientes.cod_cadena = 30006
         |AND $fctm_share_inv.aniomes >= year(tb_coeficientes.fecha_ini)&mes(tb_coeficientes.fecha_ini)
         |AND $fctm_share_inv.aniomes <= year(tb_coeficientes.fecha_ini)&mes(tb_coeficientes.fecha_ini)
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    // Columna Coef Cadena
    val coef_cadena_calc_2: Map[Long, Double] = spark.sql(
      s"""
         |SELECT $dim_agrup_cadenas.cod_cadena, tb_coeficientes.indice
         |FROM tb_coeficientes, ${dim_agrup_cadenas.getDBTable}, $fctm_share_inv
         |AND tb_coeficientes.coeficiente = "CADENA_N2"
         |AND tb_coeficientes.cod_cadena <> 30006
         |AND tb_coeficientes.cod_cadena = $dim_agrup_cadenas.cod_grupo_n2
         |AND $fctm_share_inv.aniomes >= year(tb_coeficientes.fecha_ini)&mes(tb_coeficientes.fecha_ini)
         |AND $fctm_share_inv.aniomes <= year(tb_coeficientes.fecha_ini)&mes(tb_coeficientes.fecha_ini)
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_coef_cadena_calc_1: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(coef_cadena_calc_1)
    val BC_coef_cadena_calc_2: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(coef_cadena_calc_2)

    /************************************************************************************************************/

    // Columna COEF_ANUNCIANTET

    val coef_anunciante_calc: Map[Long, Double] = spark.sql(
      s"""
         |SELECT tc.cod_cadena, tc.indice
         |FROM $tb_coeficientes AS tc $fctm_share_inv AS inv,
         |WHERE tc.coeficiente = "ANUNCIANTE"
         |AND $fctm_share_inv.aniomes >= year(tc.fecha_ini)&mes(tc.fecha_ini)
         |AND $fctm_share_inv.aniomes <= year(tc.fecha_fin)&mes(tc.fecha_fin)
       """.stripMargin).collect().map( x => x(0).asInstanceOf[Long] -> x(1).asInstanceOf[Double]).toMap

    val BC_coef_anunciante_calc: Broadcast[Map[Long, Double]] = spark.sparkContext.broadcast(coef_anunciante_calc)

    /************************************************************************************************************/

    // Columna coef_evento

    val coef_evento_calc = spark.sql(
      s"""SELECT DISTINCT $fctd_share_grps.cod_eventos, $cat_eventos.indice
         |FROM ${fctd_share_grps.getDBTable}, $cat_eventos
         |WHERE $fctd_share_grps.cod_eventos = $cat_eventos.cod_evento AND $fctd_share_grps.cod_eventos IS NOT NULL
         |AND $cat_eventos.cod_evento IS NOT NULL
         |AND $fctd_share_grps.fecha_dia >= $cat_eventos.fecha_ini
         |AND $fctd_share_grps.fecha_dia <= $cat_eventos.fecha_fin""".stripMargin).collect().map(x => x(0).asInstanceOf[java.lang.Long] -> x(1).asInstanceOf[java.lang.Double]).toMap

    val BC_coef_evento_calc: Broadcast[Map[java.lang.Long, java.lang.Double]] = spark.sparkContext.broadcast(coef_evento_calc)

    /************************************************************************************************************/

    val fctd_importe_pase: DataFrame = getSharedGrps(spark)
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

    val fctm_share_inv_col_1: DataFrame = getColumn_importe_pase( fctm_share_inv_col_0_3, BC_importe_pase)

    /************************************************************************************************************/

    val fctm_share_inv_col_2: DataFrame = getColumn_costebase(spark, fctm_share_inv_col_1, BC_costebase_marca, BC_costebase_anunc, BC_costebase_producto, BC_costebase_grupo, BC_costebase_else)

    /************************************************************************************************************/

    val fctm_share_inv_col_3: DataFrame = getColumn_coef_evento( fctm_share_inv_col_2, BC_coef_evento_calc)

    /************************************************************************************************************/

    val fctm_share_inv_col_4: DataFrame = getColumn_por_pt_mediaset( fctm_share_inv_col_3, BC_por_pt_mediaset_calc)

    /************************************************************************************************************/

    val fctm_share_inv_col_5: DataFrame = getColumn_por_pt_grupocadena( fctm_share_inv_col_4, BC_por_pt_grupocadena_calc)

    /************************************************************************************************************/

    val fctm_share_inv_col_6: DataFrame = getColumn_dif_por_primetime( fctm_share_inv_col_5)

    /************************************************************************************************************/

    val fctd_share_inv_col_7: DataFrame = getColumn_coef_pt( fctm_share_inv_col_6, BC_tb_coeficientes_list,BC_cod_cadena_disney)

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

    val por_posmediaset_calc: Map[(Long,Long,Long,Double), Double] = spark.sql(
      s"""SELECT in_pos.cod_cadena, in_pos.fecha_dia, in_pos.cod_grupo_n1, in_mediaset.grps_20_totales, (in_pos.sum_campana_grps_in_pos / in_mediaset.sum_campana_grps_in_mediaset)
         |AS por_posmediaset
         |FROM
         |(SELECT  $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, SUM(grps_20_totales) AS sum_campana_grps_in_pos FROM ${fctd_share_grps.getDBTable},${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.cod_fg_posicionado in (1)
         |AND $fctd_share_grps.nom_sect_geog = "PYB"
         |AND $fctd_share_grps.cod_fg_filtrado = 0
         |AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
         |AND $dim_agrup_cadenas.cod_grupo_n1 = 20001
         |AND $fctd_share_grps.fecha_dia between ($dim_agrup_cadenas.fecha_ini;$dim_agrup_cadenas.fecha_fin)
         |GROUP BY $fctd_share_grps.cod_cadena,$dim_agrup_cadenas.cod_grupo_n1,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS in_pos
         |JOIN
         |(SELECT $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, SUM($fctm_share_inv.grps_20_totales) AS sum_campana_grps_in_mediaset
         |FROM ${fctd_share_grps.getDBTable},${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.nom_sect_geog = "PYB"
         |AND $fctd_share_grps.cod_fg_filtrado = 0
         |AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
         |AND $dim_agrup_cadenas.cod_grupo_n1 = 20001
         |AND $fctd_share_grps.fecha_dia between ($dim_agrup_cadenas.fecha_ini;$dim_agrup_cadenas.fecha_fin)
         |GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS in_mediaset
         |ON (in_pos.cod_cadena = in_mediaset.cod_cadena)
         | AND in_pos.cod_grupo_n1 = in_mediaset.cod_grupo_n1
         | AND in_pos.fecha_dia = in_mediaset.fecha_dia
         | AND in_pos.fecha_ini = in_mediaset.fecha_ini
         | AND in_pos.fecha_fin = in_mediaset.fecha_fin
       """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long],x(2).asInstanceOf[Long],x(3).asInstanceOf[Double]) -> x(4).asInstanceOf[Double]).toMap

    val BC_por_posmediaset_calc: Broadcast[Map[(Long,Long,Long,Double), Double]] = spark.sparkContext.broadcast(por_posmediaset_calc)

    val fctd_share_inv_col_14: DataFrame = getColumn_por_posmediaset( fctd_share_inv_col_13, BC_por_posmediaset_calc)
    registerTemporalTable(fctd_share_inv_col_14, s"$fctm_share_inv")

    /************************************************************************************************************/

    val por_posgrupocadena_n1_calc: Map[(Long,Long,Long,Double), Double] = spark.sql(
      s"""SELECT in_pos.cod_cadena, in_pos.fecha_dia, in_pos.cod_grupo_n1, in_mediaset.grps_20_totales, (in_pos.sum_campana_grps_in_pos / in_mediaset.sum_campana_grps_in_mediaset)
         |AS por_posmediaset
         |FROM
         |(SELECT  $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, SUM(grps_20_totales) AS sum_campana_grps_in_pos FROM ${fctd_share_grps.getDBTable},${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.cod_fg_posicionado = 1
         |AND $fctd_share_grps.nom_sect_geog = "PYB"
         |AND $fctd_share_grps.cod_fg_filtrado = 0
         |AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
         |AND $dim_agrup_cadenas.cod_grupo_n1 = 20001
         |AND $fctd_share_grps.fecha_dia between ($dim_agrup_cadenas.fecha_ini;$dim_agrup_cadenas.fecha_fin)
         |GROUP BY $fctd_share_grps.cod_cadena,$dim_agrup_cadenas.cod_grupo_n1,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS in_pos
         |JOIN
         |(SELECT $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, SUM($fctm_share_inv.grps_20_totales) AS sum_campana_grps_in_mediaset
         |FROM ${fctd_share_grps.getDBTable},${dim_agrup_cadenas.getDBTable}
         |WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
         |AND $fctd_share_grps.nom_sect_geog = "PYB"
         |AND $fctd_share_grps.cod_fg_filtrado = 0
         |AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
         |AND $dim_agrup_cadenas.cod_grupo_n1 = 20001
         |AND $fctd_share_grps.fecha_dia between ($dim_agrup_cadenas.fecha_ini;$dim_agrup_cadenas.fecha_fin)
         |GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1) AS in_mediaset
         |ON (in_pos.cod_cadena = in_mediaset.cod_cadena)
       """.stripMargin).collect().map( x => (x(0).asInstanceOf[Long],x(1).asInstanceOf[Long],x(2).asInstanceOf[Long],x(3).asInstanceOf[Double]) -> x(4).asInstanceOf[Double]).toMap

    val BC_por_posgrupocadena_n1_calc: Broadcast[Map[(Long,Long,Long,Double),Double]] = spark.sparkContext.broadcast(por_posgrupocadena_n1_calc)

    val fctd_share_inv_col_15: DataFrame = getColumn_por_posGrupoCadena_n1( fctd_share_inv_col_14, BC_por_posgrupocadena_n1_calc)

    /************************************************************************************************************/

    val fctd_share_inv_col_16: DataFrame = getColumn_dif_por_posicionamiento( fctd_share_inv_col_15)

    /************************************************************************************************************/

    val fctd_share_inv_col_17: DataFrame = getColumn_coef_posic( fctd_share_inv_col_16, BC_tb_coeficientes_list)

    /************************************************************************************************************/

    val fctd_share_inv_col_18: DataFrame = getColumn_cuota_por_grupo( fctd_share_inv_col_17, BC_cuota_por_grupo_calc)

    /************************************************************************************************************/

    val fctd_share_inv_col_19: DataFrame = getColumn_coef_cuota( fctd_share_inv_col_18, BC_tb_coeficientes_list)

    /************************************************************************************************************/

    val fctd_share_inv_col_20: DataFrame = getColumn_coef_cadena( fctd_share_inv_col_19, BC_coef_cadena_calc_1, BC_coef_cadena_calc_2)

    /************************************************************************************************************/

    val fctd_share_inv_col_21: DataFrame = getColumn_num_cadenas_forta_emitido( fctd_share_inv_col_20, BC_num_cadenas_forta_calc)

    /************************************************************************************************************/

    val fctd_share_inv_col_22: DataFrame = getColumn_coef_forta( fctd_share_inv_col_21, BC_coef_forta_param_valor, BC_indice_coeficiente_forta)

    /************************************************************************************************************/

    val fctd_share_inv_col_23: DataFrame = getColumn_coef_anunciante( fctd_share_inv_col_22, BC_coef_anunciante_calc)

    /************************************************************************************************************/

    val fctd_share_inv_col_24: DataFrame = getColumn_inv_pre(spark, fctd_share_inv_col_23)
    registerTemporalTable(fctd_share_inv_col_24, s"$fctm_share_inv")

    /************************************************************************************************************/
    // TODO Ahora está puesto con fecha_dia pero hay que ver si se va a particionar fctm_share_inversion y hacerlo por el campo fecha_part
    val inv_est_pe_calc: Map[String, java.lang.Double] = spark.sql(
      s"""
         |SELECT
         |N1_INFOADEX.DES_GRUPO_N1 AS des_grupo_n1,
         |GRPS_N2.SUMAINVN2 AS inv_est_pe
         |-- SUBSTRING(GRPS_N2.fecha_dia, 1, 7) AS fecha_dia
         |FROM
         |(SELECT
         |$dim_agrup_cadenas.cod_grupo_n2,
         |sum($fctm_share_inv.inv_pre) AS SUMAINVN2,
         |CAST(CONCAT(SUBSTRING($fctm_share_inv.fecha_dia, 1, 7), '-01') AS TIMESTAMP) as fecha_dia
         |FROM $fctm_share_inv, ${dim_agrup_cadenas.getDBTable}
         |WHERE $fctm_share_inv.COD_CADENA = $dim_agrup_cadenas.COD_CADENA
         |AND CAST(CONCAT(SUBSTRING($fctm_share_inv.fecha_dia, 1, 7), '-01') AS TIMESTAMP) >= CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_ini, 1, 7), '-01') AS TIMESTAMP)
         |AND CAST(CONCAT(SUBSTRING($fctm_share_inv.fecha_dia, 1, 7), '-01') AS TIMESTAMP) <= CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_fin, 1, 7), '-31') AS TIMESTAMP)
         |AND DES_GRUPO_N0="TTV"
         |GROUP BY $dim_agrup_cadenas.cod_grupo_n2, $fctm_share_inv.fecha_dia) AS GRPS_N2
         |RIGHT JOIN
         |(SELECT cod_grupo_n1, cod_grupo_n2, CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_ini, 1, 7), '-01') AS TIMESTAMP) as mesanioini,
         |CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_fin, 1, 7), '-31') AS TIMESTAMP) as mesaniofin
         |FROM ${dim_agrup_cadenas.getDBTable}
         |WHERE DES_GRUPO_N0 LIKE "%INFOADEX%") AS N1_INFOADEX
         |WHERE GRPS_N2.cod_grupo_n2 = N1_INFOADEX.cod_grupo_n2
         |AND GRPS_N2.fecha_dia >= N1_INFOADEX.mesanioini
         |AND GRPS_N2.fecha_dia <= N1_INFOADEX.mesaniofin
       """.stripMargin).collect().map(x => x(0).asInstanceOf[String] -> x(1).asInstanceOf[java.lang.Double]).toMap
    val BC_inv_est_pe_calc: Broadcast[Map[String, java.lang.Double]] = spark.sparkContext.broadcast(inv_est_pe_calc)


    createTable_tb_inv_agregada(spark, BC_inv_est_pe_calc)

    val coef_corrector_DF: DataFrame = spark.sql(
      s"""
         |SELECT $dim_agrup_cadenas.cod_cadena AS cod_cadena1, $tb_inv_agregada.coef_corrector, CAST(CONCAT($tb_inv_agregada.anho, $tb_inv_agregada.mes) AS BIGINT) AS aniomes1
         |FROM ${tb_inv_agregada.getDBTable} AS $tb_inv_agregada, ${dim_agrup_cadenas.getDBTable}
         |WHERE $tb_inv_agregada.gr_n1_infoadex = $dim_agrup_cadenas.des_grupo_n1
         |AND CONCAT($tb_inv_agregada.anho, $tb_inv_agregada.mes) >= CONCAT(YEAR(fecha_ini), MONTH(fecha_ini))
         |AND CONCAT($tb_inv_agregada.anho, $tb_inv_agregada.mes) <= CONCAT(YEAR(fecha_fin), MONTH(fecha_fin))
       """.stripMargin)

    val joinDF = fctd_share_inv_col_24.join(coef_corrector_DF, fctd_share_inv_col_24("cod_cadena") === coef_corrector_DF("cod_cadena1")
      && fctd_share_inv_col_24("aniomes") === coef_corrector_DF("aniomes1"), "left")

    val fctd_share_inv_col_25: DataFrame = joinDF.drop("cod_cadena1","aniomes1")

    val fctd_share_inv_col_26: DataFrame = getColumn_inv_final(fctd_share_inv_col_25)

    val result = fctd_share_inv_col_26

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
  def createTable_tb_inv_agregada(spark: SparkSession,  BC_inv_est_pe_calc: Broadcast[Map[String, java.lang.Double]]) : Unit = {


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

  def getColumn_inv_est_pe(originDF: DataFrame, BC_inv_est_pe_calc: Broadcast[Map[String, java.lang.Double]]): DataFrame = {
    originDF.withColumn("inv_est_pe", UDF_inv_est_pe(BC_inv_est_pe_calc)(col("gr_n1_infoadex")))
  }

  def UDF_inv_est_pe(BC_inv_est_pe_calc: Broadcast[Map[String, java.lang.Double]]): UserDefinedFunction = {

    udf[java.lang.Double, String]( gr_n1_infoadex => FN_inv_est_pe(BC_inv_est_pe_calc.value, gr_n1_infoadex))

  }

  def FN_inv_est_pe(invEstPe_calc: Map[String, java.lang.Double], gr_n1_infoadex: String): java.lang.Double =  {

    invEstPe_calc.getOrElse(gr_n1_infoadex, 0D)

  }

  def getColumn_coef_corrector_inv_agregada(originDF: DataFrame): DataFrame = {

    originDF.withColumn("coef_corrector", UDF_coef_corrector_inv_agregada()(col("inv_est_infoadex"), col("inv_est_pe")))

  }

  def UDF_coef_corrector_inv_agregada(): UserDefinedFunction = {
    udf[java.lang.Double, java.lang.Double, java.lang.Double] ( (inv_est_infoadex, inv_est_pe) => FN_coef_corrector_inv_agregada(inv_est_infoadex, inv_est_pe))
  }

  def FN_coef_corrector_inv_agregada(inv_est_infoadex: java.lang.Double, inv_est_pe: java.lang.Double): java.lang.Double = {

    try {
      Some(((inv_est_infoadex - inv_est_pe) / inv_est_pe) + 1).get
    } catch {
      case e: Exception => 0D
    }
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
    rel_camp_trgt_may = new Entity(parametrizationCfg, "rel_camp_trgt_may" , true)
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

  def getColumn_importe_pase(originDF: DataFrame, BC_importe_pase: Broadcast[Map[(Long,String,Long,Long,Long,Long,Long,Double), Double]]): DataFrame = {
    originDF.withColumn("importe_pase", UDF_importe_pase(BC_importe_pase)(col("aniomes"),col("nom_cadena"),col("cod_target_compra"),col("cod_anunc"),col("cod_anunciante_subsidiario"),col("cod_anuncio"),col("cod_marca"),col("grps_20_totales")))
  }

  def UDF_importe_pase(BC_importe_pase: Broadcast[Map[(Long,String,Long,Long,Long,Long,Long,Double), Double]]): UserDefinedFunction = {
    udf[Double,Long,String,Long,Long,Long,Long,Long,Double]((aniomes, nom_cadena,cod_target_compra,cod_anunc, cod_anunciante_subsidiario, cod_anuncio, cod_marca, grps_20_totales)  => FN_cod_fg_grupomediaset(aniomes, nom_cadena,cod_target_compra,cod_anunc, cod_anunciante_subsidiario, cod_anuncio, cod_marca, grps_20_totales, BC_importe_pase.value))
  }

  def FN_cod_fg_grupomediaset(aniomes:Long, nom_cadena: String,cod_target_compra:Long,cod_anunc:Long, cod_anunciante_subsidiario:Long, cod_anuncio:Long, cod_marca:Long, grps_20_totales:Double, importe_pase: Map[(Long,String,Long,Long,Long,Long,Long,Double), Double]): Double = {

    importe_pase.getOrElse((aniomes, nom_cadena,cod_target_compra,cod_anunc, cod_anunciante_subsidiario, cod_anuncio, cod_marca, grps_20_totales), 0D)

  }

  /************************************************************************************************************/

  def getColumn_costebase(spark: SparkSession, originDF: DataFrame, BC_costebase_marca: Broadcast[Map[(Long,Long), Double]], BC_costebase_anunc: Broadcast[Map[(Long,Long), Double]],
                          BC_costebase_producto: Broadcast[Map[(Long,Long), Double]], BC_costebase_grupo: Broadcast[Map[(Long,Long), Double]], BC_costebase_else: Broadcast[Map[(Long,Long),Double]]): DataFrame = {

    import spark.implicits._

    originDF.withColumn("costebase",
      UDF_costebase(BC_costebase_marca, BC_costebase_anunc,
        BC_costebase_producto, BC_costebase_grupo, BC_costebase_else)(col("cod_fg_cadmediaset"), col("cod_fg_campemimediaset"), col("cod_fg_anuncmediaset"),
        col("cod_fg_prodmediaset"), col("cod_fg_grupomediaset"), col("cod_marca"), col("cod_anunc"),
        col("cod_producto"), col("cod_grupo"),col ("mesanio")))

  }

  def UDF_costebase(BC_costebase_marca: Broadcast[Map[(Long,Long), Double]], BC_costebase_anunc: Broadcast[Map[(Long,Long), Double]],
                    BC_costebase_producto: Broadcast[Map[(Long,Long), Double]], BC_costebase_grupo: Broadcast[Map[(Long,Long), Double]], BC_costebase_else: Broadcast[Map[(Long,Long), Double]]): UserDefinedFunction = {
    udf[Double, Int, Int, Int, Int, Int, Long, Long, Long, Long, Long]((cod_fg_cadmediaset, cod_fg_campemimediaset, cod_fg_anuncmediaset, cod_fg_prodmediaset, cod_fg_grupomediaset, cod_marca, cod_anunc, cod_producto, cod_grupo, mesanio) => FN_costebase(BC_costebase_marca.value,
      BC_costebase_anunc.value, BC_costebase_producto.value, BC_costebase_grupo.value,BC_costebase_else.value, cod_fg_cadmediaset, cod_fg_campemimediaset, cod_fg_anuncmediaset, cod_fg_prodmediaset, cod_fg_grupomediaset, cod_marca, cod_anunc, cod_producto, cod_grupo, mesanio))
  }

  def FN_costebase(costebase_marca: Map[(Long,Long), Double], costebase_anunc: Map[(Long,Long), Double], costebase_producto: Map[(Long,Long), Double], costebase_grupo: Map[(Long,Long), Double],costebase_else: Map[(Long,Long), Double], cod_fg_cadmediaset: Int,
                   cod_fg_campemimediaset: Int, cod_fg_anuncmediaset: Int, cod_fg_prodmediaset: Int, cod_fg_grupomediaset: Int, cod_marca: Long, cod_anunc: Long, cod_producto: Long, cod_grupo: Long, mesanio: Long): Double = {

    var result = 0D

    if(cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 1) {
      result = costebase_marca.getOrElse((mesanio,cod_marca), 0D)
    } else if (cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 0 && cod_fg_anuncmediaset == 1) {
      result = costebase_anunc.getOrElse((mesanio, cod_anunc), 0D)
    } else if (cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 0 && cod_fg_anuncmediaset == 0 && cod_fg_prodmediaset == 1) {
      result = costebase_producto.getOrElse((mesanio, cod_producto), 0D)
    } else if (cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 0 && cod_fg_anuncmediaset == 0 && cod_fg_prodmediaset == 0 && cod_fg_grupomediaset == 1) {
      result = costebase_grupo.getOrElse((mesanio, cod_grupo), 0D)
    }
    else if(cod_fg_cadmediaset == 1){
      result = costebase_else.getOrElse((mesanio, cod_anunc),0D)
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

  def getColumn_por_pt_mediaset( originDF: DataFrame, BC_por_pt_mediaset_calc: Broadcast[Map[(Long,Long,Long,Long), Double]]): DataFrame = {

    originDF.withColumn("por_pt_mediaset", UDF_por_pt_mediaset(BC_por_pt_mediaset_calc)(col("aniomes"), col("cod_anuncio"), col("cod_anunc"),col("cod_cadena")))
  }

  def UDF_por_pt_mediaset(BC_por_pt_mediaset_calc: Broadcast[Map[(Long,Long,Long,Long), Double]]): UserDefinedFunction = {

    udf[Double, Long, Long,Long,Long]( (aniomes, cod_anuncio, cod_anunc, cod_cadena) => FN_por_pt_mediaset(BC_por_pt_mediaset_calc.value, aniomes, cod_anuncio, cod_anunc, cod_cadena ))
  }

  def FN_por_pt_mediaset(por_pt_mediaset_calc: Map[(Long,Long,Long,Long), Double], aniomes: Long, cod_anuncio: Long, cod_anunc: Long, cod_cadena: Long): Double = {

    por_pt_mediaset_calc.getOrElse((aniomes, cod_anuncio, cod_anunc, cod_cadena), 0D)

  }

  /************************************************************************************************************/

  def getColumn_por_pt_grupocadena( originDF: DataFrame, BC_por_pt_grupocadena_calc: Broadcast[Map[(Long,Long,Long,Long), Double]]): DataFrame = {
    originDF.withColumn("por_pt_grupocadena", UDF_por_pt_grupocadena(BC_por_pt_grupocadena_calc)(col("cod_cadena"),col("aniomes"),col("cod_grupo_n2"),col("cod_anuncio")))
  }

  def UDF_por_pt_grupocadena(BC_por_pt_grupocadena_calc:  Broadcast[Map[(Long,Long,Long,Long), Double]]): UserDefinedFunction = {
    udf[Double, Long, Long, Long, Long]((cod_cadena, aniomes, cod_grupo_n2, cod_anuncio) => FN_por_pt_grupocadena(BC_por_pt_grupocadena_calc.value, cod_cadena, aniomes, cod_grupo_n2, cod_anuncio))
  }

  def FN_por_pt_grupocadena(porPtGrupoCadena_calc: Map[(Long,Long,Long,Long), Double], cod_cadena: Long, aniomes: Long, cod_grupo_n2: Long, cod_anuncio: Long): Double = {
    porPtGrupoCadena_calc.getOrElse((cod_cadena, aniomes, cod_grupo_n2, cod_anuncio), 0D)
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

  def getColumn_coef_pt( originDF: DataFrame, BC_tb_coeficientes_list: Broadcast[List[Coeficientes]],BC_cod_cadena_disney: Broadcast[List[Long]]): DataFrame = {
    originDF.withColumn("coef_pt", UDF_coef_pt(BC_tb_coeficientes_list,BC_cod_cadena_disney)(col("dif_por_primetime"),col("aniomes"),col("cod_cadena")))
  }

  def UDF_coef_pt(BC_tb_coeficientes_list: Broadcast[List[Coeficientes]],BC_cod_cadena_disney_list:Broadcast[List[Long]]): UserDefinedFunction = {

    udf[Double,Double,BigInt,Long]( (dif_por_primetime,aniomes,cod_cadena)  => FN_coef_pt( BC_tb_coeficientes_list.value,BC_cod_cadena_disney_list.value, dif_por_primetime,aniomes,cod_cadena) )

  }

  def FN_coef_pt(Coeficientes_list: List[Coeficientes], cod_cadena_disney: List[Long], dif_por_primetime: Double, aniomes : BigInt, cod_cadena : Long): Double = {

    var result = 1D
    if(!cod_cadena_disney.contains(cod_cadena)){
      for (elem <- Coeficientes_list) {
        if ( elem.coeficiente.equalsIgnoreCase("PRIMETIME")
          && dif_por_primetime > elem.MIN_RANGO
          && dif_por_primetime <= elem.MAX_RANGO
          && aniomes >= transformToAniomes(elem.fecha_ini)
          && aniomes <= transformToAniomes(elem.fecha_fin)){
          result = elem.INDICE
        }
      }
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_por_cualmediaset( originDF: DataFrame, BC_por_cualmediaset_calc: Broadcast[Map[(Long,Long,Long,Double), Double]]): DataFrame = {

    originDF.withColumn("por_cualmediaset", UDF_por_cualmediaset(BC_por_cualmediaset_calc)(col("cod_cadena"), col("fecha_dia"), col("cod_grupo_n1"), col("grps_20_totales")))
  }

  def UDF_por_cualmediaset(BC_por_cualmediaset_calc: Broadcast[Map[(Long,Long,Long,Double), Double]]): UserDefinedFunction = {

    udf[Double,Long,Long,Long,Double]( (cod_cadena,fecha_dia,cod_grupo_n1,grps_20_totales)  => FN_por_cualmediaset(BC_por_cualmediaset_calc.value, cod_cadena, fecha_dia, cod_grupo_n1, grps_20_totales))
  }

  def FN_por_cualmediaset(porCualmediaset_calc: Map[(Long,Long,Long,Double), Double], cod_cadena: Long, fecha_dia: Long, cod_grupo_n1: Long, grps_20_totales: Double ): Double = {

    porCualmediaset_calc.getOrElse((cod_cadena, fecha_dia, cod_grupo_n1, grps_20_totales), 0D)
  }

  /************************************************************************************************************/

  def getColumn_por_cualgrupocadena( originDF: DataFrame, BC_por_cualgrupocadena_calc: Broadcast[Map[(Long,Long,Long,Double), Double]]): DataFrame = {

    originDF.withColumn("por_cualgrupocadena", UDF_por_cualgrupocadena(BC_por_cualgrupocadena_calc)(col("cod_cadena"), col("fecha_dia"), col("cod_grupo_n2"), col("grps_20_totales")))
  }

  def UDF_por_cualgrupocadena(BC_por_cualgrupocadena_calc: Broadcast[Map[(Long,Long,Long,Double), Double]]): UserDefinedFunction = {

    udf[Double, Long,Long,Long,Double]( (cod_cadena,fecha_dia,cod_grupo_n2,grps_20_totales) => FN_por_cualgrupocadena(BC_por_cualgrupocadena_calc.value, cod_cadena,fecha_dia,cod_grupo_n2,grps_20_totales))
  }

  def FN_por_cualgrupocadena(porCualGrupoCadena_calc: Map[(Long,Long,Long,Double), Double], cod_cadena: Long, fecha_dia: Long, cod_grupo_n2: Long, grps_20_totales: Double ): Double = {

    porCualGrupoCadena_calc.getOrElse((cod_cadena,fecha_dia,cod_grupo_n2,grps_20_totales), 0D)

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

    udf[Double, Double,BigInt]( (dif_por_cualitativos,aniomes)  => FN_coef_cual( BC_tb_coeficientes_list.value, dif_por_cualitativos,aniomes) )
  }

  def FN_coef_cual( Coeficientes_list: List[Coeficientes], dif_por_cualitativos: Double,aniomes: BigInt ): Double = {

    var result = 1D
    for (elem <- Coeficientes_list) {
      if ( elem.coeficiente.equalsIgnoreCase("Cualitativos")
        && dif_por_cualitativos > elem.MIN_RANGO
        && dif_por_cualitativos <= elem.MAX_RANGO
        && aniomes >= transformToAniomes(elem.fecha_ini)
        && aniomes <= transformToAniomes(elem.fecha_fin)) {
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

  def getColumn_por_posmediaset( originDF: DataFrame, BC_por_posmediaset_calc: Broadcast[Map[(Long,Long,Long,Double), Double]]): DataFrame = {

    originDF.withColumn("por_posmediaset", UDF_por_posmediaset(BC_por_posmediaset_calc)(col("cod_cadena"),col("fecha_dia"),col("cod_grupo_n1"),col("grps_20_totales")))
  }


  def UDF_por_posmediaset(BC_por_posmediaset_calc: Broadcast[Map[(Long,Long,Long,Double), Double]]): UserDefinedFunction = {
    udf[Double, Long,Long,Long,Double] ( (cod_cadena,fecha_dia,cod_grupo_n1,grps_20_totales) => FN_por_posmediaset(BC_por_posmediaset_calc.value, cod_cadena,fecha_dia,cod_grupo_n1,grps_20_totales))
  }

  def FN_por_posmediaset(porPosMediaset_calc: Map[(Long,Long,Long,Double), Double], cod_cadena: Long, fecha_dia:Long, cod_grupo_n1: Long, grps_20_totales: Double): Double = {

    porPosMediaset_calc.getOrElse((cod_cadena,fecha_dia,cod_grupo_n1,grps_20_totales), 0D)

  }

  /************************************************************************************************************/

  def getColumn_por_posGrupoCadena_n1( originDF: DataFrame, BC_por_posgrupocadena_n1_calc: Broadcast[Map[(Long,Long,Long,Double),Double]]): DataFrame = {

    originDF.withColumn("por_posGrupoCadena", UDF_por_posGrupoCadena(BC_por_posgrupocadena_n1_calc)(col("cod_cadena"),col("fecha_dia"),col("cod_grupo_n1"),col("grps_20_totales")))
  }


  def UDF_por_posGrupoCadena(BC_por_posgrupocadena_n1_calc: Broadcast[Map[(Long,Long,Long,Double),Double]]): UserDefinedFunction = {
    udf[Double,Long,Long,Long,Double] ( (cod_cadena,fecha_dia,cod_grupo_n1,grps_20_totales) => FN_por_posGrupoCadena(BC_por_posgrupocadena_n1_calc.value, cod_cadena,fecha_dia,cod_grupo_n1,grps_20_totales))
  }

  def FN_por_posGrupoCadena(porPosGrupoCadena_n1_calc: Map[(Long,Long,Long,Double),Double], cod_cadena: Long, fecha_dia:Long, cod_grupo_n1:Long, grps_20_totales: Double): Double = {

    porPosGrupoCadena_n1_calc.getOrElse((cod_cadena, fecha_dia, cod_grupo_n1, grps_20_totales), 0D)

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

    originDF.withColumn("coef_posic", UDF_coef_posic(BC_tb_coeficientes_list)(col("dif_por_posicionamiento"), col("cod_fg_cualitativo")))

  }

  def UDF_coef_posic(BC_tb_coeficientes_list: Broadcast[List[Coeficientes]]): UserDefinedFunction = {

    udf[Double, Double, Long, BigInt]( (dif_por_posicionamiento,cod_fg_cualitativo,aniomes)  => FN_coef_posic( BC_tb_coeficientes_list.value, dif_por_posicionamiento,cod_fg_cualitativo,aniomes) )

  }

  def FN_coef_posic(Coeficientes_list: List[Coeficientes], dif_por_posicionamiento: Double, cod_fg_cualitativo: Long, aniomes: BigInt): Double = {

    var result = 1D
    if (cod_fg_cualitativo != 1) {
      for (elem <- Coeficientes_list) {
        if (elem.coeficiente.equalsIgnoreCase("POSICIONAMIENTO")
          && dif_por_posicionamiento > elem.MIN_RANGO
          && dif_por_posicionamiento <= elem.MAX_RANGO
          && aniomes >= transformToAniomes(elem.fecha_ini)
          && aniomes <= transformToAniomes(elem.fecha_fin)) {
          result = elem.INDICE
        }
      }
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_cuota_por_grupo( originDF: DataFrame, BC_cuota_por_grupo_calc: Broadcast[Map[(Long,Long,Long,Long,Long), Double]]): DataFrame = {
    originDF.withColumn("cuota_por_grupo", UDF_cuota_por_grupo(BC_cuota_por_grupo_calc)(col("aniomes"),col("cod_grupo_n1"),col("fecha_dia"),col("fecha_ini"),col("fecha_fin")))
  }

  def UDF_cuota_por_grupo(BC_cuota_por_grupo_calc: Broadcast[Map[(Long,Long,Long,Long,Long), Double]]): UserDefinedFunction = {
    udf[Double, Long,Long,Long,Long,Long]((aniomes,cod_grupo_n1,fecha_dia,fecha_ini,fecha_fin) => FN_cuota_por_grupo(BC_cuota_por_grupo_calc.value, aniomes,cod_grupo_n1,fecha_dia,fecha_ini,fecha_fin))
  }

  def FN_cuota_por_grupo(cuotaPorGrupo_calc: Map[(Long,Long,Long,Long,Long), Double], aniomes:Long,cod_grupo_n1:Long,fecha_dia:Long,fecha_ini:Long,fecha_fin:Long): Double = {
    cuotaPorGrupo_calc.getOrElse((aniomes,cod_grupo_n1,fecha_dia,fecha_ini,fecha_fin), 0D)
  }

  /************************************************************************************************************/

  def getColumn_coef_cuota( originDF: DataFrame, BC_tb_coeficientes_list: Broadcast[List[Coeficientes]]): DataFrame = {

    originDF.withColumn("coef_cuota", UDF_coef_cuota(BC_tb_coeficientes_list)(col("cuota_por_grupo")))

  }

  def UDF_coef_cuota(BC_tb_coeficientes_list: Broadcast[List[Coeficientes]]): UserDefinedFunction = {

    udf[Double, Double, BigInt]( (cuota_por_grupo,aniomes)  => FN_coef_cuota( BC_tb_coeficientes_list.value, cuota_por_grupo, aniomes) )

  }

  def FN_coef_cuota(Coeficientes_list: List[Coeficientes], cuota_por_grupo: Double, aniomes: BigInt): Double = {

    var result = 1D
    for (elem <- Coeficientes_list) {
      if ( elem.coeficiente.equalsIgnoreCase("CUOTA")
        && cuota_por_grupo > elem.MIN_RANGO
        && cuota_por_grupo <= elem.MAX_RANGO
        && aniomes >= transformToAniomes(elem.fecha_ini)
        && aniomes <= transformToAniomes(elem.fecha_fin)) {
        result = elem.INDICE
      }
    }
    result
  }

  /************************************************************************************************************/


  def getColumn_coef_cadena( originDF: DataFrame, BC_coef_cadena_cacl1: Broadcast[Map[Long, Double]],BC_coef_cadena_cacl2: Broadcast[Map[Long, Double]]): DataFrame = {

    originDF.withColumn("coef_cadena", UDF_coef_cadena(BC_coef_cadena_cacl1,BC_coef_cadena_cacl2)(col("cod_cadena"),col("cod_fg_forta")))
  }

  def UDF_coef_cadena(BC_coef_cadena_cacl: Broadcast[Map[Long, Double]], BC_coef_cadena_cacl2: Broadcast[Map[Long, Double]]): UserDefinedFunction = {

    udf[Double, Long, Long]( (cod_cadena,cod_fg_forta) => FN_coef_cadena(BC_coef_cadena_cacl.value, BC_coef_cadena_cacl2.value, cod_cadena, cod_fg_forta))
  }

  def FN_coef_cadena(coefCadena_cacl1: Map[Long, Double], coefCadena_cacl2: Map[Long, Double], cod_cadena: Long, cod_fg_forta: Long): Double = {

    var result = 1D
    if (cod_cadena == 30006 && cod_fg_forta == 0 ) {
      result = coefCadena_cacl1.getOrElse(cod_cadena, 1D)
    }
    else if(cod_cadena != 30006) {
      result = coefCadena_cacl2.getOrElse(cod_cadena, 1D)
    }

    result
  }

  /************************************************************************************************************/
  /**
    * Añade la columna "num_cadenas_forta_emitido" al DataFrame de origen
    * @param BC_num_cadenas_forta_calc: Lista que contiene el numero de cadenas forta en las que ha sido emitido un determinado anuncio
    * @return
    */
  def getColumn_num_cadenas_forta_emitido( originDF: DataFrame, BC_num_cadenas_forta_calc: Broadcast[Map[(Long,Long), Long]]): DataFrame = {

    originDF.withColumn("num_cadenas_forta_emitido", UDF_num_cadenas_forta_emitido(BC_num_cadenas_forta_calc)(col("cod_anuncio"), col("aniomes")))
  }

  def UDF_num_cadenas_forta_emitido(BC_num_cadenas_forta_calc: Broadcast[Map[(Long,Long), Long]]): UserDefinedFunction = {

    udf[Long, Long, Long]((cod_anuncio,aniomes) => FN_num_cadenas_forta_emitido(BC_num_cadenas_forta_calc.value, cod_anuncio, aniomes))
  }

  def FN_num_cadenas_forta_emitido(numCadenasFortaEmitidas: Map[(Long,Long), Long],cod_anuncio: Long, aniomes: Long): Long = {

    numCadenasFortaEmitidas.getOrElse((cod_anuncio,aniomes), 0L)

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
    originDF.withColumn("coef_anunciante", UDF_coef_anunciante(BC_coef_anunciante_calc)(col("cod_anunc"), col("cod_anunciante_subsidiario")))
  }

  def UDF_coef_anunciante(BC_coef_anunciante_calc: Broadcast[Map[Long, Double]]): UserDefinedFunction = {
    udf[Double, Long, Long]( (cod_anunc, cod_anunciante_subsidiario) => FN_coef_anunciante(BC_coef_anunciante_calc.value, cod_anunc, cod_anunciante_subsidiario))
  }

  def FN_coef_anunciante(coefAnunciante_calc: Map[Long, Double], cod_anunc: Long, cod_anunciante_subsidiario: Long): Double = {

    var result = 1D
    result = coefAnunciante_calc.getOrElse(cod_anunc, 1D)
    if(result == 1D){
      result = coefAnunciante_calc.getOrElse(cod_anunciante_subsidiario, 1D)
    }
    result

  }

  /************************************************************************************************************/

  def getColumn_inv_pre(spark: SparkSession, originDF: DataFrame) : DataFrame = {
    import spark.implicits._

    originDF.withColumn("inv_pre", when($"cod_fg_cadmediaset" === 0,
      ($"costebase" / $"grps_20_totales") * $"coef_evento" * $"coef_pt" * $"coef_cual" * $"coef_posic" * $"coef_cuota" * $"coef_cadena" * $"coef_forta" * $"coef_anunciante")
      .otherwise($"costebase" / $"grps_20_totales"))

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
  def getSharedGrps(spark: SparkSession): DataFrame = {
    spark.sql(
      s"""SELECT $fctd_share_grps.*
         |FROM ${fctd_share_grps.getDBTable}, ${rel_camp_trgt_may.getDBTable}, ${ordenado.getDBTable}
         |ON ($rel_camp_trgt_may.cod_target = $fctd_share_grps.cod_target AND $fctd_share_grps.cod_anuncio = $rel_camp_trgt_may.cod_anuncio
         |AND $fctd_share_grps.cod_cadena = $rel_camp_trgt_may.cod_cadena AND $ordenado.cod_grptarget_vent_gen = $fctd_share_grps.cod_target
         |AND $fctd_share_grps.cod_fg_filtrado = 0
         |AND $fctd_share_grps.cod_sect_geog = 16255
         |AND $ordenado.cod_conexion <> 81
         |AND $ordenado.cod_est_linea IN (2,3,4)
         |AND $ordenado.fg_prod_placement <> 1 )""".stripMargin)

  }

  def transformToAniomes(millis: Long): Int = {
    val year = new DateTime(millis).getYear.toString
    val month = new DateTime(millis).getMonthOfYear.toString
    year.concat(month).toInt
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
                               aniomes,
                               grps_20_totales,
                               COD_DAY_PART,
                               COD_CADENA,
                               NOM_CADENA,
                               COD_ANUNCIO,
                               NOM_ANUNCIO,
                               cod_anunciante_subsidiario,
                               nom_anunciante_subsidiario,
                               COD_PRODUCTO,
                               NOM_PRODUCTO,
                               COD_GRUPO,
                               NOM_GRUPO,
                               COD_SECTOR,
                               NOM_SECTOR,
                               COD_CUALITATIVO,
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
                               cod_fg_autonomica,
                               cod_fg_forta,
                               cod_fg_boing,
                               cod_target_compra,
                               cod_fg_campemimediaset,
                               cod_eventos,
                               nom_eventos,
                               cod_fg_anuncmediaset
                             FROM $fctm_share_inv
                              AND substr(dia_progrmd, 0, 10) >= "2019-01-01" AND substr(dia_progrmd, 0, 10) <= "2019-01-31"
    """)
    // TODO añadir filtro de fecha con los nuevos parametros
  }

  /**
    * Query de agregación sobre los campos de la tabla temporal filtrada fctm_share_inv
    * @param spark: Instanciación del objeto spark para poder acceder a sus métodos de SQL
    * @return
    */
  def getTablon_fctm_share_inv(spark: SparkSession): DataFrame = {

    val agrup_by_mediaset = spark.sql(s"""
                            SELECT
                               aniomes,
                               SUM(grps_20_totales) AS grps_20_totales,
                               COD_DAY_PART,
                               COD_CADENA,
                               NOM_CADENA,
                               COD_ANUNCIO,
                               NOM_ANUNCIO,
                               cod_anunciante_subsidiario,
                               nom_anunciante_subsidiario,
                               COD_PRODUCTO,
                               NOM_PRODUCTO,
                               COD_GRUPO,
                               NOM_GRUPO,
                               COD_SECTOR,
                               NOM_SECTOR,
                               COD_CUALITATIVO,
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
                               cod_fg_autonomica,
                               cod_fg_forta,
                               cod_fg_boing,
                               cod_target_compra,
                               cod_fg_filtrado,
                               cod_fg_campemimediaset,
                               cod_eventos,
                               nom_eventos,
                               cod_fg_anuncmediaset,
                               cod_fg_cadmediaset
                               FROM $fctm_share_inv
                               WHERE cod_fg_cadmediaset = 1
                               GROUP BY
                               dia_progrmd,
                               aniomes,
                               COD_DAY_PART,
                               COD_CADENA,
                                NOM_CADENA,
                                COD_ANUNCIO,
                                NOM_ANUNCIO,
                                cod_anunciante_subsidiario,
                                nom_anunciante_subsidiario,
                                COD_PRODUCTO,
                                NOM_PRODUCTO,
                                COD_GRUPO,
                                NOM_GRUPO,
                                COD_SECTOR,
                                NOM_SECTOR,
                                COD_CUALITATIVO,
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
                                cod_target_compra,
                                cod_eventos,
                                nom_eventos
                             """)

    val agrup_by_Nomediaset = spark.sql(s"""
                            SELECT
                               aniomes,
                               SUM(grps_20_totales) AS grps_20_totales,
                               COD_DAY_PART,
                               COD_CADENA,
                               NOM_CADENA,
                               COD_ANUNCIO,
                               NOM_ANUNCIO,
                               cod_anunciante_subsidiario,
                               nom_anunciante_subsidiario,
                               COD_PRODUCTO,
                               NOM_PRODUCTO,
                               COD_GRUPO,
                               NOM_GRUPO,
                               COD_SECTOR,
                               NOM_SECTOR,
                               COD_CUALITATIVO,
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
                               cod_fg_autonomica,
                               cod_fg_forta,
                               cod_fg_boing,
                               cod_target_compra,
                               cod_fg_filtrado,
                               cod_fg_campemimediaset,
                               cod_eventos,
                               nom_eventos,
                               cod_fg_anuncmediase,
                               cod_fg_cadmediaset
                               FROM $fctm_share_inv
                               WHERE cod_fg_cadmediaset = 0
                               GROUP BY
                               dia_progrmd,
                               aniomes,
                               COD_DAY_PART,
                               COD_CADENA,
                                NOM_CADENA,
                                COD_ANUNCIO,
                                NOM_ANUNCIO,
                                cod_anunciante_subsidiario,
                                nom_anunciante_subsidiario,
                                COD_PRODUCTO,
                                NOM_PRODUCTO,
                                COD_GRUPO,
                                NOM_GRUPO,
                                COD_SECTOR,
                                NOM_SECTOR,
                                COD_CUALITATIVO,
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
                                cod_target_compra,
                                cod_eventos,
                                nom_eventos
                             """)

    val inversion_DF = agrup_by_mediaset.union(agrup_by_Nomediaset).toDF()
    inversion_DF
  }



}
