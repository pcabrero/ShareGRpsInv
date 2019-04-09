package es.pue.mediaset.share

import java.util.Properties

class InversionSQLstrs(parametrizationCfg: Properties, initialMonth: String, endMonth: String) {
  // Tablas Cloudera
  val rel_camp_trgt_may = new Entity(parametrizationCfg, "rel_camp_trgt_may" , true)
  val fcts_mercado_lineal = new Entity(parametrizationCfg, "fcts_mercado_lineal", true)
  val fctd_share_grps = new Entity(parametrizationCfg, "fctd_share_grps", true)
  val ordenado = new Entity(parametrizationCfg, "fcts_ordenado", true)

  val tb_inv_agregada = new Entity(parametrizationCfg, "tb_inv_agregada")
  val fctm_share_inv = new Entity(parametrizationCfg, "fctm_share_inv")

  // Salesforce
  val dim_agrup_cadenas = new Entity(parametrizationCfg, "dim_agrup_cadenas")
  val dim_linea_negocio = new Entity(parametrizationCfg, "dim_linea_negocio")
  val tb_parametros = new Entity(parametrizationCfg, "tb_parametros")
  val tb_configuraciones = new Entity(parametrizationCfg, "tb_configuraciones")
  val tb_eventos = new Entity(parametrizationCfg, "tb_eventos")
  val r_sectores_econ = new Entity(parametrizationCfg, "r_sectores_econ")
  val tb_coeficientes = new Entity(parametrizationCfg, "tb_coeficientes")
  val cat_eventos = new Entity(parametrizationCfg, "cat_eventos")
  val cat_coeficientes = new Entity(parametrizationCfg, "cat_coeficientes")
  val tb_inversion_agregada = new Entity(parametrizationCfg, "tb_inversion_agregada")
  val dim_conf_canales = new Entity(parametrizationCfg, "dim_conf_canales")

  val por_pt_mediaset_calc_sqlString: String = s"""SELECT in_pt.aniomes, in_pt.cod_anuncio, in_pt.cod_anunc, in_pt.cod_cadena, (in_pt.sum_campana_grps_in_pt / in_mediaset.sum_campana_grps_in_mediaset) AS por_pt_mediaset
                                        FROM (SELECT aniomes, cod_anuncio, cod_anunc, cod_cadena, SUM(grps_20_totales) AS sum_campana_grps_in_pt
                                        FROM ${fctd_share_grps.getDBTable}
                                        WHERE des_day_part = "PT"
                                        AND $fctd_share_grps.cod_cadena IN
                                        (SELECT $dim_agrup_cadenas.cod_cadena FROM ${dim_agrup_cadenas.getDBTable}
                                        WHERE cod_grupo_n1 = 20001)
                                        AND cod_day_part = 1
                                        AND cod_sect_geog = 16255
                                        AND $fctd_share_grps.cod_fg_filtrado = 0
                                        AND cod_target_compra = cod_target
                                        GROUP BY aniomes,cod_anuncio,cod_anunc,cod_cadena) AS in_pt
                                        JOIN
                                        (SELECT aniomes, cod_anuncio, cod_anunc, $fctd_share_grps.cod_cadena AS cod_cadena, SUM($fctd_share_grps.grps_20_totales) AS sum_campana_grps_in_mediaset
                                        FROM ${fctd_share_grps.getDBTable} WHERE $fctd_share_grps.cod_cadena IN (SELECT $dim_agrup_cadenas.cod_cadena FROM
                                        ${dim_agrup_cadenas.getDBTable} WHERE cod_grupo_n1 = 20001)
                                        AND $fctd_share_grps.cod_sect_geog = 16255
                                        AND $fctd_share_grps.cod_fg_filtrado = 0
                                        AND cod_target_compra = cod_target
                                        GROUP BY $fctd_share_grps.aniomes, $fctd_share_grps.cod_anuncio, $fctd_share_grps.cod_anunc, $fctd_share_grps.cod_cadena) AS in_mediaset
                                        ON in_pt.cod_cadena = in_mediaset.cod_cadena
                                        AND in_pt.aniomes = in_mediaset.aniomes
                                        AND in_pt.cod_anunc = in_mediaset.cod_anunc
                                        AND in_pt.cod_anuncio = in_mediaset.cod_anuncio
                                        """

  val por_cualmediaset_calc_sqlString: String = s"""SELECT a.cod_cadena, a.fecha_dia, (a.grps_20_totales/b.sum_grps_20_totales) AS por_cual_mediaset FROM
                                                   (SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, SUM(grps_20_totales)
                                                   AS grps_20_totales
                                                   FROM ${fctd_share_grps.getDBTable},${dim_agrup_cadenas.getDBTable}
                                                   WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
                                                   AND $fctd_share_grps.cod_cualitativo = 1
                                                   AND $fctd_share_grps.cod_sect_geog = 16255
                                                   AND $fctd_share_grps.cod_fg_filtrado = 0
                                                   AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
                                                   AND $dim_agrup_cadenas.cod_grupo_n1 = 20001
                                                   AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
                                                   GROUP BY $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini,$dim_agrup_cadenas.fecha_fin) AS a
                                                   JOIN
                                                   (SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $fctd_share_grps.fecha_dia,$dim_agrup_cadenas.cod_grupo_n1,$dim_agrup_cadenas.fecha_ini,$dim_agrup_cadenas.fecha_fin, SUM(grps_20_totales) AS sum_grps_20_totales
                                                   FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
                                                   WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
                                                   AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
                                                   AND $fctd_share_grps.cod_sect_geog = 16255
                                                   AND $fctd_share_grps.cod_fg_filtrado = 0
                                                   AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
                                                   AND $dim_agrup_cadenas.cod_grupo_n1 = 20001
                                                   GROUP BY  $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $fctd_share_grps.fecha_dia,$dim_agrup_cadenas.cod_grupo_n1,$dim_agrup_cadenas.fecha_ini,$dim_agrup_cadenas.fecha_fin) AS b
                                                    ON a.cod_cadena = b.cod_cadena
                                                    AND a.aniomes = b.aniomes
                                                    AND a.cod_grupo_n1 = b.cod_grupo_n1
                                                    AND a.fecha_dia = b.fecha_dia
                                                    AND a.fecha_ini = b.fecha_ini
                                                    AND a.fecha_fin = b.fecha_fin
                                                    """



  val por_pt_grupocadena_calc_sqlString: String =
    s"""SELECT a.cod_cadena, a.aniomes, a.cod_anuncio,a.fecha_dia,(a.grps_20_totales / b.sum_grps_20_totales) AS por_pt_grupocadena
       FROM (
       SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $fctd_share_grps.cod_anuncio, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, sum(grps_20_totales) as grps_20_totales
       FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
       WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena AND $fctd_share_grps.cod_day_part = 1 AND $fctd_share_grps.cod_sect_geog = 16255 AND $fctd_share_grps.cod_fg_filtrado = 0 AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
       GROUP BY $fctd_share_grps.cod_cadena, $fctd_share_grps.cod_anuncio, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, $fctd_share_grps.aniomes) AS a
       JOIN (
       SELECT $fctd_share_grps.cod_cadena,$fctd_share_grps.aniomes, $fctd_share_grps.cod_anuncio, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, sum(grps_20_totales) AS sum_grps_20_totales
       FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable} WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena AND $fctd_share_grps.cod_sect_geog = 16255 AND $fctd_share_grps.cod_fg_filtrado = 0 AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
       GROUP BY $fctd_share_grps.cod_cadena, $fctd_share_grps.cod_anuncio, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, $fctd_share_grps.aniomes) AS b ON a.cod_cadena = b.cod_cadena AND a.cod_anuncio = b.cod_anuncio AND a.aniomes = b.aniomes AND a.cod_grupo_n1 = b.cod_grupo_n1 AND a.fecha_dia = b.fecha_dia AND a.fecha_ini = b.fecha_ini AND a.fecha_fin = b.fecha_fin"""

  val por_cualgrupocadena_calc_sqlString: String = s"""SELECT a.cod_cadena, a.aniomes,a.fecha_dia, (a.grps_20_totales/b.sum_grps_20_totales) AS por_cualgrupocadena
                                                      FROM (
                                                      SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n2, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin,
                                                      SUM(grps_20_totales) AS grps_20_totales
                                                      FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
                                                      WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
                                                      AND $fctd_share_grps.cod_cualitativo = 1
                                                      AND  $fctd_share_grps.cod_sect_geog = 16255
                                                      AND  $fctd_share_grps.cod_target_compra =  $fctd_share_grps.cod_target
                                                      AND  $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
                                                      GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2,$fctd_share_grps.fecha_dia,$dim_agrup_cadenas.fecha_ini,$dim_agrup_cadenas.fecha_fin,$fctd_share_grps.aniomes)
                                                      AS a JOIN (
                                                      SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $fctd_share_grps.fecha_dia,$dim_agrup_cadenas.cod_grupo_n2,$dim_agrup_cadenas.fecha_ini,$dim_agrup_cadenas.fecha_fin,
                                                      SUM(grps_20_totales) AS sum_grps_20_totales
                                                      FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
                                                      WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
                                                      AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
                                                      AND $fctd_share_grps.cod_sect_geog = 16255
                                                      AND $fctd_share_grps.cod_fg_filtrado = 0
                                                      AND $fctd_share_grps.cod_target_compra =  $fctd_share_grps.cod_target
                                                      GROUP BY $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $fctd_share_grps.fecha_dia,$dim_agrup_cadenas.cod_grupo_n2,$dim_agrup_cadenas.fecha_ini,$dim_agrup_cadenas.fecha_fin) AS b
                                                      ON a.cod_cadena = b.cod_cadena
                                                      AND a.aniomes = b.aniomes
                                                      AND a.cod_grupo_n2 = b.cod_grupo_n2
                                                      AND a.fecha_dia = b.fecha_dia
                                                      AND a.fecha_ini = b.fecha_ini
                                                      AND a.fecha_fin = b.fecha_fin
       """

  val cuota_por_grupo_calc_sqlString: String =  s"""SELECT a.cod_cadena, a.aniomes, a.fecha_dia, (a.grps_20_totales / b.sum_grps_20_totales) AS cuota
                                                   FROM (
                                                   SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin,
                                                   SUM(grps_20_totales) AS grps_20_totales FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable} WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
                                                   AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
                                                   GROUP BY $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS a
                                                   JOIN(
                                                   SELECT $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin,  SUM(grps_20_totales) AS sum_grps_20_totales FROM ${fctd_share_grps.getDBTable}, ${dim_agrup_cadenas.getDBTable}
                                                   WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
                                                   AND $dim_agrup_cadenas.cod_grupo_n1 = 10001
                                                   AND $fctd_share_grps.cod_sect_geog = 16255
                                                   AND $fctd_share_grps.fecha_dia BETWEEN $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
                                                   GROUP BY $fctd_share_grps.cod_cadena, $fctd_share_grps.aniomes, $dim_agrup_cadenas.cod_grupo_n1, $fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS b
                                                   ON a.cod_cadena = b.cod_cadena AND a.aniomes = b.aniomes AND a.cod_grupo_n1 = b.cod_grupo_n1 AND a.fecha_dia = b.fecha_dia AND a.fecha_ini = b.fecha_ini AND a.fecha_fin = b.fecha_fin"""

  val cadenas_mediaset_grupo_n1_sqlString: String = s"""SELECT DISTINCT sh.cod_cadena FROM ${dim_agrup_cadenas.getDBTable} AS ag, ${fctd_share_grps.getDBTable} AS sh
                                                       | WHERE sh.cod_cadena IS NOT NULL
                                                       | AND ag.cod_cadena = sh.cod_cadena
                                                       | AND cod_grupo_n1 = 20001
                                                       | AND sh.aniomes >= concat(year(ag.fecha_ini),month(ag.fecha_ini))
                                                       | AND sh.aniomes <= concat(year(ag.fecha_fin),month(ag.fecha_fin))
       """


  val producto_mediaset_sqlString: String = s"""SELECT sh.cod_producto FROM ${dim_agrup_cadenas.getDBTable} AS ag, ${fctd_share_grps.getDBTable} AS sh
                                               | WHERE sh.cod_producto IS NOT NULL
                                               | AND ag.cod_cadena = sh.cod_cadena
                                               | AND cod_grupo_n1 = 20001
                                               | AND sh.aniomes >= concat(year(ag.fecha_ini),month(ag.fecha_ini))
                                               | AND sh.aniomes <= concat(year(ag.fecha_fin),month(ag.fecha_fin))
       """

  val grupo_mediaset_sqlString: String = s"""SELECT  sh.cod_grupo FROM ${dim_agrup_cadenas.getDBTable} AS ag, ${fctd_share_grps.getDBTable} AS sh
                                            | WHERE sh.cod_grupo IS NOT NULL
                                            | AND ag.cod_cadena = sh.cod_cadena
                                            | AND cod_grupo_n1 = 20001
                                            | AND sh.aniomes >= concat(year(ag.fecha_ini),month(ag.fecha_ini))
                                            | AND sh.aniomes <= concat(year(ag.fecha_fin),month(ag.fecha_fin))
       """

  val importe_pase_sqlString: String = s"""SELECT share.aniomes, share.cod_cadena, share.cod_target_compra, share.cod_anunc, share.cod_anunciante_subsidiario, share.cod_anuncio, share.cod_marca, IF(sum(ord.importe_pase) IS NULL, 0, sum(ord.importe_pase)) as importe_pase FROM (SELECT sh.dia_progrmd,sh.aniomes, sh.cod_cadena, sh.cod_target_compra, sh.cod_anunc, sh.cod_anunciante_subsidiario, sh.cod_anuncio, sh.cod_marca, SUM(sh.grps_20_totales) grps from ${fctd_share_grps.getDBTable} AS sh where sh.cod_sect_geog = 16255 and sh.cod_fg_filtrado = 0 and sh.cod_target=sh.cod_target_compra GROUP BY sh.dia_progrmd, sh.aniomes, sh.cod_cadena, sh.cod_target_compra, sh.cod_anunc, sh.cod_anunciante_subsidiario, sh.cod_anuncio, sh.cod_marca) share left join (SELECT orden.dia_progrmd, orden.cod_marca, orden.cod_anunc, orden.cod_grptarget_venta_gen, orden.cod_anuncio, orden.importe_pase, can.cod_cadena FROM ${ordenado.getDBTable} orden left join ${dim_conf_canales.getDBTable} can on orden.cod_conexion = can.cod_conexion WHERE orden.cod_conexion <> 81 AND cod_est_linea IN (2,3,4)) ord ON ord.cod_grptarget_venta_gen = share.cod_target_compra AND share.cod_marca = ord.cod_marca AND share.cod_anuncio = ord.cod_anuncio AND share.cod_anunc = ord.cod_anunc AND share.dia_progrmd = ord.dia_progrmd and ord.cod_cadena = share.cod_cadena group by share.aniomes, share.cod_cadena,share.cod_target_compra, share.cod_anunc, share.cod_anunciante_subsidiario, share.cod_anuncio, share.cod_marca """

  val costebase_marca_sqlString: String = s"""SELECT inv.aniomes, inv.cod_anuncio, SUM(inv.importe_pase) / SUM(inv.grps_20_totales) as costebase_marca
                                             |FROM $fctm_share_inv as inv, ${dim_agrup_cadenas.getDBTable} as dim WHERE inv.cod_cadena = dim.cod_cadena AND dim.cod_grupo_n1 = 20001  GROUP BY inv.aniomes, inv.cod_anuncio
           """
  val costebase_anunc_sqlString: String = s"""SELECT $fctm_share_inv.aniomes, $fctm_share_inv.cod_anunc, SUM($fctm_share_inv.importe_pase) / SUM($fctm_share_inv.grps_20_totales) as costebase_anunc FROM $fctm_share_inv, ${dim_agrup_cadenas.getDBTable} WHERE $fctm_share_inv.cod_cadena = $dim_agrup_cadenas.cod_cadena AND $dim_agrup_cadenas.cod_grupo_n1 = 20001 GROUP BY $fctm_share_inv.aniomes, $fctm_share_inv.cod_anunc """
  val costebase_producto_sqlString: String = s"""SELECT $fctm_share_inv.aniomes,$fctm_share_inv.cod_producto, SUM($fctm_share_inv.importe_pase) / SUM($fctm_share_inv.grps_20_totales) as costebase_producto FROM $fctm_share_inv, ${dim_agrup_cadenas.getDBTable} WHERE $fctm_share_inv.cod_cadena = $dim_agrup_cadenas.cod_cadena AND $dim_agrup_cadenas.cod_grupo_n1 = 20001 GROUP BY $fctm_share_inv.aniomes,$fctm_share_inv.cod_producto """
  val costebase_grupo_sqlString: String = s"""SELECT $fctm_share_inv.aniomes,$fctm_share_inv.cod_grupo, SUM($fctm_share_inv.importe_pase) / SUM($fctm_share_inv.grps_20_totales) as costebase_grupo FROM $fctm_share_inv, ${dim_agrup_cadenas.getDBTable} WHERE $fctm_share_inv.cod_cadena = $dim_agrup_cadenas.cod_cadena AND $dim_agrup_cadenas.cod_grupo_n1 = 20001 GROUP BY $fctm_share_inv.aniomes,$fctm_share_inv.cod_grupo """
  val costebase_else_sqlString: String = s"""SELECT $fctm_share_inv.aniomes,$fctm_share_inv.cod_anuncio,SUM($fctm_share_inv.importe_pase) / SUM($fctm_share_inv.grps_20_totales) as costebase_else FROM $fctm_share_inv, ${dim_agrup_cadenas.getDBTable} WHERE $fctm_share_inv.cod_cadena = $dim_agrup_cadenas.cod_cadena AND $dim_agrup_cadenas.cod_grupo_n1 = 20001 GROUP BY $fctm_share_inv.aniomes,$fctm_share_inv.cod_anuncio """
  val cod_cadena_disney_sqlString: String = s"""SELECT cod_cadena FROM ${dim_agrup_cadenas.getDBTable}
                                               |WHERE cod_grupo_n1 = 20003
       """
  val cod_posicion_pb2_in_posicionado_sqlString: String = s"""SELECT cod_posicion_pb2 FROM ${fctd_share_grps.getDBTable}
                                                             |WHERE cod_posicion_pb2 IN (1, 2, 3, 997, 998, 999)
       """
  val num_cadenas_forta_calc_sqlString: String = s"""
                                                    SELECT aniomes, cod_anuncio, COUNT(DISTINCT cod_cadena) AS num_cadenas_forta_emitido
                                                    FROM ${fctd_share_grps.getDBTable}
                                                    WHERE cod_fg_forta = 1
                                                    GROUP BY aniomes, cod_anuncio
       """


  val coef_forta_param_valor_sqlString: String =  s"""SELECT valor FROM tb_parametros
                                                     |WHERE nom_param = "FORTA" LIMIT 1
       """
  val indice_coeficiente_forta_sqlString: String = s"""SELECT indice FROM tb_coeficientes
                                                      |WHERE coeficiente = "FORTA" LIMIT 1
       """
  val coef_cadena_calc_1_sqlString: String = s"""
                                                |SELECT tb_coeficientes.cod_cadena, tb_coeficientes.indice as coef_cadena1
                                                |FROM tb_coeficientes, $fctm_share_inv
                                                |WHERE tb_coeficientes.coeficiente = "CADENA_N2"
                                                |AND tb_coeficientes.cod_cadena = 30006
                                                |AND $fctm_share_inv.aniomes >= year(tb_coeficientes.fecha_ini)&month(tb_coeficientes.fecha_ini)
                                                |AND $fctm_share_inv.aniomes <= year(tb_coeficientes.fecha_ini)&month(tb_coeficientes.fecha_ini)
       """
  val coef_cadena_calc_2_sqlString: String = s"""
                                                |SELECT $dim_agrup_cadenas.cod_cadena, tb_coeficientes.indice as coef_cadena2
                                                |FROM tb_coeficientes, ${dim_agrup_cadenas.getDBTable}, $fctm_share_inv
                                                |WHERE tb_coeficientes.coeficiente = "CADENA_N2"
                                                |AND tb_coeficientes.cod_cadena <> 30006
                                                |AND tb_coeficientes.cod_cadena = $dim_agrup_cadenas.cod_grupo_n2
                                                |AND $fctm_share_inv.aniomes >= year(tb_coeficientes.fecha_ini)&month(tb_coeficientes.fecha_ini)
                                                |AND $fctm_share_inv.aniomes <= year(tb_coeficientes.fecha_ini)&month(tb_coeficientes.fecha_ini)
       """
  val coef_anunciante_calc_sqlString: String = s"""
                                                  |SELECT tc.cod_cadena, tc.indice as coef_anunciante
                                                  |FROM $tb_coeficientes AS tc, $fctm_share_inv AS inv
                                                  |WHERE tc.coeficiente = "ANUNCIANTE"
                                                  |AND inv.aniomes >= year(tc.fecha_ini)&month(tc.fecha_ini)
                                                  |AND inv.aniomes <= year(tc.fecha_fin)&month(tc.fecha_fin)
       """
  val coef_evento_calc_sqlString: String = s"""SELECT DISTINCT $fctd_share_grps.cod_eventos, $cat_eventos.indice as coef_evento FROM ${fctd_share_grps.getDBTable}, $cat_eventos WHERE($fctd_share_grps.cod_eventos = $cat_eventos.cod_evento AND $fctd_share_grps.cod_eventos IS NOT NULL AND $cat_eventos.cod_evento IS NOT NULL AND $fctd_share_grps.fecha_dia >= $cat_eventos.fecha_ini AND $fctd_share_grps.fecha_dia <= $cat_eventos.fecha_fin) """

  val por_posmediaset_calc_sqlString: String = s"""SELECT in_pos.cod_cadena, in_pos.fecha_dia, (in_pos.sum_campana_grps_in_pos / in_mediaset.sum_campana_grps_in_mediaset)
                                                  AS por_posmediaset
                                                  FROM(
                                                  SELECT  $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, SUM($fctd_share_grps.grps_20_totales) AS sum_campana_grps_in_pos FROM ${fctd_share_grps.getDBTable},${dim_agrup_cadenas.getDBTable}
                                                  WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
                                                  AND $fctd_share_grps.cod_fg_posicionado in (1)
                                                  AND $fctd_share_grps.cod_sect_geog = 16255
                                                  AND $fctd_share_grps.cod_fg_filtrado = 0
                                                  AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
                                                  AND $dim_agrup_cadenas.cod_grupo_n1 = 20001
                                                  AND $fctd_share_grps.fecha_dia between $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
                                                  GROUP BY $fctd_share_grps.cod_cadena,$dim_agrup_cadenas.cod_grupo_n1,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS in_pos
                                                  JOIN(
                                                  SELECT $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, SUM($fctd_share_grps.grps_20_totales) AS sum_campana_grps_in_mediaset
                                                  FROM ${fctd_share_grps.getDBTable},${dim_agrup_cadenas.getDBTable}
                                                  WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
                                                  AND $fctd_share_grps.cod_sect_geog = 16255
                                                  AND $fctd_share_grps.cod_fg_filtrado = 0
                                                  AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
                                                  AND $dim_agrup_cadenas.cod_grupo_n1 = 20001
                                                  AND $fctd_share_grps.fecha_dia between $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
                                                  GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n1,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS in_mediaset
                                                  ON in_pos.cod_cadena = in_mediaset.cod_cadena
                                                  AND in_pos.cod_grupo_n1 = in_mediaset.cod_grupo_n1
                                                  AND in_pos.fecha_dia = in_mediaset.fecha_dia
                                                  AND in_pos.fecha_ini = in_mediaset.fecha_ini
                                                  AND in_pos.fecha_fin = in_mediaset.fecha_fin """



  val por_posgrupocadena_n1_calc_sqlString: String = s"""SELECT in_pos.cod_cadena,in_pos.fecha_dia, (in_pos.sum_campana_grps_in_pos / in_mediaset.sum_campana_grps_in_mediaset)
                                                        AS por_posGrupoCadenaN1
                                                        FROM(
                                                        SELECT  $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, SUM($fctd_share_grps.grps_20_totales) AS sum_campana_grps_in_pos FROM ${fctd_share_grps.getDBTable},${dim_agrup_cadenas.getDBTable}
                                                        WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
                                                        AND $fctd_share_grps.cod_fg_posicionado = 1
                                                        AND $fctd_share_grps.cod_sect_geog = 16255
                                                        AND $fctd_share_grps.cod_fg_filtrado = 0
                                                        AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
                                                        AND $dim_agrup_cadenas.cod_grupo_n1 = 20001
                                                        AND $fctd_share_grps.fecha_dia between $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
                                                        GROUP BY $fctd_share_grps.cod_cadena,$dim_agrup_cadenas.cod_grupo_n2,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS in_pos
                                                        JOIN(
                                                        SELECT $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin, SUM($fctd_share_grps.grps_20_totales) AS sum_campana_grps_in_mediaset
                                                        FROM ${fctd_share_grps.getDBTable},${dim_agrup_cadenas.getDBTable}
                                                        WHERE $fctd_share_grps.cod_cadena = $dim_agrup_cadenas.cod_cadena
                                                        AND $fctd_share_grps.cod_sect_geog = 16255
                                                        AND $fctd_share_grps.cod_fg_filtrado = 0
                                                        AND $fctd_share_grps.cod_target_compra = $fctd_share_grps.cod_target
                                                        AND $dim_agrup_cadenas.cod_grupo_n1 = 20001
                                                        AND $fctd_share_grps.fecha_dia between $dim_agrup_cadenas.fecha_ini AND $dim_agrup_cadenas.fecha_fin
                                                        GROUP BY $fctd_share_grps.cod_cadena, $dim_agrup_cadenas.cod_grupo_n2,$fctd_share_grps.fecha_dia, $dim_agrup_cadenas.fecha_ini, $dim_agrup_cadenas.fecha_fin) AS in_mediaset
                                                        ON in_pos.cod_cadena = in_mediaset.cod_cadena
                                                        AND in_pos.cod_grupo_n2 = in_mediaset.cod_grupo_n2
                                                        AND in_pos.fecha_dia = in_mediaset.fecha_dia
                                                        AND in_pos.fecha_ini = in_mediaset.fecha_ini
                                                        AND in_pos.fecha_fin = in_mediaset.fecha_fin
       """

  val inv_est_pe_calc_sqlString: String = s"""
                                             SELECT
                                             N1_INFOADEX.DES_GRUPO_N1 AS gr_n1_infoadex,
                                             GRPS_N2.SUMAINVN2 AS inv_est_pe
                                             -- SUBSTRING(GRPS_N2.fecha_dia, 1, 7) AS fecha_dia
                                             FROM
                                             (SELECT
                                             $dim_agrup_cadenas.cod_grupo_n2,
                                             sum($fctm_share_inv.inv_pre) AS SUMAINVN2,
                                             CAST(CONCAT(SUBSTRING($fctm_share_inv.fecha_dia, 1, 7), '-01') AS TIMESTAMP) as fecha_dia
                                             FROM $fctm_share_inv, ${dim_agrup_cadenas.getDBTable}
                                             WHERE $fctm_share_inv.COD_CADENA = $dim_agrup_cadenas.COD_CADENA
                                             AND CAST(CONCAT(SUBSTRING($fctm_share_inv.fecha_dia, 1, 7), '-01') AS TIMESTAMP) >= CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_ini, 1, 7), '-01') AS TIMESTAMP)
                                             AND CAST(CONCAT(SUBSTRING($fctm_share_inv.fecha_dia, 1, 7), '-01') AS TIMESTAMP) <= CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_fin, 1, 7), '-31') AS TIMESTAMP)
                                             AND DES_GRUPO_N0="TTV"
                                             GROUP BY $dim_agrup_cadenas.cod_grupo_n2, $fctm_share_inv.fecha_dia) AS GRPS_N2
                                             RIGHT JOIN
                                             (SELECT DES_GRUPO_N1, cod_grupo_n1, cod_grupo_n2, CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_ini, 1, 7), '-01') AS TIMESTAMP) as mesanioini,
                                             CAST(CONCAT(SUBSTRING($dim_agrup_cadenas.fecha_fin, 1, 7), '-31') AS TIMESTAMP) as mesaniofin
                                             FROM ${dim_agrup_cadenas.getDBTable}
                                             WHERE DES_GRUPO_N0 LIKE "%INFOADEX%") AS N1_INFOADEX
                                             WHERE GRPS_N2.cod_grupo_n2 = N1_INFOADEX.cod_grupo_n2
                                             AND GRPS_N2.fecha_dia >= N1_INFOADEX.mesanioini
                                             AND GRPS_N2.fecha_dia <= N1_INFOADEX.mesaniofin
       """

  val coef_corrector_DF_sqlString: String = s"""
                                               SELECT $dim_agrup_cadenas.cod_cadena AS cod_cadena1, $tb_inv_agregada.coef_corrector, CAST(CONCAT($tb_inv_agregada.anho, $tb_inv_agregada.mes) AS BIGINT) AS aniomes1
                                               FROM ${tb_inv_agregada.getDBTable} AS $tb_inv_agregada, ${dim_agrup_cadenas.getDBTable}
                                               WHERE $tb_inv_agregada.gr_n1_infoadex = $dim_agrup_cadenas.des_grupo_n1
                                               AND CONCAT($tb_inv_agregada.anho, $tb_inv_agregada.mes) >= CONCAT(YEAR(fecha_ini), MONTH(fecha_ini))
                                               AND CONCAT($tb_inv_agregada.anho, $tb_inv_agregada.mes) <= CONCAT(YEAR(fecha_fin), MONTH(fecha_fin))
       """

  val getSharedGrps_sqlString: String =
    s"""SELECT $fctd_share_grps.* FROM ${fctd_share_grps.getDBTable}, ${rel_camp_trgt_may.getDBTable}, ${ordenado.getDBTable}
       WHERE ($rel_camp_trgt_may.cod_target = $fctd_share_grps.cod_target AND $fctd_share_grps.cod_anuncio = $rel_camp_trgt_may.cod_anuncio
       AND $fctd_share_grps.cod_cadena = $rel_camp_trgt_may.cod_cadena AND $ordenado.cod_grptarget_venta_gen = $fctd_share_grps.cod_target
       AND $fctd_share_grps.cod_fg_filtrado = 0
       AND $fctd_share_grps.cod_sect_geog = 16255
       AND $ordenado.cod_conexion <> 81
       AND $ordenado.cod_est_linea IN (2,3,4)
       AND substr(fctd_share_grps.dia_progrmd, 0, 10) >= """" + initialMonth + """-01" AND substr(fctd_share_grps.dia_progrmd, 0, 10) <= """" + endMonth +"""-31" )"""

  val getTablon_fctm_share_inv_sqlString: String = s"""
                            SELECT
                               fecha_dia,
                               aniomes,
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
                               cod_fg_anuncmediaset,
                               cod_fg_cadmediaset,
                               SUM(grps_20_totales) AS grps_20_totales,
                               MAX(COD_CUALITATIVO) AS COD_CUALITATIVO,
                               MAX(cod_fg_posicionado) AS cod_fg_posicionado,
                               MAX(cod_day_part) AS cod_day_part
                               FROM $fctm_share_inv
                               WHERE cod_fg_cadmediaset = 1
                               GROUP BY
                               fecha_dia,
                               aniomes,
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
                               cod_fg_cadmediaset,
                               cod_eventos,
                               nom_eventos,
                               cod_fg_anuncmediaset
                             """

  val agrup_by_Nomediaset_sqlString: String = s"""
                            SELECT
                              fecha_dia,
                              aniomes,
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
                              cod_fg_anuncmediaset,
                              cod_fg_cadmediaset,
                              SUM(grps_20_totales) AS grps_20_totales,
                              COD_CUALITATIVO,
                              cod_fg_posicionado,
                              cod_day_part
                              FROM $fctm_share_inv
                              WHERE cod_fg_cadmediaset = 0
                              GROUP BY
                              fecha_dia,
                              aniomes,
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
                              cod_fg_cadmediaset,
                              cod_eventos,
                              nom_eventos,
                              cod_fg_anuncmediaset,
                              COD_CUALITATIVO,
                              cod_fg_posicionado,
                              cod_day_part
                             """


}

