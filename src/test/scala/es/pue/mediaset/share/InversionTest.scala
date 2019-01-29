package es.pue.mediaset.share

import org.junit.Test
import org.junit.Assert._

class InversionTest {

  // TODO Test's cod_fg_cadmediaset ----------------------------------------------------------------------

  @Test
  // Contains cod_cadena
  def FN_cod_fg_cadmediaset_t1(): Unit = {

    // cadenasMediasetGrupo_n1
    val cadenasMediasetGrupo_n1: Set[Long] = Set(100L, 5012L, 5008L)


    // fctm_share_inv
    val cod_cadena = 5008L

    assertEquals(1,  Inversion.FN_cod_fg_cadmediaset(
      cadenasMediasetGrupo_n1,
      cod_cadena))
  }


  @Test
   // NOT contains cod_cadena
  def FN_cod_fg_cadmediaset_t2(): Unit = {

    // cadenasMediasetGrupo_n1
    val cadenasMediasetGrupo_n1: Set[Long] = Set(100L, 200L, 350L)

    // fctm_share_inv
    val cod_cadena = 5008L

    assertEquals(0,  Inversion.FN_cod_fg_cadmediaset(
      cadenasMediasetGrupo_n1,
      cod_cadena))

  }


  // TODO Test's cod_fg_prodmediaset ---------------------------------------------------------------------

  @Test
  // Contains cod_cadena
  def FN_cod_fg_prodmediaset_t1(): Unit = {

    // productoMediaset_list
    val productoMediaset_list: List[Int] = List(50, 501, 5008)


    // fctm_share_inv
    val cod_producto = 50L

    assertEquals(1,  Inversion.FN_cod_fg_prodmediaset(
      productoMediaset_list,
      cod_producto))
  }


  @Test
  // NOT contains cod_cadena
  def FN_cod_fg_prodmediaset_t2(): Unit = {

    // productoMediaset_list
    val productoMediaset_list: List[Int] = List(100, 501, 5008)

    // fctm_share_inv
    val cod_producto = 50L

    assertEquals(0,  Inversion.FN_cod_fg_prodmediaset(
      productoMediaset_list,
      cod_producto))

  }


  // TODO Test's cod_fg_grupomediaset --------------------------------------------------------------------

  @Test
  // Contains cod_cadena
  def FN_cod_fg_grupomediaset_t1(): Unit = {

    // productoMediaset_list
    val grupoMediaset_list: List[Int] = List(50, 501, 5008)


    // fctm_share_inv
    val cod_grupo = 50L

    assertEquals(1,  Inversion.FN_cod_fg_grupomediaset(
      grupoMediaset_list,
      cod_grupo))
  }


  @Test
  // NOT contains cod_cadena
  def FN_cod_fg_grupomediaset_t2(): Unit = {

    // productoMediaset_list
    val grupoMediaset_list: List[Int] = List(100, 501, 5008)

    // fctm_share_inv
    val cod_grupo = 50L

    assertEquals(0,  Inversion.FN_cod_fg_grupomediaset(
      grupoMediaset_list,
      cod_grupo))

  }

  // TODO Test's importe_pase  ----------------------------------------------------------------------------


  // TODO Test's costebase  ----------------------------------------------------------------------------

  @Test
  // cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 1
  def FN_costebase_t1(): Unit = {

    // Broadcasts
    val costebase_marca = 2D
    val costebase_anunc = 10D
    val costebase_producto = 15D
    val costebase_grupo = 1D
    // Falta uno

    // fctm_share_inv
    val cod_fg_cadmediaset = 0
    val cod_fg_campemimediaset = 1
    val cod_fg_anuncmediaset = 0
    val cod_fg_prodmediaset = 0
    val cod_fg_grupomediaset = 0
    val importe_pase = 2D
    val grps_20 = 10D

        assertEquals(2D,  Inversion.FN_costebase(
          costebase_marca, costebase_anunc, costebase_producto, costebase_grupo,
          cod_fg_cadmediaset, cod_fg_campemimediaset, cod_fg_anuncmediaset,
          cod_fg_prodmediaset, cod_fg_grupomediaset, importe_pase, grps_20), 0.0)
  }

  @Test
  // cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 0 && cod_fg_anuncmediaset == 1
  def FN_costebase_t2(): Unit = {

    // Broadcasts
    val costebase_marca = 2D
    val costebase_anunc = 10D
    val costebase_producto = 15D
    val costebase_grupo = 1D
    // Falta uno

    // fctm_share_inv
    val cod_fg_cadmediaset = 0
    val cod_fg_campemimediaset = 0
    val cod_fg_anuncmediaset = 1
    val cod_fg_prodmediaset = 0
    val cod_fg_grupomediaset = 0
    val importe_pase = 2D
    val grps_20 = 10D

    assertEquals(10D,  Inversion.FN_costebase(
      costebase_marca, costebase_anunc, costebase_producto, costebase_grupo,
      cod_fg_cadmediaset, cod_fg_campemimediaset, cod_fg_anuncmediaset,
      cod_fg_prodmediaset, cod_fg_grupomediaset, importe_pase, grps_20), 0.0)
  }

  @Test
  // cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 0 && cod_fg_anuncmediaset == 0 && cod_fg_prodmediaset == 1
  def FN_costebase_t3(): Unit = {

    // Broadcasts
    val costebase_marca = 2D
    val costebase_anunc = 10D
    val costebase_producto = 15D
    val costebase_grupo = 1D
    // Falta uno

    // fctm_share_inv
    val cod_fg_cadmediaset = 0
    val cod_fg_campemimediaset = 0
    val cod_fg_anuncmediaset = 0
    val cod_fg_prodmediaset = 1
    val cod_fg_grupomediaset = 0
    val importe_pase = 2D
    val grps_20 = 10D

    assertEquals(15D,  Inversion.FN_costebase(
      costebase_marca, costebase_anunc, costebase_producto, costebase_grupo,
      cod_fg_cadmediaset, cod_fg_campemimediaset, cod_fg_anuncmediaset,
      cod_fg_prodmediaset, cod_fg_grupomediaset, importe_pase, grps_20), 0.0)
  }

  @Test
  // cod_fg_cadmediaset == 0 && cod_fg_campemimediaset == 0 && cod_fg_anuncmediaset == 0 && cod_fg_prodmediaset == 0 && cod_fg_grupomediaset == 1
  def FN_costebase_t4(): Unit = {

    // Broadcasts
    val costebase_marca = 2D
    val costebase_anunc = 10D
    val costebase_producto = 15D
    val costebase_grupo = 1D
    // Falta uno

    // fctm_share_inv
    val cod_fg_cadmediaset = 0
    val cod_fg_campemimediaset = 0
    val cod_fg_anuncmediaset = 0
    val cod_fg_prodmediaset = 0
    val cod_fg_grupomediaset = 1
    val importe_pase = 2D
    val grps_20 = 10D

    assertEquals(1D,  Inversion.FN_costebase(
      costebase_marca, costebase_anunc, costebase_producto, costebase_grupo,
      cod_fg_cadmediaset, cod_fg_campemimediaset, cod_fg_anuncmediaset,
      cod_fg_prodmediaset, cod_fg_grupomediaset, importe_pase, grps_20), 0.0)
  }


  // ------------------------------------------------------------------------------------------------------
//
//  @Test
//  // CatEventos is empty
//  def FN_coef_evento_t1() : Unit = {
//
//    // CatEventos
//    val CE_cod_evento = 123L
//    val CE_activo = "activo"
//    val CE_des_evento = "nombre de evento"
//    val CE_fecha_fin = 2L
//    val CE_fecha_ini = 0L
//    val CE_indice = "1.723"
//
//    // fctd_share_grps
//    val cod_eventos = 1L
//
//    assertEquals(1D,  Inversion.FN_coef_evento(
//      List(),
//      cod_eventos), 0.0)
//
//    // Using 0.0 as the delta is the same as using the deprecated method.
//    // The delta is intended to reflect how close the numbers can be and still be considered equal.
//    // Use values like 0.1 or 0.01 or 0.001, etc, depending on how much error the application can tolerate.
//
//  }
//
//  @Test
//  // Match encontrado se devuelve el indice
//  def FN_coef_evento_t2() : Unit = {
//
//    // CatEventos
//    val CE_cod_evento = 1L // Encaja con cod_eventos
//    val CE_activo = "activo"
//    val CE_des_evento = "nombre de evento"
//    val CE_fecha_fin = 2L
//    val CE_fecha_ini = 0L
//    val CE_indice = "2.0"
//
//    // fctd_share_grps
//    val cod_eventos = 1L
//
//    assertEquals(2.0D,  Inversion.FN_coef_evento(
//      List(CatEventos(CE_cod_evento, CE_activo, CE_des_evento, CE_fecha_fin, CE_fecha_ini, CE_indice)),
//      cod_eventos), 0.0)
//
//  }
//
//  @Test
//  // Match NO encontrado
//  def FN_coef_evento_t3() : Unit = {
//
//    // CatEventos
//    val CE_cod_evento = 10L // NO Encaja con cod_eventos
//    val CE_activo = "activo"
//    val CE_des_evento = "nombre de evento"
//    val CE_fecha_fin = 2L
//    val CE_fecha_ini = 0L
//    val CE_indice = "2.0"
//
//    // fctd_share_grps
//    val cod_eventos = 1L
//
//    assertEquals(1.0D,  Inversion.FN_coef_evento(
//      List(CatEventos(CE_cod_evento, CE_activo, CE_des_evento, CE_fecha_fin, CE_fecha_ini, CE_indice)),
//      cod_eventos), 0.0)
//
//  }
//
//
//  // TODO Test's POR_PT_MEDIASET  ----------------------------------------------------------------------------
//
//
//  // TODO Test's POR_TP_GRUPOCADENA  ----------------------------------------------------------------------------
//
//
//  // TODO Test's DIF_POR_PRIMETIME  ----------------------------------------------------------------------------
//
//  @Test
//  // Diferencia correcta
//  def FN_dif_por_primetime_t1() : Unit = {
//
//    // fctm_share_inv
//    val por_pt_grupocadena = 10.0D
//    val por_pt_mediaset = 5.0D
//
//    assertEquals(5.0D,  Inversion.FN_dif_por_primetime( por_pt_grupocadena, por_pt_mediaset), 0.0)
//
//  }
//
//
//  // -------------------------------------------------------------------------------------------------------------
//
//
//  @Test
//  // Coeficientes is empty
//  def FN_coef_pt_t1(): Unit = {
//
//    // Coeficientes
//    val CO_Anyo = "2019"
//    val CO_coeficiente = "activo"
//    val CO_des_cadena = "nombre de evento"
//    val CO_fecha_act = 10L
//    val CO_fecha_fin = 2L
//    val CO_fecha_ini = 1L
//    val CO_flag = 10L
//    val CO_indice = 2.0D
//    val CO_max_rango = 1.2D
//    val CO_Mes = "Nov"
//    val CO_min_rango = 0.5D
//
//    // fctm_share_inv
//    val dif_por_primetime = 1L
//
//    assertEquals(1D,  Inversion.FN_coef_pt(
//      List(), dif_por_primetime), 0.0)
//
//  }
//
//
//  @Test
//  // Match encontrado
//  def FN_coef_pt_t2(): Unit = {
//
//    // Coeficientes
//    val CO_Anyo = "2019"
//    val CO_coeficiente = "primeTime"
//    val CO_des_cadena = "nombre de evento"
//    val CO_fecha_act = 10L
//    val CO_fecha_fin = 2L
//    val CO_fecha_ini = 1L
//    val CO_flag = 10L
//    val CO_indice = 2.0D // Elemento a devolver
//    val CO_max_rango = 1.0D
//    val CO_Mes = "Nov"
//    val CO_min_rango = 0.0D
//
//    // fctm_share_inv
//    val dif_por_primetime = 0.5D
//
//    assertEquals(2.0D,  Inversion.FN_coef_pt(
//      List(Coeficientes(CO_Anyo, CO_coeficiente, CO_des_cadena, CO_fecha_act, CO_fecha_fin,
//        CO_fecha_ini, CO_flag, CO_indice, CO_max_rango, CO_Mes, CO_min_rango)),
//        dif_por_primetime), 0.0)
//
//  }
//
//
//  @Test
//  // Match NO encontrado
//  def FN_coef_pt_t3(): Unit = {
//
//    // Coeficientes
//    val CO_Anyo = "2019"
//    val CO_coeficiente = "no es primetime"
//    val CO_des_cadena = "nombre de evento"
//    val CO_fecha_act = 10L
//    val CO_fecha_fin = 2L
//    val CO_fecha_ini = 1L
//    val CO_flag = 10L
//    val CO_indice = 2.0D
//    val CO_max_rango = 1.2D
//    val CO_Mes = "Nov"
//    val CO_min_rango = 0.5D
//
//    // fctm_share_inv
//    val dif_por_primetime = 1L
//
//    assertEquals(1D,  Inversion.FN_coef_pt(
//      List(Coeficientes(CO_Anyo, CO_coeficiente, CO_des_cadena, CO_fecha_act, CO_fecha_fin,
//        CO_fecha_ini, CO_flag, CO_indice, CO_max_rango, CO_Mes, CO_min_rango)),
//      dif_por_primetime), 0.0)
//
//  }
//
//  // TODO Test's POR_CUALMEDIASET ----------------------------------------------------------------------------
//
//
//  // TODO Test's POR_CUALGRUPOCADENA ----------------------------------------------------------------------------
//
//
//  // TODO Test's DIF_POR_CUALITATIVOS ----------------------------------------------------------------------------
//
//  @Test
//  // Diferencia correcta
//  def FN_dif_por_cualitativos_t1() : Unit = {
//
//    // fctm_share_inv
//    val por_cualgrupocadena = 10.0D
//    val por_cualmediaset = 5.0D
//
//    assertEquals(5.0D,  Inversion.FN_dif_por_cualitativos( por_cualgrupocadena, por_cualmediaset), 0.0)
//
//  }
//
//  // TODO Test's COEF_CUAL -----------------------------------------------------------------------------------------
//
//
//  // TODO Test's COD_POSICIONADO ------------------------------------------------------------------------------------------
//
//
//  // TODO Test's POR_POSMEDIASET ------------------------------------------------------------------------------------------
//
//
//  // TODO Test's POR_POSGRUPOCADENAN2 -------------------------------------------------------------------------------------
//
//
//  // TODO Test's DIF_POR_POSICIONAMIENTO ---------------------------------------------------------------------------------------
//
//  @Test
//  // Diferencia correcta
//  def FN_dif_por_posicionamiento_t1() : Unit = {
//
//    // fctm_share_inv
//    val por_posGrupoCadenaN2 = 10.0D
//    val por_posmediaset = 5.0D
//
//    assertEquals(5.0D,  Inversion.FN_dif_por_posicionamiento( por_posGrupoCadenaN2, por_posmediaset), 0.0)
//
//  }
//
//  //  --------------------------------------------------------------------------------------------------------------------
//
//
//  @Test
//  // Coeficientes is empty
//  def FN_coef_posic_t1(): Unit = {
//
//    // Coeficientes
//    val CO_Anyo = "2019"
//    val CO_coeficiente = "Posicionado"
//    val CO_des_cadena = "nombre de evento"
//    val CO_fecha_act = 10L
//    val CO_fecha_fin = 2L
//    val CO_fecha_ini = 1L
//    val CO_flag = 10L
//    val CO_indice = 2.0D
//    val CO_max_rango = 1.2D
//    val CO_Mes = "Nov"
//    val CO_min_rango = 0.5D
//
//    // fctm_share_inv
//    val dif_por_posicionamiento = 1L
//
//    assertEquals(1D,  Inversion.FN_coef_posic(
//      List(), dif_por_posicionamiento), 0.0)
//
//  }
//
//
//  @Test
//  // Match encontrado
//  def FN_coef_posic_t2(): Unit = {
//
//    // Coeficientes
//    val CO_Anyo = "2019"
//    val CO_coeficiente = "Posicionado"
//    val CO_des_cadena = "nombre de evento"
//    val CO_fecha_act = 10L
//    val CO_fecha_fin = 2L
//    val CO_fecha_ini = 1L
//    val CO_flag = 10L
//    val CO_indice = 2.0D // Elemento a devolver
//    val CO_max_rango = 1.0D
//    val CO_Mes = "Nov"
//    val CO_min_rango = 0.0D
//
//    // fctm_share_inv
//    val dif_por_posicionamiento = 0.5D
//
//    assertEquals(2.0D,  Inversion.FN_coef_posic(
//      List(Coeficientes(CO_Anyo, CO_coeficiente, CO_des_cadena, CO_fecha_act, CO_fecha_fin,
//        CO_fecha_ini, CO_flag, CO_indice, CO_max_rango, CO_Mes, CO_min_rango)),
//      dif_por_posicionamiento), 0.0)
//
//  }
//
//
//  @Test
//  // Match NO encontrado
//  def FN_coef_posic_t3(): Unit = {
//
//    // Coeficientes
//    val CO_Anyo = "2019"
//    val CO_coeficiente = "no es posicionado"
//    val CO_des_cadena = "nombre de evento"
//    val CO_fecha_act = 10L
//    val CO_fecha_fin = 2L
//    val CO_fecha_ini = 1L
//    val CO_flag = 10L
//    val CO_indice = 2.0D
//    val CO_max_rango = 1.2D
//    val CO_Mes = "Nov"
//    val CO_min_rango = 0.5D
//
//    // fctm_share_inv
//    val dif_por_posicionamiento = 1L
//
//    assertEquals(1D,  Inversion.FN_coef_posic(
//      List(Coeficientes(CO_Anyo, CO_coeficiente, CO_des_cadena, CO_fecha_act, CO_fecha_fin,
//        CO_fecha_ini, CO_flag, CO_indice, CO_max_rango, CO_Mes, CO_min_rango)),
//      dif_por_posicionamiento), 0.0)
//
//  }
//
//  // TODO Test's CUOTA_POR_GRUPO ----------------------------------------------------------------------------------------------
//
//
//  // -----------------------------------------------------------------------------------------------------------------------
//
//  @Test
//  // Coeficientes is empty
//  def FN_coef_cuota_t1(): Unit = {
//
//    // Coeficientes
//    val CO_Anyo = "2019"
//    val CO_coeficiente = "CuoTa"
//    val CO_des_cadena = "nombre de evento"
//    val CO_fecha_act = 10L
//    val CO_fecha_fin = 2L
//    val CO_fecha_ini = 1L
//    val CO_flag = 10L
//    val CO_indice = 2.0D
//    val CO_max_rango = 1.2D
//    val CO_Mes = "Nov"
//    val CO_min_rango = 0.5D
//
//    // fctm_share_inv
//    val cuota_por_grupo = 1L
//
//    assertEquals(1D,  Inversion.FN_coef_cuota(
//      List(), cuota_por_grupo), 0.0)
//
//  }
//
//
//  @Test
//  // Match encontrado
//  def FN_coef_cuota_t2(): Unit = {
//
//    // Coeficientes
//    val CO_Anyo = "2019"
//    val CO_coeficiente = "CuoTa"
//    val CO_des_cadena = "nombre de evento"
//    val CO_fecha_act = 10L
//    val CO_fecha_fin = 2L
//    val CO_fecha_ini = 1L
//    val CO_flag = 10L
//    val CO_indice = 2.0D // Elemento a devolver
//    val CO_max_rango = 1.0D
//    val CO_Mes = "Nov"
//    val CO_min_rango = 0.0D
//
//    // fctm_share_inv
//    val cuota_por_grupo = 0.5D
//
//    assertEquals(2.0D,  Inversion.FN_coef_cuota(
//      List(Coeficientes(CO_Anyo, CO_coeficiente, CO_des_cadena, CO_fecha_act, CO_fecha_fin,
//        CO_fecha_ini, CO_flag, CO_indice, CO_max_rango, CO_Mes, CO_min_rango)),
//      cuota_por_grupo), 0.0)
//
//  }
//
//
//  @Test
//  // Match NO encontrado
//  def FN_coef_cuota_t3(): Unit = {
//
//    // Coeficientes
//    val CO_Anyo = "2019"
//    val CO_coeficiente = "no es posicionado"
//    val CO_des_cadena = "nombre de evento"
//    val CO_fecha_act = 10L
//    val CO_fecha_fin = 2L
//    val CO_fecha_ini = 1L
//    val CO_flag = 10L
//    val CO_indice = 2.0D
//    val CO_max_rango = 1.2D
//    val CO_Mes = "Nov"
//    val CO_min_rango = 0.5D
//
//    // fctm_share_inv
//    val cuota_por_grupo = 1L
//
//    assertEquals(1D,  Inversion.FN_coef_cuota(
//      List(Coeficientes(CO_Anyo, CO_coeficiente, CO_des_cadena, CO_fecha_act, CO_fecha_fin,
//        CO_fecha_ini, CO_flag, CO_indice, CO_max_rango, CO_Mes, CO_min_rango)),
//      cuota_por_grupo), 0.0)
//
//  }
//
//
//  // TODo Test's COEF_CADENA ----------------------------------------------------------------------------------------------
//
//
//  // TODO Test's NUM_CADENAS_FORTA_EMITIDO ----------------------------------------------------------------------------------------------
//
//
//  // TODO Test's COEF_FORTA ----------------------------------------------------------------------------------------------
//
//
//  // TODO Test's COEF_ANUNCIANTE ----------------------------------------------------------------------------------------------
//
//
//  // TODO Test's INV_PRE ----------------------------------------------------------------------------------------------
//
//
//  // TODO Test's COEF_CORRECTOR ----------------------------------------------------------------------------------------------
//
//
//  // TODO Test's INV_FINAL ----------------------------------------------------------------------------------------------
//
//
//  // ---------------------------------------------------------------------------------------------------------------------
//
//
//  @Test
//  // lookupColValue = 0
//  def FN_set_nom_on_column_t1(): Unit = {
//
//    assertEquals("no", Share.FN_set_nom_on_column(0L))
//
//  }
//
//  @Test
//  // lookupColValue = 0
//  def FN_set_nom_on_column_t2(): Unit = {
//
//    assertEquals("si", Share.FN_set_nom_on_column(1L))
//
//  }
//
//  @Test
//  // lookupColValue = 0
//  def FN_set_nom_on_column_t3(): Unit = {
//
//    assertEquals("no", Share.FN_set_nom_on_column(null))
//
//  }

}
