package es.pue.mediaset.share

import es.pue.mediaset.share.Share.{Configuraciones, LineaNegocio, AgrupCadenas, relCampaniaTrgt, Eventos}
import org.junit.Test
import org.junit.Assert._

class ShareTest {

  @Test
  // LineaNegocio is empty
  def FN_cod_tp_computo_km_t1(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 0L
    val LN_des_comunicacion = 0L
    val LN_des_tipologia = 0L
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

// <<<<<<< mediaset_inversion
  //*****************************************************************************************
//
//  @Test
//  // Match no encontrado. Valor por defecto
//  def FN_cod_tp_computo_km_t1(): Unit = {
//
//    assertEquals(3005L, Share.FN_cod_tp_computo_km(
//      List(
//        LineaNegocio(1L, 13L, 2001L, 3001L, 1001L, "NORMAL", "OTROS NO SOLAPADO", 325035900, 915145200, "CONVENCIONAL", "CONVENCIONAL", "CONVENCIONAL")
//      ),
//      param_tipologias_duracion, param_duracion_iiee, fecha_dia, cod_tipologia, cod_comunicacion, duracion))
//
//  }
//
//  @Test
//  // Match encontrado && cod_tipologia IN param_tipologias_duracion && duracion < param_duracion_iiee
//  def FN_cod_tp_computo_km_t2(): Unit = {
//
//    assertEquals(3002L, Share.FN_cod_tp_computo_km(
//      List(
//        LineaNegocio(1L, 4L, 2001L, 3001L, 1001L, "NORMAL", "OTROS NO SOLAPADO", 915145200, 325035900, "CONVENCIONAL", "CONVENCIONAL", "CONVENCIONAL")
//      ),
//      param_tipologias_duracion, param_duracion_iiee, fecha_dia, 4, 1, 60))
//
//  }
//
//  @Test
//  // Match encontrado && cod_tipologia IN param_tipologias_duracion && duracion >= param_duracion_iiee
//  def FN_cod_tp_computo_km_t3(): Unit = {
//
//    assertEquals(3003L, Share.FN_cod_tp_computo_km(
//      List(
//        LineaNegocio(1L, 4L, 2001L, 3001L, 1001L, "NORMAL", "OTROS NO SOLAPADO", 915145200, 325035900, "CONVENCIONAL", "CONVENCIONAL", "CONVENCIONAL")
//      ),
//      param_tipologias_duracion, param_duracion_iiee, fecha_dia, 4, 1, 100))
//
//  }
//
//  @Test
//  // Match encontrado && cod_tipologia NOT IN param_tipologias_duracion
//  def FN_cod_tp_computo_km_t4(): Unit = {
//
//    assertEquals(3001L, Share.FN_cod_tp_computo_km(
//      List(
//        LineaNegocio(1L, 101010L, 2001L, 3001L, 1001L, "NORMAL", "OTROS NO SOLAPADO", 915145200, 325035900, "CONVENCIONAL", "CONVENCIONAL", "CONVENCIONAL")
//      ),
//      param_tipologias_duracion, param_duracion_iiee, fecha_dia, 101010, 1, duracion))
//
//  }
//
//  //*****************************************************************************************
//
//  @Test
//  // Match no encontrado. Valor por defecto
//  def FN_nom_tp_computo_km_t1(): Unit = {
//
//    assertEquals("Sin Especificar", Share.FN_nom_tp_computo_km(
//      List(
//        LineaNegocio(1L, 13L, 2001L, 3001L, 1001L, "NORMAL", "OTROS NO SOLAPADO", 325035900, 915145200, "CONVENCIONAL", "CONVENCIONAL", "CONVENCIONAL")
//      ),
//      param_tipologias_duracion, param_duracion_iiee, fecha_dia, cod_tipologia, cod_comunicacion, duracion))
//
//  }
//
//  @Test
//  // Match encontrado && cod_tipologia IN param_tipologias_duracion && duracion < param_duracion_iiee
//  def FN_nom_tp_computo_km_t2(): Unit = {
//
//    assertEquals("IIEE 12'", Share.FN_nom_tp_computo_km(
//      List(
//        LineaNegocio(1L, 4L, 2001L, 3001L, 1001L, "NORMAL", "OTROS NO SOLAPADO", 915145200, 325035900, "CONVENCIONAL", "CONVENCIONAL", "CONVENCIONAL")
//      ),
//      param_tipologias_duracion, param_duracion_iiee, fecha_dia, 4, 1, 60))
//
//  }
//
//  @Test
//  // Match encontrado && cod_tipologia IN param_tipologias_duracion && duracion > param_duracion_iiee
//  def FN_nom_tp_computo_km_t3(): Unit = {
//
//    assertEquals("IIEE 3'", Share.FN_nom_tp_computo_km(
//      List(
//        LineaNegocio(1L, 4L, 2001L, 3001L, 1001L, "NORMAL", "OTROS NO SOLAPADO", 915145200, 325035900, "CONVENCIONAL", "CONVENCIONAL", "CONVENCIONAL")
//      ),
//      param_tipologias_duracion, param_duracion_iiee, fecha_dia, 4, 1, 100))
//
//  }
//
//  @Test
//  // Match encontrado && cod_tipologia NOT IN param_tipologias_duracion
//  def FN_nom_tp_computo_km_t4(): Unit = {
//
//    assertEquals("CONVENCIONAL", Share.FN_nom_tp_computo_km(
//      List(
//        LineaNegocio(1L, 101010L, 2001L, 3001L, 1001L, "NORMAL", "OTROS NO SOLAPADO", 915145200, 325035900, "CONVENCIONAL", "CONVENCIONAL", "CONVENCIONAL")
//      ),
//      param_tipologias_duracion, param_duracion_iiee, fecha_dia, 101010, 1, duracion))
//
//  }
// =======
    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L
    val param_tipologias_duracion: Array[Int] = Array(4, 5)
    val param_duracion_iiee: Int = 100
    val duracion: Int = 90


    assertEquals(3005L, Share.FN_cod_tp_computo_km(
      List(), param_tipologias_duracion, param_duracion_iiee,
      fecha_dia, LN_cod_comunicacion, LN_cod_tipologia, duracion ))

  }

  @Test
  // Match encontrado AND duracion < param_duracion_iiee
  def FN_cod_tp_computo_km_t2(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L
    val param_tipologias_duracion: Array[Int] = Array(0, 4, 5)
    val param_duracion_iiee: Int = 100
    val duracion: Int = 90

    assertEquals(3002L, Share.FN_cod_tp_computo_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ), param_tipologias_duracion, param_duracion_iiee,
      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion, duracion ))

  }


  @Test
  // Match encontrado AND duracion >= param_duracion_iiee
  def FN_cod_tp_computo_km_t3(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L
    val param_tipologias_duracion: Array[Int] = Array(0, 4, 5)
    val param_duracion_iiee: Int = 100
    val duracion: Int = 120

    assertEquals(3003L, Share.FN_cod_tp_computo_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ), param_tipologias_duracion, param_duracion_iiee,
      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion, duracion ))

  }


  @Test
  // Match encontrado PERO param_tipologias_duracion NOT CONTAIN cod_tipologia
  def FN_cod_tp_computo_km_t4(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L
    val param_tipologias_duracion: Array[Int] = Array(4, 5)
    val param_duracion_iiee: Int = 100
    val duracion: Int = 120

    assertEquals(0L, Share.FN_cod_tp_computo_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ), param_tipologias_duracion, param_duracion_iiee,
      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion, duracion ))

  }
  //*****************************************************************************************

  @Test
  // LineaNegocio is empty
  def FN_nom_tp_computo_km_t1(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 0L
    val LN_des_comunicacion = 0L
    val LN_des_tipologia = 0L
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L
    val param_tipologias_duracion: Array[Int] = Array(4, 5)
    val param_duracion_iiee: Int = 100
    val duracion: Int = 90


    assertEquals("SIN ESPECIFICAR", Share.FN_nom_tp_computo_km(
      List(), param_tipologias_duracion, param_duracion_iiee,
      fecha_dia, LN_cod_comunicacion, LN_cod_tipologia, duracion ))

  }

  @Test
  // Match encontrado AND duracion < param_duracion_iiee
  def FN_nom_tp_computo_km_t2(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L
    val param_tipologias_duracion: Array[Int] = Array(0, 4, 5)
    val param_duracion_iiee: Int = 100
    val duracion: Int = 90

    assertEquals("IIEE 12'", Share.FN_nom_tp_computo_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ), param_tipologias_duracion, param_duracion_iiee,
      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion, duracion ))

  }

  @Test
  // Match encontrado AND duracion >= param_duracion_iiee
  def FN_nom_tp_computo_km_t3(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L
    val param_tipologias_duracion: Array[Int] = Array(0, 4, 5)
    val param_duracion_iiee: Int = 100
    val duracion: Int = 120

    assertEquals("IIEE 3'", Share.FN_nom_tp_computo_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ), param_tipologias_duracion, param_duracion_iiee,
      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion, duracion ))

  }


  @Test
  // Match encontrado PERO param_tipologias_duracion NOT CONTAIN cod_tipologia
  def FN_nom_tp_computo_km_t4(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L
    val param_tipologias_duracion: Array[Int] = Array(4, 5)
    val param_duracion_iiee: Int = 100
    val duracion: Int = 120

    assertEquals("mmm", Share.FN_nom_tp_computo_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ), param_tipologias_duracion, param_duracion_iiee,
      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion, duracion ))

  }
// >>>>>>> refactoring_inversion


  //*****************************************************************************************

  @Test
  // Configuraciones is empty
  def FN_cod_identif_franja_t1(): Unit = {

    // Configuraciones
    val Conf_cod_accion = 123L
    val Conf_cod_anunciante_kantar = 0L
    val Conf_cod_anunciante_pe = 0L
    val Conf_cod_cadena = 0L
    val Conf_cod_campana = 0L
    val Conf_cod_tipologia = 0L
    val Conf_cod_programa = 0L
    val Conf_des_accion = "NO ES FILTRAR"
    val Conf_des_anunciante_kantar = "mmm"
    val Conf_des_anunciante_pe = "GOL"
    val Conf_des_cadena = "2K GAMES"
    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
    val Conf_des_programa = "CONVENCIONAL"
    val Conf_des_tipologia = ""
    val Conf_fecha_fin = 2L
    val Conf_fecha_ini = 0L
    val Conf_iiee2_formato = ""

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anuncio = 0L
    val cod_anunc = 0L
    val cod_anunciante_subsidiario = 0L
    val cod_cadena = 999L
    val cod_programa = 0L

    assertEquals(0, Share.FN_cod_identif_franja(
      List(),
      fecha_dia, cod_programa, cod_cadena))

  }

  @Test
  // elem.des_accion != "Filtrar" AND Match encontrado
  def FN_cod_identif_franja_t2(): Unit = {

    // Configuraciones
    val Conf_cod_accion = 123L
    val Conf_cod_anunciante_kantar = 0L
    val Conf_cod_anunciante_pe = 0L
    val Conf_cod_cadena = 999L
    val Conf_cod_campana = 0L
    val Conf_cod_tipologia = 0L
    val Conf_cod_programa = 0L
    val Conf_des_accion = "NO ES FILTRAR"
    val Conf_des_anunciante_kantar = "mmm"
    val Conf_des_anunciante_pe = "GOL"
    val Conf_des_cadena = "2K GAMES"
    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
    val Conf_des_programa = "CONVENCIONAL"
    val Conf_des_tipologia = ""
    val Conf_fecha_fin = 2L
    val Conf_fecha_ini = 0L
    val Conf_iiee2_formato = ""

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anuncio = 0L
    val cod_anunc = 0L
    val cod_anunciante_subsidiario = 0L
    val cod_programa = 0L
    val cod_cadena = 999L


    assertEquals(123L, Share.FN_cod_identif_franja(
      List(
        Configuraciones(Conf_cod_accion, Conf_cod_anunciante_kantar, Conf_cod_anunciante_pe, Conf_cod_cadena,
          Conf_cod_campana, Conf_cod_programa, Conf_cod_tipologia, Conf_des_accion, Conf_des_anunciante_kantar,
          Conf_des_anunciante_pe, Conf_des_cadena, Conf_des_campana, Conf_des_programa, Conf_des_tipologia, Conf_fecha_fin, Conf_fecha_ini, Conf_iiee2_formato)
      ),
      fecha_dia, cod_programa, cod_cadena))

  }

  @Test
  // elem.des_accion != "Filtrar" AND Match NO encontrado
  def FN_cod_identif_franja_t3(): Unit = {

    // Configuraciones
    val Conf_cod_accion = 123L
    val Conf_cod_anunciante_kantar = 0L
    val Conf_cod_anunciante_pe = 0L
    val Conf_cod_cadena = 0L
    val Conf_cod_campana = 0L
    val Conf_cod_tipologia = 0L
    val Conf_cod_programa = 0L
    val Conf_des_accion = "NO ES FILTRAR"
    val Conf_des_anunciante_kantar = "mmm"
    val Conf_des_anunciante_pe = "GOL"
    val Conf_des_cadena = "2K GAMES"
    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
    val Conf_des_programa = "CONVENCIONAL"
    val Conf_des_tipologia = ""
    val Conf_fecha_fin = 2L
    val Conf_fecha_ini = 0L
    val Conf_iiee2_formato = ""

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anuncio = 0L
    val cod_anunc = 0L
    val cod_anunciante_subsidiario = 0L
    val cod_cadena = 999L
    val cod_programa = 0L

    val param_cod_campana = 1L
    val param_cod_anunciante_pe = 1L
    val param_cod_anunciante_kantar = 1L

    assertEquals(999L, Share.FN_cod_identif_franja(
      List(
        Configuraciones(Conf_cod_accion, Conf_cod_anunciante_kantar, Conf_cod_anunciante_pe, Conf_cod_cadena,
          Conf_cod_campana, Conf_cod_programa, Conf_cod_tipologia, Conf_des_accion, Conf_des_anunciante_kantar,
          Conf_des_anunciante_pe, Conf_des_cadena, Conf_des_campana, Conf_des_programa, Conf_des_tipologia, Conf_fecha_fin, Conf_fecha_ini, Conf_iiee2_formato)
      ),
      fecha_dia, cod_programa, cod_cadena))

  }

  @Test
  // elem.des_accion = "Filtrar"
  def FN_cod_identif_franja_t4(): Unit = {

    // Configuraciones
    val Conf_cod_accion = 123L
    val Conf_cod_anunciante_kantar = 0L
    val Conf_cod_anunciante_pe = 0L
    val Conf_cod_cadena = 0L
    val Conf_cod_campana = 0L
    val Conf_cod_tipologia = 0L
    val Conf_cod_programa = 0L
    val Conf_des_accion = "Filtrar"
    val Conf_des_anunciante_kantar = "mmm"
    val Conf_des_anunciante_pe = "GOL"
    val Conf_des_cadena = "2K GAMES"
    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
    val Conf_des_programa = "CONVENCIONAL"
    val Conf_des_tipologia = ""
    val Conf_fecha_fin = 2L
    val Conf_fecha_ini = 0L
    val Conf_iiee2_formato = ""

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anuncio = 0L
    val cod_anunc = 0L
    val cod_anunciante_subsidiario = 0L
    val cod_cadena = 999L
    val cod_programa = 0L

    val param_cod_campana = 1L
    val param_cod_anunciante_pe = 1L
    val param_cod_anunciante_kantar = 1L

    assertEquals(0L, Share.FN_cod_identif_franja(
      List(
        Configuraciones(Conf_cod_accion, Conf_cod_anunciante_kantar, Conf_cod_anunciante_pe, Conf_cod_cadena,
          Conf_cod_campana, Conf_cod_programa, Conf_cod_tipologia, Conf_des_accion, Conf_des_anunciante_kantar,
          Conf_des_anunciante_pe, Conf_des_cadena, Conf_des_campana, Conf_des_programa, Conf_des_tipologia, Conf_fecha_fin, Conf_fecha_ini, Conf_iiee2_formato)
      ),
      fecha_dia, cod_programa, cod_cadena))

  }

  //*****************************************************************************************

  @Test
  // Configuraciones is empty
  def FN_nom_identif_franja_t1(): Unit = {

    // Configuraciones
    val Conf_cod_accion = 123L
    val Conf_cod_anunciante_kantar = 0L
    val Conf_cod_anunciante_pe = 0L
    val Conf_cod_cadena = 0L
    val Conf_cod_campana = 0L
    val Conf_cod_tipologia = 0L
    val Conf_cod_programa = 0L
    val Conf_des_accion = "Filtrar"
    val Conf_des_anunciante_kantar = "mmm"
    val Conf_des_anunciante_pe = "GOL"
    val Conf_des_cadena = "2K GAMES"
    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
    val Conf_des_programa = "CONVENCIONAL"
    val Conf_des_tipologia = ""
    val Conf_fecha_fin = 2L
    val Conf_fecha_ini = 0L
    val Conf_iiee2_formato = ""

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anuncio = 0L
    val cod_anunc = 0L
    val cod_anunciante_subsidiario = 0L
    val cod_cadena = 999L
    val cod_programa = 0L
    val nom_cadena = "nom_cadena"

    assertEquals("Si la lista Configuraciones está vacia debe devolver el valor por defecto que será una cadena vacia",
      "", Share.FN_nom_identif_franja(
      List(),
      fecha_dia, cod_programa, cod_cadena, nom_cadena))

  }

  @Test
  // elem.des_accion != "Filtrar" AND Match encontrado
  def FN_nom_identif_franja_t2(): Unit = {

    // Configuraciones
    val Conf_cod_accion = 123L
    val Conf_cod_anunciante_kantar = 0L
    val Conf_cod_anunciante_pe = 0L
    val Conf_cod_cadena = 0L
    val Conf_cod_campana = 0L
    val Conf_cod_tipologia = 0L
    val Conf_cod_programa = 0L
    val Conf_des_accion = "NO ES FILTRAR"
    val Conf_des_anunciante_kantar = "mmm"
    val Conf_des_anunciante_pe = "GOL"
    val Conf_des_cadena = "2K GAMES"
    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
    val Conf_des_programa = "CONVENCIONAL"
    val Conf_des_tipologia = ""
    val Conf_fecha_fin = 2L
    val Conf_fecha_ini = 0L
    val Conf_iiee2_formato = ""

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anuncio = 0L
    val cod_anunc = 0L
    val cod_anunciante_subsidiario = 0L
    val cod_cadena = 0L
    val cod_programa = 0L
    val nom_cadena = "nom_cadena"

    assertEquals("NO ES FILTRAR", Share.FN_nom_identif_franja(
      List(
        Configuraciones(Conf_cod_accion, Conf_cod_anunciante_kantar, Conf_cod_anunciante_pe, Conf_cod_cadena,
          Conf_cod_campana, Conf_cod_programa, Conf_cod_tipologia, Conf_des_accion, Conf_des_anunciante_kantar,
          Conf_des_anunciante_pe, Conf_des_cadena, Conf_des_campana, Conf_des_programa, Conf_des_tipologia, Conf_fecha_fin, Conf_fecha_ini, Conf_iiee2_formato)
      ),
      fecha_dia, cod_programa, cod_cadena, nom_cadena))
  }

  @Test
  // elem.des_accion != "Filtrar" AND Match NO encontrado
  def FN_nom_identif_franja_t3(): Unit = {

    // Configuraciones
    val Conf_cod_accion = 123L
    val Conf_cod_anunciante_kantar = 0L
    val Conf_cod_anunciante_pe = 0L
    val Conf_cod_cadena = 0L
    val Conf_cod_campana = 0L
    val Conf_cod_tipologia = 0L
    val Conf_cod_programa = 0L
    val Conf_des_accion = "NO ES FILTRAR"
    val Conf_des_anunciante_kantar = "mmm"
    val Conf_des_anunciante_pe = "GOL"
    val Conf_des_cadena = "2K GAMES"
    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
    val Conf_des_programa = "CONVENCIONAL"
    val Conf_des_tipologia = ""
    val Conf_fecha_fin = 2L
    val Conf_fecha_ini = 0L
    val Conf_iiee2_formato = ""

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anuncio = 0L
    val cod_anunc = 0L
    val cod_anunciante_subsidiario = 0L
    val nom_cadena = "nom_cadena"
    val cod_cadena = 999L
    val cod_programa = 999L

    val param_cod_campana = 1L
    val param_cod_anunciante_pe = 1L
    val param_cod_anunciante_kantar = 1L

    assertEquals("elem.des_accion != 'Filtrar' AND Match NO encontrado, debe devolver el valor de nom_cadena",
      "nom_cadena", Share.FN_nom_identif_franja(
      List(
        Configuraciones(Conf_cod_accion, Conf_cod_anunciante_kantar, Conf_cod_anunciante_pe, Conf_cod_cadena,
          Conf_cod_campana, Conf_cod_programa, Conf_cod_tipologia, Conf_des_accion, Conf_des_anunciante_kantar,
          Conf_des_anunciante_pe, Conf_des_cadena, Conf_des_campana, Conf_des_programa, Conf_des_tipologia, Conf_fecha_fin, Conf_fecha_ini, Conf_iiee2_formato)
      ),
      fecha_dia, cod_programa, cod_cadena, nom_cadena))

  }

  @Test
  // elem.des_accion = "Filtrar"
  def FN_nom_identif_franja_t4(): Unit = {

    // Configuraciones
    val Conf_cod_accion = 123L
    val Conf_cod_anunciante_kantar = 0L
    val Conf_cod_anunciante_pe = 0L
    val Conf_cod_cadena = 0L
    val Conf_cod_campana = 0L
    val Conf_cod_tipologia = 0L
    val Conf_cod_programa = 0L
    val Conf_des_accion = "Filtrar"
    val Conf_des_anunciante_kantar = "mmm"
    val Conf_des_anunciante_pe = "GOL"
    val Conf_des_cadena = "2K GAMES"
    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
    val Conf_des_programa = "CONVENCIONAL"
    val Conf_des_tipologia = ""
    val Conf_fecha_fin = 2L
    val Conf_fecha_ini = 0L
    val Conf_iiee2_formato = ""

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anuncio = 0L
    val cod_anunc = 0L
    val cod_anunciante_subsidiario = 0L
    val nom_cadena = "nom_cadena"
    val cod_cadena = 999L
    val cod_programa = 0L

    val param_cod_campana = 1L
    val param_cod_anunciante_pe = 1L
    val param_cod_anunciante_kantar = 1L

    assertEquals("", Share.FN_nom_identif_franja(
      List(
        Configuraciones(Conf_cod_accion, Conf_cod_anunciante_kantar, Conf_cod_anunciante_pe, Conf_cod_cadena,
          Conf_cod_campana, Conf_cod_programa, Conf_cod_tipologia, Conf_des_accion, Conf_des_anunciante_kantar,
          Conf_des_anunciante_pe, Conf_des_cadena, Conf_des_campana, Conf_des_programa, Conf_des_tipologia, Conf_fecha_fin, Conf_fecha_ini, Conf_iiee2_formato)
      ),
      fecha_dia, cod_programa, cod_cadena, nom_cadena))

  }

  //*****************************************************************************************


  @Test
  //cod_posicion_pb2 match posiciones preferentes
  def FN_cod_fg_posicionado_t1(): Unit = {

    // Configuraciones
    val cod_posicion_pb2:Long = 997L
    val list_posiciones_preferentes = Array(1L,2L,3L,997L,998L,999L)



    assertEquals(1, Share.FN_cod_fg_posicionado(cod_posicion_pb2))

  }

  @Test
  //cod_posicion_pb2 doesnt match posiciones preferentes
  def FN_cod_fg_posicionado_t2(): Unit = {

    // Configuraciones
    val cod_posicion_pb2:java.lang.Long = 2000L

    assertEquals(0, Share.FN_cod_fg_posicionado(cod_posicion_pb2))

  }

  //*****************************************************************************************


// <<<<<<< mediaset_inversion
//
//  @Test
//  // LineaNegocio is empty
//  def FN_cod_lineanegocio_km_t1(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 0L
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 0L
//    val LN_des_comunicacion = 0L
//    val LN_des_tipologia = 0L
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee"
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//
//    assertEquals(0L, Share.FN_cod_tp_lineanegocio_km(
//      List(),
//      fecha_dia, LN_cod_comunicacion, LN_cod_tipologia ))
//
//  }
//
//  @Test
//  // Match encontrado debe devolver el valor de elem.cod_tp_lineanegocio_km
//  def FN_cod_lineanegocio_km_t2(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 0L
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 100L // Debe devolver este valor
//    val LN_des_comunicacion = "aaaa"
//    val LN_des_tipologia = "bbbb"
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee"
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//    assertEquals(100, Share.FN_cod_tp_lineanegocio_km(
//      List(
//        LineaNegocio(LN_cod_comunicacion, LN_cod_tipologia, LN_cod_tp_categr_km, LN_cod_tp_computo_km,
//          LN_cod_tp_lineanegocio_km, LN_des_comunicacion, LN_des_tipologia, LN_fecha_fin, LN_fecha_ini,
//          LN_nom_tp_categr_km, LN_nom_tp_computo_km, LN_nom_tp_lineanegocio_km)
//      ),
//      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion))
//
//  }
//
//  @Test
//  // Match NO encontrado debe devolver 1003L
//  def FN_cod_lineanegocio_km_t3(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 0L
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 100L // Debe devolver este valor
//    val LN_des_comunicacion = "aaaa"
//    val LN_des_tipologia = "bbbb"
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee"
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//    assertEquals(1003, Share.FN_cod_tp_lineanegocio_km(
//      List(
//        LineaNegocio(LN_cod_comunicacion, LN_cod_tipologia, LN_cod_tp_categr_km, LN_cod_tp_computo_km,
//          LN_cod_tp_lineanegocio_km, LN_des_comunicacion, LN_des_tipologia, LN_fecha_fin, LN_fecha_ini,
//          LN_nom_tp_categr_km, LN_nom_tp_computo_km, LN_nom_tp_lineanegocio_km)
//      ),
//      fecha_dia, cod_tipologia, cod_comunicacion))
//
//  }
//
//  //*****************************************************************************************
//
//
//  @Test
//  // LineaNegocio is empty
//  def FN_nom_lineanegocio_km_t1(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 0L
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 0L
//    val LN_des_comunicacion = 0L
//    val LN_des_tipologia = 0L
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee"
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//
//    assertEquals("", Share.FN_nom_tp_lineanegocio_km(
//      List(),
//      fecha_dia, LN_cod_comunicacion, LN_cod_tipologia ))
//
//  }
//
//  @Test
//  // Match encontrado debe devolver el valor de elem.nom_tp_lineanegocio_km
//  def FN_nom_lineanegocio_km_t2(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 0L
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 100L // Debe devolver este valor
//    val LN_des_comunicacion = "aaaa"
//    val LN_des_tipologia = "bbbb"
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee"
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//    assertEquals("GOL", Share.FN_nom_tp_lineanegocio_km(
//      List(
//        LineaNegocio(LN_cod_comunicacion, LN_cod_tipologia, LN_cod_tp_categr_km, LN_cod_tp_computo_km,
//          LN_cod_tp_lineanegocio_km, LN_des_comunicacion, LN_des_tipologia, LN_fecha_fin, LN_fecha_ini,
//          LN_nom_tp_categr_km, LN_nom_tp_computo_km, LN_nom_tp_lineanegocio_km)
//      ),
//      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion))
//
//  }
//
//  @Test
//  // Match NO encontrado debe devolver "Sin Especificar"
//  def FN_nom_lineanegocio_km_t3(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 0L
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 100L // Debe devolver este valor
//    val LN_des_comunicacion = "aaaa"
//    val LN_des_tipologia = "bbbb"
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee"
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//    assertEquals("Sin Especificar", Share.FN_nom_tp_lineanegocio_km(
//      List(
//        LineaNegocio(LN_cod_comunicacion, LN_cod_tipologia, LN_cod_tp_categr_km, LN_cod_tp_computo_km,
//          LN_cod_tp_lineanegocio_km, LN_des_comunicacion, LN_des_tipologia, LN_fecha_fin, LN_fecha_ini,
//          LN_nom_tp_categr_km, LN_nom_tp_computo_km, LN_nom_tp_lineanegocio_km)
//      ),
//      fecha_dia, cod_tipologia, cod_comunicacion))
//
//  }
//
//  //*****************************************************************************************
//
//
//  @Test
//  // LineaNegocio is empty
//  def FN_cod_tp_categr_km_t1(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 0L
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 0L
//    val LN_des_comunicacion = 0L
//    val LN_des_tipologia = 0L
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee"
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//
//    assertEquals(0, Share.FN_cod_tp_categr_km(
//      List(),
//      fecha_dia, LN_cod_comunicacion, LN_cod_tipologia ))
//
//  }
//
//  @Test
//  // Match encontrado debe devolver el valor de elem.cod_tp_categr_km
//  def FN_cod_tp_categr_km_t2(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 100L // Debe devolver este valor
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 10L
//    val LN_des_comunicacion = "aaaa"
//    val LN_des_tipologia = "bbbb"
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee"
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//    assertEquals(100, Share.FN_cod_tp_categr_km(
//      List(
//        LineaNegocio(LN_cod_comunicacion, LN_cod_tipologia, LN_cod_tp_categr_km, LN_cod_tp_computo_km,
//          LN_cod_tp_lineanegocio_km, LN_des_comunicacion, LN_des_tipologia, LN_fecha_fin, LN_fecha_ini,
//          LN_nom_tp_categr_km, LN_nom_tp_computo_km, LN_nom_tp_lineanegocio_km)
//      ),
//      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion))
//
//  }
//
//  @Test
//  // Match NO encontrado debe devolver 2006L
//  def FN_cod_tp_categr_km_t3(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 0L
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 100L
//    val LN_des_comunicacion = "aaaa"
//    val LN_des_tipologia = "bbbb"
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee"
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//    assertEquals(2006, Share.FN_cod_tp_categr_km(
//      List(
//        LineaNegocio(LN_cod_comunicacion, LN_cod_tipologia, LN_cod_tp_categr_km, LN_cod_tp_computo_km,
//          LN_cod_tp_lineanegocio_km, LN_des_comunicacion, LN_des_tipologia, LN_fecha_fin, LN_fecha_ini,
//          LN_nom_tp_categr_km, LN_nom_tp_computo_km, LN_nom_tp_lineanegocio_km)
//      ),
//      fecha_dia, cod_tipologia, cod_comunicacion))
//
//  }
//
//  //*****************************************************************************************
//
//
//  @Test
//  // LineaNegocio is empty
//  def FN_nom_tp_categr_km_t1(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 0L
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 0L
//    val LN_des_comunicacion = 0L
//    val LN_des_tipologia = 0L
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee"
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//
//    assertEquals("", Share.FN_nom_tp_categr_km(
//      List(),
//      fecha_dia, LN_cod_comunicacion, LN_cod_tipologia ))
//
//  }
//
//  @Test
//  // Match encontrado debe devolver el valor de elem.nom_tp_categr_km
//  def FN_nom_tp_categr_km_t2(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 100L
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 10L
//    val LN_des_comunicacion = "aaaa"
//    val LN_des_tipologia = "bbbb"
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee" // Debe devolver este valor
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//    assertEquals("eeee", Share.FN_nom_tp_categr_km(
//      List(
//        LineaNegocio(LN_cod_comunicacion, LN_cod_tipologia, LN_cod_tp_categr_km, LN_cod_tp_computo_km,
//          LN_cod_tp_lineanegocio_km, LN_des_comunicacion, LN_des_tipologia, LN_fecha_fin, LN_fecha_ini,
//          LN_nom_tp_categr_km, LN_nom_tp_computo_km, LN_nom_tp_lineanegocio_km)
//      ),
//      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion))
//
//  }
//
//  @Test
//  // Match NO encontrado debe devolver "Sin Especificar"
//  def FN_nom_tp_categr_km_t3(): Unit = {
//
//    // LineaNegocio
//    val LN_cod_comunicacion = 123L
//    val LN_cod_tipologia = 0L
//    val LN_cod_tp_categr_km = 0L
//    val LN_cod_tp_computo_km = 0L
//    val LN_cod_tp_lineanegocio_km = 100L
//    val LN_des_comunicacion = "aaaa"
//    val LN_des_tipologia = "bbbb"
//    val LN_fecha_fin = 2L
//    val LN_fecha_ini = 0L
//    val LN_nom_tp_categr_km = "eeee"
//    val LN_nom_tp_computo_km = "mmm"
//    val LN_nom_tp_lineanegocio_km = "GOL"
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_tipologia = 20L
//    val cod_comunicacion = 15L
//
//    assertEquals("Sin Especificar", Share.FN_nom_tp_categr_km(
//      List(
//        LineaNegocio(LN_cod_comunicacion, LN_cod_tipologia, LN_cod_tp_categr_km, LN_cod_tp_computo_km,
//          LN_cod_tp_lineanegocio_km, LN_des_comunicacion, LN_des_tipologia, LN_fecha_fin, LN_fecha_ini,
//          LN_nom_tp_categr_km, LN_nom_tp_computo_km, LN_nom_tp_lineanegocio_km)
//      ),
//      fecha_dia, cod_tipologia, cod_comunicacion))
//
//  }
// =======

  @Test
  // LineaNegocio is empty
  def FN_cod_lineanegocio_km_t1(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 0L
    val LN_des_comunicacion = 0L
    val LN_des_tipologia = 0L
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L


    assertEquals(1003L, Share.FN_cod_tp_lineanegocio_km(
      List(),
      fecha_dia, LN_cod_comunicacion, LN_cod_tipologia ))

  }

  @Test
  // Match encontrado debe devolver el valor de elem.cod_tp_lineanegocio_km
  def FN_cod_lineanegocio_km_t2(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L // Debe devolver este valor
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L

    assertEquals(100, Share.FN_cod_tp_lineanegocio_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ),
      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion))

  }

  @Test
  // Match NO encontrado debe devolver 1003L
  def FN_cod_lineanegocio_km_t3(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L // Debe devolver este valor
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L

    assertEquals(1003, Share.FN_cod_tp_lineanegocio_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ),
      fecha_dia, cod_tipologia, cod_comunicacion))

  }

  //*****************************************************************************************


  @Test
  // LineaNegocio is empty
  def FN_nom_lineanegocio_km_t1(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 0L
    val LN_des_comunicacion = 0L
    val LN_des_tipologia = 0L
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L


    assertEquals("SIN ESPECIFICAR", Share.FN_nom_tp_lineanegocio_km(
      List(),
      fecha_dia, LN_cod_comunicacion, LN_cod_tipologia ))

  }

  @Test
  // Match encontrado debe devolver el valor de elem.nom_tp_lineanegocio_km
  def FN_nom_lineanegocio_km_t2(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L // Debe devolver este valor
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L

    assertEquals("GOL", Share.FN_nom_tp_lineanegocio_km(
      List(
      LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
        LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
    ),
      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion))

  }

  @Test
  // Match NO encontrado debe devolver "SIN ESPECIFICAR"
  def FN_nom_lineanegocio_km_t3(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L // Debe devolver este valor
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L

    assertEquals("SIN ESPECIFICAR", Share.FN_nom_tp_lineanegocio_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ),
      fecha_dia, cod_tipologia, cod_comunicacion))

  }

  //*****************************************************************************************


  @Test
  // LineaNegocio is empty
  def FN_cod_tp_categr_km_t1(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 0L
    val LN_des_comunicacion = 0L
    val LN_des_tipologia = 0L
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L


    assertEquals(2006L, Share.FN_cod_tp_categr_km(
      List(),
      fecha_dia, LN_cod_comunicacion, LN_cod_tipologia ))

  }

  @Test
  // Match encontrado debe devolver el valor de elem.cod_tp_categr_km
  def FN_cod_tp_categr_km_t2(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 100L // Debe devolver este valor
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 10L
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L

    assertEquals(100, Share.FN_cod_tp_categr_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ),
      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion))

  }

  @Test
  // Match NO encontrado debe devolver 2006L
  def FN_cod_tp_categr_km_t3(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L

    assertEquals(2006, Share.FN_cod_tp_categr_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ),
      fecha_dia, cod_tipologia, cod_comunicacion))

  }

  //*****************************************************************************************


  @Test
  // LineaNegocio is empty
  def FN_nom_tp_categr_km_t1(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 0L
    val LN_des_comunicacion = 0L
    val LN_des_tipologia = 0L
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L


    assertEquals("SIN ESPECIFICAR", Share.FN_nom_tp_categr_km(
      List(),
      fecha_dia, LN_cod_comunicacion, LN_cod_tipologia ))

  }

  @Test
  // Match encontrado debe devolver el valor de elem.nom_tp_categr_km
  def FN_nom_tp_categr_km_t2(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 100L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 10L
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee" // Debe devolver este valor
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L

    assertEquals("eeee", Share.FN_nom_tp_categr_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ),
      fecha_dia, LN_cod_tipologia, LN_cod_comunicacion))

  }

  @Test
  // Match NO encontrado debe devolver "SIN ESPECIFICAR"
  def FN_nom_tp_categr_km_t3(): Unit = {

    // LineaNegocio
    val LN_cod_comunicacion = 123L
    val LN_cod_tipologia = 0L
    val LN_cod_tp_categr_km = 0L
    val LN_cod_tp_computo_km = 0L
    val LN_cod_tp_lineanegocio_km = 100L
    val LN_des_comunicacion = "aaaa"
    val LN_des_tipologia = "bbbb"
    val LN_fecha_fin = 2L
    val LN_fecha_ini = 0L
    val LN_nom_tp_categr_km = "eeee"
    val LN_nom_tp_computo_km = "mmm"
    val LN_nom_tp_lineanegocio_km = "GOL"

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_tipologia = 20L
    val cod_comunicacion = 15L

    assertEquals("SIN ESPECIFICAR", Share.FN_nom_tp_categr_km(
      List(
        LineaNegocio(LN_cod_tp_categr_km, LN_cod_tipologia, LN_cod_tp_lineanegocio_km, LN_fecha_fin, LN_nom_tp_lineanegocio_km,
          LN_des_comunicacion, LN_des_tipologia, LN_nom_tp_categr_km, LN_fecha_ini, LN_cod_comunicacion, LN_nom_tp_computo_km, LN_cod_tp_computo_km)
      ),
      fecha_dia, cod_tipologia, cod_comunicacion))

  }
// >>>>>>> refactoring_inversion

  //*****************************************************************************************

  @Test
  // AgrupCadenas is empty
  def FN_cod_fg_autonomica_t1(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = "n1"
    val AC_des_grupo_n2 = "n2"
    val AC_des_grupo_n0 = "n0"
    val AC_cod_forta = 0L
    val AC_des_forta = "forta"
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_cod_grupo_n2 = 2L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = "eeee"
    val AC_fecha_fin = 2L
    val AC_fecha_ini = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 10L


    assertEquals(0L, Share.FN_cod_fg_autonomica(
      List(), List[Long](5L, 10L, 3L, 4L),
      fecha_dia, cod_cadena ))

  }

  @Test
  // cod_cadena está dentro de las cadenas autonómicas Match encontrado
  def FN_cod_fg_autonomica_t2(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = "n1"
    val AC_des_grupo_n2 = "n2"
    val AC_des_grupo_n0 = "n0"
    val AC_cod_forta = 0L
    val AC_des_forta = "forta"
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_cod_grupo_n2 = 0L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = "eeee"
    val AC_fecha_fin = 2L
    val AC_fecha_ini = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 10L

    assertEquals(1, Share.FN_cod_fg_autonomica(
      List(
        AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta,
          AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)
      ), List[Long](5L, 10L, 3L, 4L),
      fecha_dia, cod_cadena ))

  }

  @Test
  // cod_cadena  NO está dentro de las cadenas autonómicas Match  no encontrado
  def FN_cod_fg_autonomica_t3(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = "n1"
    val AC_des_grupo_n2 = "n2"
    val AC_des_grupo_n0 = "n0"
    val AC_cod_forta = 0L
    val AC_des_forta = "forta"
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_cod_grupo_n2 = 400L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = "eeee"
    val AC_fecha_fin = 2L
    val AC_fecha_ini = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 10L

    assertEquals(0, Share.FN_cod_fg_autonomica(
      List(
        AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta,
          AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)
      ), List[Long](0L, 5L, 9L, 100L),
      fecha_dia, cod_cadena ))

  }

  @Test
  // cod_cadena está dentro de las cadenas autonómicas Pero las fechas no coinciden
  def FN_cod_fg_autonomica_t4(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = "n1"
    val AC_des_grupo_n2 = "n2"
    val AC_des_grupo_n0 = "n0"
    val AC_cod_forta = 0L
    val AC_des_forta = "forta"
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_cod_grupo_n2 = 30L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = "eeee"
    val AC_fecha_fin = 2L
    val AC_fecha_ini = 0L

    // fctd_share_grps
    val fecha_dia = 5L
    val cod_cadena = 10L

    assertEquals(0, Share.FN_cod_fg_autonomica(
      List(
        AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta,
          AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)
      ), List[Long](5L, 3L, 10L),
      fecha_dia, cod_cadena ))

  }

  //*****************************************************************************************

  @Test
  // AgrupCadenas is empty
  def FN_cod_fg_forta_t1(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = "n1"
    val AC_des_grupo_n2 = "n2"
    val AC_des_grupo_n0 = "n0"
    val AC_cod_forta = 0L
    val AC_des_forta = "forta"
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_cod_grupo_n2 = 2L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = "eeee"
    val AC_fecha_fin = 2L
    val AC_fecha_ini = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 10L

    assertEquals(0, Share.FN_cod_fg_forta(
      List(), List[Long](5L, 10L, 3L),
      fecha_dia, cod_cadena ))

  }

  @Test
  // cod_cadena está dentro de los códigos de cadenas forta Match encontrado
  def FN_cod_fg_forta_t2(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = "n1"
    val AC_des_grupo_n2 = "n2"
    val AC_des_grupo_n0 = "n0"
    val AC_cod_forta = 2L
    val AC_des_forta = "forta"
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_cod_grupo_n2 = 306L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = "eeee"
    val AC_fecha_fin = 2L
    val AC_fecha_ini = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 10L

    assertEquals(1, Share.FN_cod_fg_forta(
      List(
        AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta,
          AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)
      ), List[Long](0L, 5L, 10L),
      fecha_dia, cod_cadena ))

  }

  @Test
  // cod_cadena NO está dentro de los códigos de cadenas forta Match  NO encontrado
  def FN_cod_fg_forta_t3(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = "n1"
    val AC_des_grupo_n2 = "n2"
    val AC_des_grupo_n0 = "n0"
    val AC_cod_forta = 1000L
    val AC_des_forta = "forta"
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_cod_grupo_n2 = 30006L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = "eeee"
    val AC_fecha_fin = 2L
    val AC_fecha_ini = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 10L

    assertEquals(0, Share.FN_cod_fg_forta(
      List(
        AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta,
          AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)
      ), List[Long](100L, 5L, 200L),
      fecha_dia, cod_cadena ))

  }

  @Test
  // cod_cadena está dentro de las cadenas autonómicas Pero las fechas no coinciden
  def FN_cod_fg_forta_t4(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = "n1"
    val AC_des_grupo_n2 = "n2"
    val AC_des_grupo_n0 = "n0"
    val AC_cod_forta = 1L
    val AC_des_forta = "forta"
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_cod_grupo_n2 = 30006L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = "eeee"
    val AC_fecha_fin = 2L
    val AC_fecha_ini = 0L

    // fctd_share_grps
    val fecha_dia = 3L
    val cod_cadena = 10L

    assertEquals(0, Share.FN_cod_fg_forta(
      List(
        AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta,
          AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)
      ), List[Long](10L, 1L, 5000L),
      fecha_dia, cod_cadena ))

  }

  //*****************************************************************************************

  @Test
  // AgrupCadenas is empty
  def FN_cod_fg_anuncmediaset_t1(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = "n1"
    val AC_des_grupo_n2 = "n2"
    val AC_des_grupo_n0 = "n0"
    val AC_cod_forta = 0L
    val AC_des_forta = "forta"
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_cod_grupo_n2 = 2L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = "eeee"
    val AC_fecha_fin = 2L
    val AC_fecha_ini = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 10L
    val cod_anunciante_subsidiario = 15L


    assertEquals(-1, Share.FN_cod_fg_anuncmediaset(
      List(), List(10L, 50L, 30L),
      fecha_dia, cod_anunciante_subsidiario, cod_cadena ))

  }

  @Test
  // Match encontrado
  def FN_cod_fg_anuncmediaset_t2(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = "n1"
    val AC_des_grupo_n2 = "n2"
    val AC_des_grupo_n0 = "n0"
    val AC_cod_forta = 0L
    val AC_des_forta = "forta"
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_cod_grupo_n2 = 2L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = "eeee"
    val AC_fecha_fin = 2L
    val AC_fecha_ini = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 10L // Esta incluido entre los códigos de cadena de campemimediaset
    val cod_anunciante_subsidiario = 15L


    assertEquals(1, Share.FN_cod_fg_anuncmediaset(
      List(
        AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta,
          AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)
      ), List(10L, 50L, 30L),
      fecha_dia, cod_anunciante_subsidiario, cod_cadena ))

  }

  @Test
  // Match NO encontrado
  def FN_cod_fg_anuncmediaset_t3(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = "n1"
    val AC_des_grupo_n2 = "n2"
    val AC_des_grupo_n0 = "n0"
    val AC_cod_forta = 0L
    val AC_des_forta = "forta"
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_cod_grupo_n2 = 2L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = "eeee"
    val AC_fecha_fin = 2L
    val AC_fecha_ini = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 100L // NO esta incluido entre los códigos de cadena de campemimediaset
    val cod_anunciante_subsidiario = 15L


    assertEquals(0, Share.FN_cod_fg_anuncmediaset(
      List(
        AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta,
          AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)
      ), List(10L, 50L, 30L),
      fecha_dia, cod_anunciante_subsidiario, cod_cadena ))

  }

  //*****************************************************************************************

  @Test
  // lookupColValue = 0
  def FN_set_nom_on_column_t1(): Unit = {

    assertEquals("no", Share.FN_set_nom_on_column(0L))

  }

  @Test
  // lookupColValue = 0
  def FN_set_nom_on_column_t2(): Unit = {

    assertEquals("si", Share.FN_set_nom_on_column(1L))

  }

  @Test
  // lookupColValue = 0
  def FN_set_nom_on_column_t3(): Unit = {

    assertEquals("no", Share.FN_set_nom_on_column(null))

  }

  //*****************************************************************************************

  @Test
  // AgrupCadenas list is empty
  def FN_cod_fg_boing_t1(): Unit = {

    // Codigo de cadenas boing
    val codigos_de_cadenas_boing_list = List(555L, 666L, 777L)

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anuncio = 0L
    val cod_cadena = 999L

    assertEquals(0, Share.FN_cod_fg_boing(
      List(),
      codigos_de_cadenas_boing_list, fecha_dia, cod_cadena, cod_anuncio ))

  }

  @Test
  // AgrupCadenas list is NOT empty.  codigos_de_cadenas_boing_list.contains(cod_anuncio) && fecha_dia > elem.fecha_ini && fecha_dia < elem.fecha_fin
  def FN_cod_fg_boing_t2(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = ""
    val AC_des_grupo_n2 = ""
    val AC_des_grupo_n0 = ""
    val AC_fecha_fin = 2L
    val AC_cod_forta = 0L
    val AC_des_forta = ""
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_fecha_ini = 0L
    val AC_cod_grupo_n2 = 0L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = ""

    // Codigo de cadenas boing
    val codigos_de_cadenas_boing_list = List(555L, 666L, 777L)

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anuncio = 555L
    val cod_cadena = 999L

    assertEquals(1, Share.FN_cod_fg_boing(
      List(AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta, AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)),
      codigos_de_cadenas_boing_list, fecha_dia, cod_cadena, cod_anuncio ))

  }

  @Test
  // AgrupCadenas list is NOT empty.  !codigos_de_cadenas_boing_list.contains(cod_anuncio) && fecha_dia > elem.fecha_ini && fecha_dia < elem.fecha_fin
  def FN_cod_fg_boing_t3(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = ""
    val AC_des_grupo_n2 = ""
    val AC_des_grupo_n0 = ""
    val AC_fecha_fin = 2L
    val AC_cod_forta = 0L
    val AC_des_forta = ""
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_fecha_ini = 0L
    val AC_cod_grupo_n2 = 0L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = ""

    // Codigo de cadenas boing
    val codigos_de_cadenas_boing_list = List(555L, 666L, 777L)

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anuncio = 555666777L
    val cod_cadena = 999L

    assertEquals(0, Share.FN_cod_fg_boing(
      List(AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta, AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)),
      codigos_de_cadenas_boing_list, fecha_dia, cod_cadena, cod_anuncio ))

  }

  //*****************************************************************************************
//
//  @Test
//  // relCampaniaTrgt list is empty
//  def FN_cod_target_compra_t1(): Unit = {
//
//    // relCampaniaTrgt
//    val Rel_cod_anuncio = 0L
//    val Rel_nom_anuncio  = ""
//    val Rel_cod_cadena  = 0L
//    val Rel_nom_cadena  = ""
//    val Rel_cod_target = 0L
//    val Rel_cod_target_gen_may = 0L
//    val Rel_nom_target_gen_may = ""
//    val Rel_fecha_hora = 0L
//    val Rel_origen_datos = ""
//
//    // Codigo de cadenas boing
////    val codigos_de_cadenas_boing_list = List(555L, 666L, 777L)
//    val codigos_de_cadenas_boing_list = List()
//
//    // fctd_share_grps
////    val fecha_dia = 1L
//    val cod_cadena = 999L
//    val cod_anuncio = 555L
//
//    assertEquals(0, Share.FN_cod_target_compra(
//      List(relCampaniaTrgt(Rel_cod_anuncio, Rel_nom_anuncio, Rel_cod_cadena, Rel_nom_cadena, Rel_cod_target, Rel_cod_target_gen_may, Rel_nom_target_gen_may, Rel_fecha_hora, Rel_origen_datos))
//      , cod_cadena, cod_anuncio ))
//
//  }
//
//  @Test
//  // relCampaniaTrgt list is NOT empty. MATCH => elem.cod_anuncio == cod_anuncio && elem.cod_cadena == cod_cadena
//  def FN_cod_target_compra_t2(): Unit = {
//
//    // relCampaniaTrgt
//    val Rel_cod_anuncio = 555L
//    val Rel_nom_anuncio  = ""
//    val Rel_cod_cadena  = 999L
//    val Rel_nom_cadena  = ""
//    val Rel_cod_target = 888L
//    val Rel_cod_target_gen_may = 0L
//    val Rel_nom_target_gen_may = ""
//    val Rel_fecha_hora = 0L
//    val Rel_origen_datos = ""
//
//    // Codigo de cadenas boing
//    val codigos_de_cadenas_boing_list = List(555L, 666L, 777L)
//
//    // fctd_share_grps
//    val cod_cadena = 999L
//    val cod_anuncio = 555L
//
//    assertEquals(888L, Share.FN_cod_target_compra(
//      List(relCampaniaTrgt(Rel_cod_anuncio, Rel_nom_anuncio, Rel_cod_cadena, Rel_nom_cadena, Rel_cod_target, Rel_cod_target_gen_may, Rel_nom_target_gen_may, Rel_fecha_hora, Rel_origen_datos))
//      , cod_anuncio, cod_cadena ))
//
//  }
//
//  @Test
//  // relCampaniaTrgt list is NOT empty. DOESNT MATCH => elem.cod_anuncio == cod_anuncio && elem.cod_cadena == cod_cadena
//  def FN_cod_target_compra_t3(): Unit = {
//
//    // relCampaniaTrgt
//    val Rel_cod_anuncio = 555L
//    val Rel_nom_anuncio  = ""
//    val Rel_cod_cadena  = 999L
//    val Rel_nom_cadena  = ""
//    val Rel_cod_target = 888L
//    val Rel_cod_target_gen_may = 0L
//    val Rel_nom_target_gen_may = ""
//    val Rel_fecha_hora = 0L
//    val Rel_origen_datos = ""
//
//    // Codigo de cadenas boing
//    val codigos_de_cadenas_boing_list = List(555L, 666L, 777L)
//
//    // fctd_share_grps
//    val cod_cadena = 999L
//    val cod_anuncio = 555666L
//
//    assertEquals(0L, Share.FN_cod_target_compra(
//      List(relCampaniaTrgt(Rel_cod_anuncio, Rel_nom_anuncio, Rel_cod_cadena, Rel_nom_cadena, Rel_cod_target, Rel_cod_target_gen_may, Rel_nom_target_gen_may, Rel_fecha_hora, Rel_origen_datos))
//      , cod_cadena, cod_anuncio ))
//
//  }

  //*****************************************************************************************
//
//  @Test
//  // Configuraciones list is empty.
//  def FN_cod_fg_filtrado_t1(): Unit = {
//
//    // Configuraciones
//    val Conf_cod_accion = 123L
//    val Conf_cod_anunciante_kantar = 0L
//    val Conf_cod_anunciante_pe = 0L
//    val Conf_cod_cadena = 0L
//    val Conf_cod_campana = 0L
//    val Conf_cod_programa = 0L
//    val Conf_cod_tipologia = 0L
//    val Conf_des_accion = "Filtrar"
//    val Conf_des_anunciante_kantar = "mmm"
//    val Conf_des_anunciante_pe = "GOL"
//    val Conf_des_cadena = "2K GAMES"
//    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
//    val Conf_des_programa = "CONVENCIONAL"
//    val Conf_des_tipologia = ""
//    val Conf_fecha_fin = 2L
//    val Conf_fecha_ini = 0L
//    val Conf_iiee2_formato = ""
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_anunc = 0L
//    val cod_anunciante_subsidiario = 0L
//    val cod_anuncio = 555L
//    val cod_cadena = 999L
//    val cod_programa = 0L
//    val cod_tipologia = 0L
//
//    assertEquals(0L, Share.FN_cod_fg_filtrado(
//      List()
//      , fecha_dia, cod_anunc, cod_anunciante_subsidiario, cod_anuncio, cod_cadena, cod_programa, cod_tipologia))
//
//  }
//
//  @Test
//  // Configuraciones list is NOT empty. Everything MATCHs
//  def FN_cod_fg_filtrado_t2(): Unit = {
//
//    // Configuraciones
//    val Conf_cod_accion = 123L
//    val Conf_cod_anunciante_kantar = 0L
//    val Conf_cod_anunciante_pe = 0L
//    val Conf_cod_cadena = 0L
//    val Conf_cod_campana = null
//    val Conf_cod_programa = 0L
//    val Conf_cod_tipologia = 0L
//    val Conf_des_accion = "Filtrar"
//    val Conf_des_anunciante_kantar = "mmm"
//    val Conf_des_anunciante_pe = "GOL"
//    val Conf_des_cadena = "2K GAMES"
//    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
//    val Conf_des_programa = "CONVENCIONAL"
//    val Conf_des_tipologia = ""
//    val Conf_fecha_fin = 2L
//    val Conf_fecha_ini = 0L
//    val Conf_iiee2_formato = ""
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_anunc = 0L
//    val cod_anunciante_subsidiario = 0L
//    val cod_anuncio = 0L
//    val cod_cadena = 0L
//    val cod_programa = null
//    val cod_tipologia = 0L
//
//    assertEquals(1L, Share.FN_cod_fg_filtrado(
//      List(Configuraciones(Conf_cod_accion, Conf_cod_anunciante_kantar, Conf_cod_anunciante_pe, Conf_cod_cadena, Conf_cod_campana, Conf_cod_programa, Conf_cod_tipologia, Conf_des_accion, Conf_des_anunciante_kantar,
//        Conf_des_anunciante_pe, Conf_des_cadena, Conf_des_campana, Conf_des_programa, Conf_des_tipologia, Conf_fecha_fin, Conf_fecha_ini, Conf_iiee2_formato))
//      , fecha_dia, cod_anunc, cod_anunciante_subsidiario, cod_anuncio, cod_cadena, cod_programa, cod_tipologia))
//
//  }
//
//  @Test
//  // Configuraciones list is NOT empty. des_accion != "FILTRAR" no hace match con ninguna condicion
//  def FN_cod_fg_filtrado_t3(): Unit = {
//
//    // Configuraciones
//    val Conf_cod_accion = 123L
//    val Conf_cod_anunciante_kantar = 230L
//    val Conf_cod_anunciante_pe = 100L
//    val Conf_cod_cadena = 0L
//    val Conf_cod_campana = 0L
//    val Conf_cod_programa = 0L
//    val Conf_cod_tipologia = 0L
//    val Conf_des_accion = "NO ES Filtrar"
//    val Conf_des_anunciante_kantar = "mmm"
//    val Conf_des_anunciante_pe = "GOL"
//    val Conf_des_cadena = "2K GAMES"
//    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
//    val Conf_des_programa = "CONVENCIONAL"
//    val Conf_des_tipologia = ""
//    val Conf_fecha_fin = 2L
//    val Conf_fecha_ini = 0L
//    val Conf_iiee2_formato = ""
//
//    // fctd_share_grps
//    val fecha_dia = 1L
//    val cod_anunc = 0L
//    val cod_anunciante_subsidiario = 20L
//    val cod_anuncio = 0L
//    val cod_cadena = 0L
//    val cod_programa = 0L
//    val cod_tipologia = 0L
//
//    assertEquals(0L, Share.FN_cod_fg_filtrado(
//      List(Configuraciones(Conf_cod_accion, Conf_cod_anunciante_kantar, Conf_cod_anunciante_pe, Conf_cod_cadena, Conf_cod_campana, Conf_cod_programa, Conf_cod_tipologia, Conf_des_accion, Conf_des_anunciante_kantar,
//        Conf_des_anunciante_pe, Conf_des_cadena, Conf_des_campana, Conf_des_programa, Conf_des_tipologia, Conf_fecha_fin, Conf_fecha_ini, Conf_iiee2_formato))
//      , fecha_dia, cod_anunc, cod_anunciante_subsidiario, cod_anuncio, cod_cadena, cod_programa, cod_tipologia))
//
//  }
//
//  @Test
//  // Configuraciones list is NOT empty. fecha_dia no coincide en el rango de fechas
//  def FN_cod_fg_filtrado_t4(): Unit = {
//
//    // Configuraciones
//    val Conf_cod_accion = 123L
//    val Conf_cod_anunciante_kantar = 0L
//    val Conf_cod_anunciante_pe = 0L
//    val Conf_cod_cadena = 0L
//    val Conf_cod_campana = null
//    val Conf_cod_programa = 0L
//    val Conf_cod_tipologia = 0L
//    val Conf_des_accion = "NO ES Filtrar"
//    val Conf_des_anunciante_kantar = "mmm"
//    val Conf_des_anunciante_pe = "GOL"
//    val Conf_des_cadena = "2K GAMES"
//    val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
//    val Conf_des_programa = "CONVENCIONAL"
//    val Conf_des_tipologia = ""
//    val Conf_fecha_fin = 2L
//    val Conf_fecha_ini = 0L
//    val Conf_iiee2_formato = ""
//
//    // fctd_share_grps
//    val fecha_dia = 5L
//    val cod_anunc = 0L
//    val cod_anunciante_subsidiario = 0L
//    val cod_anuncio = 0L
//    val cod_cadena = 0L
//    val cod_programa = 0L
//    val cod_tipologia = 0L
//
//    assertEquals(0L, Share.FN_cod_fg_filtrado(
//      List(Configuraciones(Conf_cod_accion, Conf_cod_anunciante_kantar, Conf_cod_anunciante_pe, Conf_cod_cadena, Conf_cod_campana, Conf_cod_programa, Conf_cod_tipologia, Conf_des_accion, Conf_des_anunciante_kantar,
//        Conf_des_anunciante_pe, Conf_des_cadena, Conf_des_campana, Conf_des_programa, Conf_des_tipologia, Conf_fecha_fin, Conf_fecha_ini, Conf_iiee2_formato))
//      , fecha_dia, cod_anunc, cod_anunciante_subsidiario, cod_anuncio, cod_cadena, cod_programa, cod_tipologia))
//
//  }

//    @Test
//    // Configuraciones list is NOT empty. fecha_dia no coincide en el rango de fechas
//    def FN_cod_fg_filtrado_t4(): Unit = {
//
//      // Configuraciones
//      val Conf_cod_accion = 123L
//      val Conf_cod_anunciante_kantar = 0L
//      val Conf_cod_anunciante_pe = 0L
//      val Conf_cod_cadena = 0L
//      val Conf_cod_campana = null
//      val Conf_cod_programa = null
//      val Conf_cod_tipologia = 0L
//      val Conf_des_accion = "NO ES Filtrar"
//      val Conf_des_anunciante_kantar = "mmm"
//      val Conf_des_anunciante_pe = "GOL"
//      val Conf_des_cadena = "2K GAMES"
//      val Conf_des_campana = "FUTBOL:INTERNATIONAL CHAMPIONS CUP(D)"
//      val Conf_des_programa = "CONVENCIONAL"
//      val Conf_des_tipologia = ""
//      val Conf_fecha_fin = 2L
//      val Conf_fecha_ini = 0L
//      val Conf_iiee2_formato = ""
//
//      // fctd_share_grps
//      val fecha_dia = 5L
//      val cod_anunc = 0L
//      val cod_anunciante_subsidiario = 0L
//      val cod_anuncio = 0L
//      val cod_cadena = 0L
//      val cod_programa = null
//      val cod_tipologia = 0L
//
//      assertEquals(0L, Share.FN_cod_fg_filtrado(
//        List(Configuraciones(Conf_cod_accion, Conf_cod_anunciante_kantar, Conf_cod_anunciante_pe, Conf_cod_cadena, Conf_cod_campana, Conf_cod_programa, Conf_cod_tipologia, Conf_des_accion, Conf_des_anunciante_kantar,
//          Conf_des_anunciante_pe, Conf_des_cadena, Conf_des_campana, Conf_des_programa, Conf_des_tipologia, Conf_fecha_fin, Conf_fecha_ini, Conf_iiee2_formato))
//        , fecha_dia, cod_anunc, cod_anunciante_subsidiario, cod_anuncio, cod_cadena, cod_programa, cod_tipologia))
//
//    }

//  //*****************************************************************************************

  @Test
  // AgrupCadenas list is empty.
  def FN_cod_fg_campemimediaset_t1(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = ""
    val AC_des_grupo_n2 = ""
    val AC_des_grupo_n0 = ""
    val AC_fecha_fin = 2L
    val AC_cod_forta = 0L
    val AC_des_forta = ""
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_fecha_ini = 0L
    val AC_cod_grupo_n2 = 0L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = ""

    // Codigo de cadenas
    val codigos_de_cadenas_campemimediaset = List(555L, 666L, 777L)

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anunciante_subsidiario = 555L
    val cod_cadena = 999L


    assertEquals(-1, Share.FN_cod_fg_campemimediaset(
      List(),
      codigos_de_cadenas_campemimediaset, fecha_dia, cod_anunciante_subsidiario, cod_cadena  ))

  }

  @Test
  // AgrupCadenas list is NOT empty. MATCH
  def FN_cod_fg_campemimediaset_t2(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = ""
    val AC_des_grupo_n2 = ""
    val AC_des_grupo_n0 = ""
    val AC_fecha_fin = 2L
    val AC_cod_forta = 0L
    val AC_des_forta = ""
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_fecha_ini = 0L
    val AC_cod_grupo_n2 = 0L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = ""

    // Codigo de cadenas
    val codigos_de_cadenas_campemimediaset = List(555L, 666L, 777L)

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anunciante_subsidiario = 555L
    val cod_cadena = 555L


    assertEquals(1, Share.FN_cod_fg_campemimediaset(
            List(AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta, AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)),
      codigos_de_cadenas_campemimediaset, fecha_dia, cod_anunciante_subsidiario, cod_cadena  ))

  }

  @Test
  // AgrupCadenas list is NOT empty. Doesnt MATCH
  def FN_cod_fg_campemimediaset_t3(): Unit = {

    // AgrupCadenas
    val AC_des_grupo_n1 = ""
    val AC_des_grupo_n2 = ""
    val AC_des_grupo_n0 = ""
    val AC_fecha_fin = 2L
    val AC_cod_forta = 0L
    val AC_des_forta = ""
    val AC_cod_cadena = 0L
    val AC_cod_grupo_n0 = 0L
    val AC_fecha_ini = 0L
    val AC_cod_grupo_n2 = 0L
    val AC_cod_grupo_n1 = 0L
    val AC_des_cadena = ""

    // Codigo de cadenas
    val codigos_de_cadenas_campemimediaset = List(555L, 666L, 777L)

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_anunciante_subsidiario = 555L
    val cod_cadena = 999L


    assertEquals(0, Share.FN_cod_fg_campemimediaset(
      List(AgrupCadenas(AC_des_grupo_n1, AC_des_grupo_n2, AC_des_grupo_n0, AC_fecha_fin, AC_cod_forta, AC_des_forta, AC_cod_cadena, AC_cod_grupo_n0, AC_fecha_ini, AC_cod_grupo_n2, AC_cod_grupo_n1, AC_des_cadena)),
      codigos_de_cadenas_campemimediaset, fecha_dia, cod_anunciante_subsidiario, cod_cadena  ))

  }

  //*****************************************************************************************

  @Test
  // Eventos list is empty.
  def FN_cod_eventos_t1(): Unit = {

    // Eventos
    val Ev_cod_cadena = 0L
    val Ev_cod_evento = 0L
    val Ev_cod_programa = 0L
    val Ev_des_cadena = "AAA"
    val Ev_des_evento = "BBB"
    val Ev_des_programa = "CCC"
    val Ev_fecha_fin = 2L
    val Ev_fecha_ini = 0L
    val Ev_flag = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 999L
    val cod_programa = 999L

    assertEquals(0, Share.FN_cod_eventos(
      List(),
      fecha_dia, cod_cadena, cod_programa))

  }

  @Test
  // Eventos list is NOT empty. Everything MATCHes
  def FN_cod_eventos_t2(): Unit = {

    // Eventos
    val Ev_cod_cadena = 999L
    val Ev_cod_evento = 1000L
    val Ev_cod_programa = 888L
    val Ev_des_cadena = "AAA"
    val Ev_des_evento = "BBB"
    val Ev_des_programa = "CCC"
    val Ev_fecha_fin = 2L
    val Ev_fecha_ini = 0L
    val Ev_flag = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 999L
    val cod_programa = 888L

    assertEquals(1000, Share.FN_cod_eventos(
      List(Eventos(Ev_cod_cadena, Ev_cod_evento, Ev_cod_programa, Ev_des_cadena, Ev_des_evento, Ev_des_programa, Ev_fecha_fin, Ev_fecha_ini, Ev_flag )),
      fecha_dia, cod_cadena, cod_programa))

  }

  @Test
  // Eventos list is NOT empty. Something doesnt MATCH
  def FN_cod_eventos_t3(): Unit = {

    // Eventos
    val Ev_cod_cadena = 999L
    val Ev_cod_evento = 1000L
    val Ev_cod_programa = 888L
    val Ev_des_cadena = "AAA"
    val Ev_des_evento = "BBB"
    val Ev_des_programa = "CCC"
    val Ev_fecha_fin = 2L
    val Ev_fecha_ini = 0L
    val Ev_flag = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 999L
    val cod_programa = 888999L

    assertEquals(0, Share.FN_cod_eventos(
      List(Eventos(Ev_cod_cadena, Ev_cod_evento, Ev_cod_programa, Ev_des_cadena, Ev_des_evento, Ev_des_programa, Ev_fecha_fin, Ev_fecha_ini, Ev_flag )),
      fecha_dia, cod_cadena, cod_programa))

  }

  //*****************************************************************************************

  @Test
  // Eventos list is empty.
  def FN_nom_eventos_t1(): Unit = {

    // Eventos
    val Ev_cod_cadena = 0L
    val Ev_cod_evento = 0L
    val Ev_cod_programa = 0L
    val Ev_des_cadena = "AAA"
    val Ev_des_evento = "BBB"
    val Ev_des_programa = "CCC"
    val Ev_fecha_fin = 2L
    val Ev_fecha_ini = 0L
    val Ev_flag = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 999L
    val cod_programa = 999L

    assertEquals("", Share.FN_nom_eventos(
      List(),
      fecha_dia, cod_cadena, cod_programa))

  }

  @Test
  // Eventos list is NOT empty. Everything MATCHes
  def FN_nom_eventos_t2(): Unit = {

    // Eventos
    val Ev_cod_cadena = 999L
    val Ev_cod_evento = 1000L
    val Ev_cod_programa = 888L
    val Ev_des_cadena = "AAA"
    val Ev_des_evento = "BBB"
    val Ev_des_programa = "CCC"
    val Ev_fecha_fin = 2L
    val Ev_fecha_ini = 0L
    val Ev_flag = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 999L
    val cod_programa = 888L

    assertEquals("BBB", Share.FN_nom_eventos(
      List(Eventos(Ev_cod_cadena, Ev_cod_evento, Ev_cod_programa, Ev_des_cadena, Ev_des_evento, Ev_des_programa, Ev_fecha_fin, Ev_fecha_ini, Ev_flag )),
      fecha_dia, cod_cadena, cod_programa))

  }

  @Test
  // Eventos list is NOT empty. Something doesnt MATCH
  def FN_nom_eventos_t3(): Unit = {

    // Eventos
    val Ev_cod_cadena = 999L
    val Ev_cod_evento = 1000L
    val Ev_cod_programa = 888L
    val Ev_des_cadena = "AAA"
    val Ev_des_evento = "BBB"
    val Ev_des_programa = "CCC"
    val Ev_fecha_fin = 2L
    val Ev_fecha_ini = 0L
    val Ev_flag = 0L

    // fctd_share_grps
    val fecha_dia = 1L
    val cod_cadena = 999L
    val cod_programa = 888999L

    assertEquals("", Share.FN_nom_eventos(
      List(Eventos(Ev_cod_cadena, Ev_cod_evento, Ev_cod_programa, Ev_des_cadena, Ev_des_evento, Ev_des_programa, Ev_fecha_fin, Ev_fecha_ini, Ev_flag )),
      fecha_dia, cod_cadena, cod_programa))

  }

}
