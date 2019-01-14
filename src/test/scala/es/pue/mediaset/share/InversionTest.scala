package es.pue.mediaset.share

import es.pue.mediaset.share.Share.{Configuraciones, LineaNegocio, AgrupCadenas, relCampaniaTrgt, Eventos}
import org.junit.Test
import org.junit.Assert._

class InversionTest {


  @Test
  // CatEventos is empty
  def FN_coef_evento_t1() : Unit = {

    // CatEventos
    val CE_cod_evento = 123L
    val CE_activo = "activo"
    val CE_des_evento = "nombre de evento"
    val CE_fecha_fin = 2L
    val CE_fecha_ini = 0L
    val CE_indice = "1.723"

    // fctd_share_grps
    val cod_eventos = 1L

    assertEquals(0D,  Inversion.FN_coef_evento(
      List(),
      cod_eventos), 0)

    // Using 0.0 as the delta is the same as using the deprecated method.
    // The delta is intended to reflect how close the numbers can be and still be considered equal.
    // Use values like 0.1 or 0.01 or 0.001, etc, depending on how much error the application can tolerate.


  }

}
