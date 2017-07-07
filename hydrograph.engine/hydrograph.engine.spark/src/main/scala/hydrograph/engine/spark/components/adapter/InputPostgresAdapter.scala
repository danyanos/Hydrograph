package hydrograph.engine.spark.components.adapter

import hydrograph.engine.core.component.generator.InputPostgresEntityGenerator
import hydrograph.engine.jaxb.commontypes.TypeBaseComponent
import hydrograph.engine.spark.components.InputPostgresComponent
import hydrograph.engine.spark.components.adapter.base.InputAdatperBase
import hydrograph.engine.spark.components.base.InputComponentBase
import hydrograph.engine.spark.components.platform.BaseComponentParams

/**
  * @author danyanos
  */
class InputPostgresAdapter(typeBaseComponent: TypeBaseComponent) extends InputAdatperBase {

  private var inputPostgres: InputPostgresEntityGenerator = null
  private var sparkIPostgresComponent: InputPostgresComponent=null

/*------------------------------------------------------------------------------------------------*/

  override def createGenerator(): Unit = {
    inputPostgres = new InputPostgresEntityGenerator(typeBaseComponent)
  }

/*------------------------------------------------------------------------------------------------*/

  override def createComponent(baseComponentParams: BaseComponentParams): Unit = {
    sparkIPostgresComponent = new InputPostgresComponent(inputPostgres.getEntity, baseComponentParams)
  }

/*------------------------------------------------------------------------------------------------*/

  override def getComponent(): InputComponentBase = sparkIPostgresComponent

}
