package hydrograph.engine.spark.components

import java.util.Properties

import hydrograph.engine.core.component.entity.InputRDBMSEntity
import hydrograph.engine.spark.components.base.InputComponentBase
import hydrograph.engine.spark.components.platform.BaseComponentParams
import hydrograph.engine.spark.components.utils.{DbTableUtils, SchemaCreator, SchemaUtils}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class InputPostgresComponent(inputRDBMSEntity: InputRDBMSEntity, 
                              iComponentsParams: BaseComponentParams) extends InputComponentBase {

  val LOG: Logger = LoggerFactory.getLogger(classOf[InputPostgresComponent])

/*------------------------------------------------------------------------------------------------*/

  override def createComponent(): Map[String, DataFrame] = {

    val schemaField: StructType = SchemaCreator(inputRDBMSEntity).makeSchema()

    val sparkSession = iComponentsParams.getSparkSession()
    val runtimeConfig: RuntimeConfig = sparkSession.conf

    val properties: Properties = inputRDBMSEntity.getRuntimeProperties()
    properties.setProperty("user", inputRDBMSEntity.getUsername())
    properties.setProperty("password", inputRDBMSEntity.getPassword())
    //properties.setProperty("fetchsize", inputRDBMSEntity.getFetchSize())
    properties.setProperty("driver", inputRDBMSEntity.getJdbcDriver())
    properties.setProperty("currentSchema", inputRDBMSEntity.getSchemaName())

    val selectQuery: String = this.getSelectQuery()

    val connectionURL: String = "jdbc:postgresql://" +
                                inputRDBMSEntity.getHostName + ":" + 
                                inputRDBMSEntity.getPort + "/" +
                                inputRDBMSEntity.getDatabaseName

    LOG.info("CONNECTION URL: " + connectionURL)
    LOG.info("SELECT QUERY: " + selectQuery)

    try {
      val df: DataFrame = sparkSession.read.jdbc(connectionURL, selectQuery, properties)
      val key: String = inputRDBMSEntity.getOutSocketList.get(0).getSocketId

      LOG.info("DATAFRAME SCHEMA")
      df.schema.toList.map(structField => LOG.info(structField.name))
      LOG.info("HYDROGRAPH UI SCHEMA")
      schemaField.toList.map(structField => LOG.info(structField.name))

      SchemaUtils().compareSchema(getMappedSchema(schemaField), df.schema.toList)
      Map(key -> df)
    }
    catch {
      case e: Exception =>
        LOG.error("Error in Input  Mysql component '" + inputRDBMSEntity.getComponentId + "', " + e.getMessage, e)
        throw new DatabaseConnectionException("Error in Input Mysql Component " + inputRDBMSEntity.getComponentId, e)
    }
  }

/*------------------------------------------------------------------------------------------------*/

  private def getSelectQuery(): String = {
    if(inputRDBMSEntity.getTableName == null) {
      val formattedQuery: String = "(" + inputRDBMSEntity.getSelectQuery() + ") as alias";
      return formattedQuery;
    }
    else {
      DbTableUtils().getSelectQuery(inputRDBMSEntity.getFieldsList.asScala.toList, 
                                      inputRDBMSEntity.getTableName)
    }
  }

  /*------------------------------------------------------------------------------------------------*/

  def getMappedSchema(schema: StructType): List[StructField] = {
    schema.toList.map (structField => new StructField (structField.name, structField.dataType))
  }
}

