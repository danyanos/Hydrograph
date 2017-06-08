/*******************************************************************************
 * Copyright 2017 Capital One Services, LLC and Bitwise, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 *******************************************************************************/
package hydrograph.engine.core.component.generator;

import hydrograph.engine.jaxb.commontypes.TypeBaseComponent;
import hydrograph.engine.jaxb.commontypes.TypeBaseRecord;
import hydrograph.engine.jaxb.commontypes.TypeInputOutSocket;
import hydrograph.engine.jaxb.inputtypes.Postgres;

import hydrograph.engine.core.component.entity.InputRDBMSEntity;
import hydrograph.engine.core.component.entity.elements.OutSocket;
import hydrograph.engine.core.component.entity.elements.SchemaField;
import hydrograph.engine.core.component.entity.utils.InputEntityUtils;
import hydrograph.engine.core.component.generator.base.InputComponentGeneratorBase;
import hydrograph.engine.core.constants.Constants;

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author danyanos
 */
public class InputPostgresEntityGenerator extends InputComponentGeneratorBase {

	private static Logger LOG = LoggerFactory.getLogger(InputPostgresEntityGenerator.class);

	private Postgres inputPostgresJaxb;
	private InputRDBMSEntity inputRDBMSEntity; 

/*------------------------------------------------------------------------------------------------*/

	public InputPostgresEntityGenerator(TypeBaseComponent baseComponent) {
		super(baseComponent);
	}

/*------------------------------------------------------------------------------------------------*/

	@Override
	public void castComponentFromBase(TypeBaseComponent baseComponent) {
		inputPostgresJaxb = (Postgres) baseComponent;
	}

/*------------------------------------------------------------------------------------------------*/

	@Override
	public void createEntity() {
		inputRDBMSEntity = new InputRDBMSEntity();
	}
	
/*------------------------------------------------------------------------------------------------*/

	@Override
	public void initializeEntity() {
		LOG.trace("Initializing entity object that correspond to PostgreSQL input component: " + 
					inputPostgresJaxb.getId());
		
		//Set properties inherited from AssemblyEntityBase
		inputRDBMSEntity.setComponentId(inputPostgresJaxb.getId());
		inputRDBMSEntity.setRuntimeProperties(this.getRuntimeProperties());
		inputRDBMSEntity.setBatch(inputPostgresJaxb.getBatch());

		//Set properties inherited from InputOutputEntityBase
		inputRDBMSEntity.setFieldsList(this.getSchemaFieldList());
		inputRDBMSEntity.setOutSocketList(this.getOutSocketList());

		//Set properties for the InputRDBMSEntity class
        inputRDBMSEntity.setDatabaseName(inputPostgresJaxb.getDatabaseName().getValue());
        inputRDBMSEntity.setHostName(inputPostgresJaxb.getHostName().getValue());
		//NOTE: The JDBC Driver name is fixed in the UI.This value will never be 'null'
        inputRDBMSEntity.setJdbcDriver(inputPostgresJaxb.getJdbcDriver().getValue());
		inputRDBMSEntity.setPort(this.getPortNumber());
        inputRDBMSEntity.setUsername(inputPostgresJaxb.getUsername().getValue());
        inputRDBMSEntity.setPassword(inputPostgresJaxb.getPassword().getValue());
		//NOTE: only one of 'tableName' or 'selectQuery' will be used. 
        inputRDBMSEntity.setTableName(this.getTableNameString());
        inputRDBMSEntity.setSelectQuery(this.getSelectQueryString());
	}

/*------------------------------------------------------------------------------------------------*/

	private Properties getRuntimeProperties() {
		if(inputPostgresJaxb.getRuntimeProperties() == null) {
			return new Properties();
		}

		return InputEntityUtils.extractRuntimeProperties(inputPostgresJaxb.getRuntimeProperties());
	}

/*------------------------------------------------------------------------------------------------*/

	/*	NOTE: 	Component schemas are 'attached' to component sockets. In order to determine the 
	 *			schema we need to first access the socket
	 */
	private List<SchemaField> getSchemaFieldList() {
		List<TypeInputOutSocket> inputOutSocketList = inputPostgresJaxb.getOutSocket();

		//Input components by definition have only one outsocket
		TypeInputOutSocket inputOutSocket = inputOutSocketList.get(0);
		TypeBaseRecord schema = inputOutSocket.getSchema();

		/*	'objectSchema' contains 3 different types of objects
		 *	-	TypeBaseField
		 *	-	TypeBaseRecord
		 *	-	TypeExternalSchema
		 */
		List<Object> objectSchema = schema.getFieldOrRecordOrIncludeExternalSchema();
		return InputEntityUtils.extractInputFields(objectSchema);
	}

/*------------------------------------------------------------------------------------------------*/

	private List<OutSocket> getOutSocketList() {
		//Retrieve the JAXB representation of an 'InputOutSocket'
		List<TypeInputOutSocket> inputOutSocketList = inputPostgresJaxb.getOutSocket();
		//Extract the 'OutSocket' from the JAXB class
		return InputEntityUtils.extractOutSocket(inputOutSocketList);
	}

/*------------------------------------------------------------------------------------------------*/

	private Integer getPortNumber() {
		if(inputPostgresJaxb.getPort() == null) {
			LOG.warn("Input Postgres component '" + inputRDBMSEntity.getComponentId() + 
						"' port is not provided, using default port " + 
							Constants.DEFAULT_POSTGRES_PORT);

			return Constants.DEFAULT_POSTGRES_PORT;
		}

		return inputPostgresJaxb.getPort().getValue().intValue();
	}

/*------------------------------------------------------------------------------------------------*/

	private String getTableNameString() {
		if(inputPostgresJaxb.getTableName() == null) {
			return null;
		}

		return inputPostgresJaxb.getTableName().getValue();
	}

/*------------------------------------------------------------------------------------------------*/

	private String getSelectQueryString() {
		if(inputPostgresJaxb.getSelectQuery() == null) {
			return null;
		}

		return inputPostgresJaxb.getSelectQuery().getValue();
	}

/*------------------------------------------------------------------------------------------------*/

    @Override
    public InputRDBMSEntity getEntity() {
        return this.inputRDBMSEntity;
    }

}
