
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
 * limitations under the License.
 ******************************************************************************/

package hydrograph.engine.jaxb.ooracle;

import javax.xml.bind.annotation.XmlRegistry;


/**
 * The Class ObjectFactory .
 *
 * @author Bitwise
 */
@XmlRegistry
public class ObjectFactory {


    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: hydrograph.engine.jaxb.ooracle
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link TypeOutputOracleInSocket }
     * 
     */
    public TypeOutputOracleInSocket createTypeOutputOracleInSocket() {
        return new TypeOutputOracleInSocket();
    }

    /**
     * Create an instance of {@link TypePrimaryKeys }
     * 
     */
    public TypePrimaryKeys createTypePrimaryKeys() {
        return new TypePrimaryKeys();
    }

    /**
     * Create an instance of {@link DatabaseType }
     * 
     */
    public DatabaseType createDatabaseType() {
        return new DatabaseType();
    }

    /**
     * Create an instance of {@link TypeOracleField }
     * 
     */
    public TypeOracleField createTypeOracleField() {
        return new TypeOracleField();
    }

    /**
     * Create an instance of {@link TypeLoadChoice }
     * 
     */
    public TypeLoadChoice createTypeLoadChoice() {
        return new TypeLoadChoice();
    }

    /**
     * Create an instance of {@link TypeUpdateKeys }
     * 
     */
    public TypeUpdateKeys createTypeUpdateKeys() {
        return new TypeUpdateKeys();
    }

    /**
     * Create an instance of {@link TypeOracleRecord }
     * 
     */
    public TypeOracleRecord createTypeOracleRecord() {
        return new TypeOracleRecord();
    }

    /**
     * Create an instance of {@link TypeOutputOracleBase }
     * 
     */
    public TypeOutputOracleBase createTypeOutputOracleBase() {
        return new TypeOutputOracleBase();
    }

}
