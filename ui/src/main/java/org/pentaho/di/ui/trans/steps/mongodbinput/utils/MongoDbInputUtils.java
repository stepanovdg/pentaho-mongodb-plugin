/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.di.ui.trans.steps.mongodbinput.utils;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodbinput.DiscoverFieldsCallback;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.steps.mongodbinput.MongoDbInputDialog;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.List;

/**
 * User: Dzmitry Stsiapanau Date: 12/22/2015 Time: 06:49
 */

public class MongoDbInputUtils {

  public static void discoverFields( final MongoDbInputMeta meta, final VariableSpace vars, final int docsToSample,
                                     final MongoDbInputDialog mongoDialog ) throws KettleException {
    MongoProperties.Builder propertiesBuilder = MongoWrapperUtil.createPropertiesBuilder( meta, vars );
    String db = vars.environmentSubstitute( meta.getDbName() );
    String collection = vars.environmentSubstitute( meta.getCollection() );
    String query = vars.environmentSubstitute( meta.getJsonQuery() );
    String fields = vars.environmentSubstitute( meta.getFieldsName() );
    int numDocsToSample = docsToSample;
    if ( numDocsToSample < 1 ) {
      numDocsToSample = 100; // default
    }
    try {
      MongoDbInputData.getMongoDbInputDiscoverFieldsHolder().getMongoDbInputDiscoverFields()
        .discoverFields( propertiesBuilder, db, collection, query, fields, meta.getQueryIsPipeline(), numDocsToSample,
          meta, new DiscoverFieldsCallback() {
            @Override
            public void notifyFields( final List<MongoField> fields ) {
              if ( fields.size() > 0 ) {
                Spoon.getInstance().getDisplay().asyncExec( new Runnable() {
                  @Override
                  public void run() {
                    if ( !mongoDialog.isTableDisposed() ) {
                      meta.setMongoFields( fields );
                      mongoDialog.updateFieldTableFields( meta.getMongoFields() );
                    }
                  }
                } );
              }
            }

            @Override
            public void notifyException( Exception exception ) {
              mongoDialog.handleNotificationException( exception );
            }
          } );
    } catch ( KettleException e ) {
      throw new KettleException( "Unable to discover fields from MongoDB", e );
    }
  }


  public static boolean discoverFields( final MongoDbInputMeta meta, final VariableSpace vars, final int docsToSample )
    throws KettleException {

    MongoProperties.Builder propertiesBuilder = MongoWrapperUtil.createPropertiesBuilder( meta, vars );
    try {
      String db = vars.environmentSubstitute( meta.getDbName() );
      String collection = vars.environmentSubstitute( meta.getCollection() );
      String query = vars.environmentSubstitute( meta.getJsonQuery() );
      String fields = vars.environmentSubstitute( meta.getFieldsName() );
      int numDocsToSample = docsToSample;
      if ( numDocsToSample < 1 ) {
        numDocsToSample = 100; // default
      }
      List<MongoField> discoveredFields =
        MongoDbInputData.getMongoDbInputDiscoverFieldsHolder().getMongoDbInputDiscoverFields().discoverFields(
          propertiesBuilder, db, collection, query, fields, meta.getQueryIsPipeline(), numDocsToSample, meta );

      // return true if query resulted in documents being returned and fields
      // getting extracted
      if ( discoveredFields.size() > 0 ) {
        meta.setMongoFields( discoveredFields );
        return true;
      }
    } catch ( Exception e ) {
      if ( e instanceof KettleException ) {
        throw (KettleException) e;
      } else {
        throw new KettleException( "Unable to discover fields from MongoDB", e );
      }
    }
    return false;
  }
}

