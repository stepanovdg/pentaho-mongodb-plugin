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

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodbinput.DiscoverFieldsCallback;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputDiscoverFields;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputDiscoverFieldsHolder;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.di.ui.trans.steps.mongodbinput.MongoDbInputDialog;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 12/23/2015 Time: 05:19
 */

public class MongoDbInputUtilsTest {

  private MongoDbInputData mongoDbInputData;

  @Before public void setUp() {
    mongoDbInputData = new MongoDbInputData();
  }

  @Test public void testDiscoverFields() throws Exception {
      String dbName = "testDb";
      String collection = "testCollection";
      String query = "testQuery";
      String fields = "testFields";

      MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
      when( meta.getName() ).thenReturn( dbName );
      when( meta.getCollection() ).thenReturn( collection );
      when( meta.getJsonQuery() ).thenReturn( query );
      when( meta.getFieldsName() ).thenReturn( fields );

      VariableSpace vars = mock( VariableSpace.class );
      when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
      when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
      when( vars.environmentSubstitute( query ) ).thenReturn( query );
      when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

      int docsToSample = 1;

      MongoDbInputDialog dialog = mock( MongoDbInputDialog.class );

      //Mock the discoverFields call so that it returns a list of mongofields from the expected input
      MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
      MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );
      final List<MongoField> mongoFields = new ArrayList<MongoField>();

      doAnswer( new Answer() {
          @Override public Object answer( InvocationOnMock invocationOnMock ) {
            ( (DiscoverFieldsCallback) invocationOnMock.getArguments()[8] ).notifyFields( mongoFields );
            return null;
          }
        } ).when( mongoDbInputDiscoverFields )
          .discoverFields( any( MongoProperties.Builder.class ), anyString(), anyString(), anyString(), anyString(),
            anyBoolean(), anyInt(), any( MongoDbInputMeta.class ), any( DiscoverFieldsCallback.class ) );

      when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
      mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

    MongoDbInputUtils.discoverFields( meta, vars, docsToSample, dialog );
      verify( holder, atLeastOnce() ).getMongoDbInputDiscoverFields();

      //Test case when docsToSample is zero
    MongoDbInputUtils.discoverFields( meta, vars, 0, dialog );
      verify( holder, atLeastOnce() ).getMongoDbInputDiscoverFields();
    }

    @Test public void testDiscoverFieldsExceptionCallback() throws Exception {
      String dbName = "testDb";
      String collection = "testCollection";
      String query = "testQuery";
      String fields = "testFields";

      MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
      when( meta.getName() ).thenReturn( dbName );
      when( meta.getCollection() ).thenReturn( collection );
      when( meta.getJsonQuery() ).thenReturn( query );
      when( meta.getFieldsName() ).thenReturn( fields );

      VariableSpace vars = mock( VariableSpace.class );
      when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
      when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
      when( vars.environmentSubstitute( query ) ).thenReturn( query );
      when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

      int docsToSample = 1;

      MongoDbInputDialog dialog = mock( MongoDbInputDialog.class );

      //Mock the discoverFields call so that it returns a list of mongofields from the expected input
      MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
      MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );

      doAnswer( new Answer() {
        @Override public Object answer( InvocationOnMock invocationOnMock ) {
          ( (DiscoverFieldsCallback) invocationOnMock.getArguments()[8] ).notifyException( new KettleException() );
          return null;
        }
      } ).when( mongoDbInputDiscoverFields )
          .discoverFields( any( MongoProperties.Builder.class ), anyString(), anyString(), anyString(), anyString(),
            anyBoolean(), anyInt(), any( MongoDbInputMeta.class ), any( DiscoverFieldsCallback.class ) );

      when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
      mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

      MongoDbInputUtils.discoverFields( meta, vars, docsToSample, dialog );
      verify( dialog, atLeastOnce() ).handleNotificationException( any( Exception.class ) );
    }

    @Test public void testDiscoverFieldsThrowsException() throws Exception {
      String dbName = "testDb";
      String collection = "testCollection";
      String query = "testQuery";
      String fields = "testFields";

      MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
      when( meta.getName() ).thenReturn( dbName );
      when( meta.getCollection() ).thenReturn( collection );
      when( meta.getJsonQuery() ).thenReturn( query );
      when( meta.getFieldsName() ).thenReturn( fields );

      VariableSpace vars = mock( VariableSpace.class );
      when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
      when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
      when( vars.environmentSubstitute( query ) ).thenReturn( query );
      when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

      int docsToSample = 1;

      MongoDbInputDialog dialog = mock( MongoDbInputDialog.class );

      //Mock the discoverFields call so that it returns a list of mongofields from the expected input
      MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
      MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );

      doThrow( new KettleException() ).when( mongoDbInputDiscoverFields )
          .discoverFields( any( MongoProperties.Builder.class ), anyString(), anyString(), anyString(), anyString(),
            anyBoolean(), anyInt(), any( MongoDbInputMeta.class ), any( DiscoverFieldsCallback.class ) );

      when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
      mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

      try {
        MongoDbInputUtils.discoverFields( meta, vars, docsToSample, dialog );
      } catch ( Exception expected ) {
        //expected
      }
    }

    @Test public void testDiscoverFieldsWithoutCallback() throws Exception {
      String dbName = "testDb";
      String collection = "testCollection";
      String query = "testQuery";
      String fields = "testFields";

      MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
      when( meta.getName() ).thenReturn( dbName );
      when( meta.getCollection() ).thenReturn( collection );
      when( meta.getJsonQuery() ).thenReturn( query );
      when( meta.getFieldsName() ).thenReturn( fields );

      VariableSpace vars = mock( VariableSpace.class );
      when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
      when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
      when( vars.environmentSubstitute( query ) ).thenReturn( query );
      when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

      int docsToSample = 1;

      //Mock the discoverFields call so that it returns a list of mongofields from the expected input
      MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
      MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );
      List<MongoField> mongoFields = new ArrayList<MongoField>();
      mongoFields.add( new MongoField() );
      when( mongoDbInputDiscoverFields
          .discoverFields( any( MongoProperties.Builder.class ), anyString(), anyString(), anyString(), anyString(),
            anyBoolean(), anyInt(), any( MongoDbInputMeta.class ) ) ).thenReturn( mongoFields );
      when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
      mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

      boolean result = MongoDbInputUtils.discoverFields( meta, vars, docsToSample );
      assertTrue( result );

      //Test case when docsToSample is zero
      result = MongoDbInputUtils.discoverFields( meta, vars, 0 );
      assertTrue( result );

      //Test case when no fields are found
      mongoFields.clear();
      result = MongoDbInputUtils.discoverFields( meta, vars, docsToSample );
      assertFalse( result );
    }

    @Test public void testDiscoverFieldsWithoutCallbackThrowsKettleException() throws Exception {
      String dbName = "testDb";
      String collection = "testCollection";
      String query = "testQuery";
      String fields = "testFields";

      MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
      when( meta.getName() ).thenReturn( dbName );
      when( meta.getCollection() ).thenReturn( collection );
      when( meta.getJsonQuery() ).thenReturn( query );
      when( meta.getFieldsName() ).thenReturn( fields );

      VariableSpace vars = mock( VariableSpace.class );
      when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
      when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
      when( vars.environmentSubstitute( query ) ).thenReturn( query );
      when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

      int docsToSample = 1;

      //Mock the discoverFields call so that it returns a list of mongofields from the expected input
      MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
      MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );
      when( mongoDbInputDiscoverFields
          .discoverFields( any( MongoProperties.Builder.class ), anyString(), anyString(), anyString(), anyString(),
            anyBoolean(), anyInt(), any( MongoDbInputMeta.class ) ) )
          .thenThrow( new KettleException( "testException" ) );
      when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
      mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

      try {
        MongoDbInputUtils.discoverFields( meta, vars, docsToSample );
      } catch ( KettleException e ) {
        //Expected
      }
    }

    @Test public void testDiscoverFieldsWithoutCallbackThrowsException() throws Exception {
      String dbName = "testDb";
      String collection = "testCollection";
      String query = "testQuery";
      String fields = "testFields";

      MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
      when( meta.getName() ).thenReturn( dbName );
      when( meta.getCollection() ).thenReturn( collection );
      when( meta.getJsonQuery() ).thenReturn( query );
      when( meta.getFieldsName() ).thenReturn( fields );

      VariableSpace vars = mock( VariableSpace.class );
      when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
      when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
      when( vars.environmentSubstitute( query ) ).thenReturn( query );
      when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

      int docsToSample = 1;

      //Mock the discoverFields call so that it returns a list of mongofields from the expected input
      MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
      MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );
      when( mongoDbInputDiscoverFields
          .discoverFields( any( MongoProperties.Builder.class ), anyString(), anyString(), anyString(), anyString(),
            anyBoolean(), anyInt(), any( MongoDbInputMeta.class ) ) ).thenThrow( new NullPointerException() );
      when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
      mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

      try {
        MongoDbInputUtils.discoverFields( meta, vars, docsToSample );
      } catch ( KettleException e ) {
        //Expected
      }
    }

}
  