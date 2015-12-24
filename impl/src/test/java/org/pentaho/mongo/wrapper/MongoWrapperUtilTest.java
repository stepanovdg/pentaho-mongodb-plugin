/*!
 * Copyright 2010 - 2015 Pentaho Corporation.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.mongo.wrapper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProperties;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/22/14.
 */
public class MongoWrapperUtilTest {
  private MongoWrapperClientFactory cachedFactory;
  private MongoWrapperClientFactory mockFactory;

  @Before public void setup() {
    cachedFactory = MongoWrapperUtil.getMongoWrapperClientFactory();
    mockFactory = mock( MongoWrapperClientFactory.class );
    MongoWrapperUtil.setMongoWrapperClientFactory( mockFactory );
  }

  @After public void tearDown() {
    MongoWrapperUtil.setMongoWrapperClientFactory( cachedFactory );
  }

  @Test public void testCreateCalledNoReadPrefs() throws MongoDbException {
    MongoDbMeta mongoDbMeta = mock( MongoDbMeta.class );
    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );

    MongoClientWrapper wrapper = mock( MongoClientWrapper.class );
    when( mockFactory.createMongoClientWrapper( any( MongoProperties.class ), any( KettleMongoUtilLogger.class ) ) )
        .thenReturn( wrapper );
    assertEquals( wrapper,
        MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, variableSpace, logChannelInterface ) );
  }

  @Test public void testCreateCalledReadPrefs() throws MongoDbException {
    MongoDbMeta mongoDbMeta = mock( MongoDbMeta.class );
    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );

    MongoClientWrapper wrapper = mock( MongoClientWrapper.class );
    when( mongoDbMeta.getReadPrefTagSets() ).thenReturn( Arrays.asList( "test", "test2" ) );
    when( mockFactory.createMongoClientWrapper( any( MongoProperties.class ), any( KettleMongoUtilLogger.class ) ) )
        .thenReturn( wrapper );
    assertEquals( wrapper,
        MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, variableSpace, logChannelInterface ) );
  }
}
