/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.sparkProcessors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class FilterDataFrameTest {

    private TestRunner testRunner;

    @Before
    public void init() {
     
        
    }

    @Test
    public void testProcessor() {
    	testRunner = TestRunners.newTestRunner(new FilterDataFrame());
        testRunner.setProperty(FilterDataFrame.OUTPUT_DF_NAME,"retal_df");
        testRunner.setProperty(FilterDataFrame.INPUT_DF_NAME,"sales_df");
        testRunner.setProperty(FilterDataFrame.OUTPUT_COLUMNS,"id,name,address");
        testRunner.setProperty(FilterDataFrame.DROP_NULLS,"true");
        testRunner.enqueue("");
        testRunner.setValidateExpressionUsage(false);
    	 testRunner.run();
         testRunner.assertTransferCount(CreateDataFrame.SUCCESS, 1);
    }
    
    @Test
    public void testRemoveNullsColumns() {
    	testRunner = TestRunners.newTestRunner(new FilterDataFrame());
        testRunner.setProperty(FilterDataFrame.OUTPUT_DF_NAME,"retal_df");
        testRunner.setProperty(FilterDataFrame.INPUT_DF_NAME,"sales_df");
        testRunner.setProperty(FilterDataFrame.OUTPUT_COLUMNS,"id,name,address");
        testRunner.setProperty(FilterDataFrame.DROP_NULLS,"true");
        testRunner.setProperty(FilterDataFrame.NULL_COLUMNS, "zipcode");
        testRunner.enqueue("");
        testRunner.setValidateExpressionUsage(false);
    	 testRunner.run();
         testRunner.assertTransferCount(CreateDataFrame.SUCCESS, 1);
    }
    
    @Test
    public void testFilters() {
    	testRunner = TestRunners.newTestRunner(new FilterDataFrame());
        testRunner.setProperty(FilterDataFrame.OUTPUT_DF_NAME,"retal_df");
        testRunner.setProperty(FilterDataFrame.INPUT_DF_NAME,"sales_df");
        testRunner.setProperty(FilterDataFrame.OUTPUT_COLUMNS,"id,name,address");
        testRunner.setProperty(FilterDataFrame.DROP_NULLS,"true");
        testRunner.setProperty("Filter1", "id >= 1234");
        testRunner.setProperty("Filter2", "name == 'karthik'");
        testRunner.enqueue("");
        testRunner.setValidateExpressionUsage(false);
    	testRunner.run();
        testRunner.assertTransferCount(CreateDataFrame.SUCCESS, 1);
    }

}
