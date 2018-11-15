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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"spark","DataFrame"})
@CapabilityDescription("Create a spark dataframe")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class JoinDataFrame extends AbstractProcessor {
	
	public static final PropertyDescriptor OUTPUT_DF_NAME = new PropertyDescriptor
            .Builder().name("OUTPUT_DF_NAME")
            .displayName("Name of the  output DF")
            .description("Name for your DF, ex. salesDF")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DF_NAME1 = new PropertyDescriptor
            .Builder().name("DF_NAME1")
            .displayName("Name of the  DF")
            .description("Name for your DF, ex. salesDF")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor DF_NAME2 = new PropertyDescriptor
            .Builder().name("DF_NAME2")
            .displayName("Name of the second DF")
            .description("Name for your DF, ex. salesDF")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor JOIN_COLUMN = new PropertyDescriptor
            .Builder().name("JOIN_COLUMN")
            .displayName("Column to join the two DFs on")
            .description("Column to join the two DFs on")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor JOIN_TYPE = new PropertyDescriptor
            .Builder().name("JOIN_TYPE")
            .displayName("Type of the JOIN ")
            .description("Join type, inner/outer")
            .required(true)
            .defaultValue("inner")
            .allowableValues("inner","outer")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor OUTPUT_COLUMNS = new PropertyDescriptor
            .Builder().name("OUTPUT_COLUMNS")
            .displayName("Columns to output")
            .description("Columns to output")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(DF_NAME1);
        descriptors.add(DF_NAME2);
        descriptors.add(JOIN_COLUMN);
        descriptors.add(JOIN_TYPE);
        descriptors.add(OUTPUT_DF_NAME);
        descriptors.add(OUTPUT_COLUMNS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        String output_df=context.getProperty(OUTPUT_DF_NAME).getValue();
        String df_name1=context.getProperty(DF_NAME1).getValue();
        String df_name2=context.getProperty(DF_NAME2).getValue();
        final StringBuilder sb = new StringBuilder();
        //sb.append("from pyspark.sql import SQLContext\n");
        //sb.append("sqlContext = SQLContext(sc)\n");
        String joinColumn = context.getProperty(JOIN_COLUMN).getValue();
        String outputColumn = context.getProperty(OUTPUT_COLUMNS).getValue();
        ArrayList<String> colList = new ArrayList<String>();
        for(String column:outputColumn.split(",")){
        	colList.add("'"+column+"'");
        }
        String joinType = context.getProperty(JOIN_TYPE).getValue();
        sb.append("\n");
        sb.append(String.format("%s=%s.join(%s,%s.%s==%s.%s,'%s').drop(%s.%s).select(%s)",
        		output_df,df_name1,df_name2,df_name1,joinColumn,df_name2,joinColumn,
        		joinType,df_name2,joinColumn,String.join(",", colList)));
        sb.append("\n");
        //final String data = sb.toString();
        flowFile = session.append(flowFile, new OutputStreamCallback() {
                public void process(final OutputStream out) throws IOException {
                    out.write(sb.toString().getBytes());
                }
            });
        session.transfer(flowFile, SUCCESS);
        // TODO implement
    }
}
