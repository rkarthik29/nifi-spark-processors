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
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
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
public class FilterDataFrame extends AbstractProcessor {

    public static final PropertyDescriptor OUTPUT_DF_NAME = new PropertyDescriptor
            .Builder().name("OUTPUT_DF_NAME")
            .displayName("Name of the DF to output the filtered records")
            .description("Name for your DF, ex. salesDF")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor INPUT_DF_NAME = new PropertyDescriptor
            .Builder().name("INPUT_DF_NAME")
            .displayName("Name of the DF to filter columns from")
            .description("Name for your DF, ex. salesDF")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    
    public static final PropertyDescriptor OUTPUT_COLUMNS = new PropertyDescriptor
            .Builder().name("OUTPUT_COLUMNS")
            .displayName("columns to output, comma seperated")
            .description("columns to include in the new DF, ex. col1,col2")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor DROP_NULLS = new PropertyDescriptor
            .Builder().name("DROP_NULLS")
            .displayName("drop records having nulls for provided columns")
            .description("drop records having nulls for provieded columns, ex. col1,col2")
            .required(true)
            .defaultValue("true")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true","false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor NULL_COLUMNS = new PropertyDescriptor
            .Builder().name("NULL_COLUMNS")
            .displayName("columns to check for null, if left empty all columns will be checked")
            .description("columns to check for null, if left empty all columns will be checked")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

	public static final String DF_NAME = null;

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(OUTPUT_DF_NAME);
        descriptors.add(INPUT_DF_NAME);
        descriptors.add(OUTPUT_COLUMNS);
        descriptors.add(NULL_COLUMNS);
        descriptors.add(DROP_NULLS);
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
    
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
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
        String o_df_name=context.getProperty(OUTPUT_DF_NAME).getValue();
        String i_df_name=context.getProperty(INPUT_DF_NAME).getValue();
        Boolean dropNulls = context.getProperty(DROP_NULLS).asBoolean();
        final StringBuilder sb = new StringBuilder();
        //sb.append("from pyspark.sql import SQLContext\n");
        //sb.append("sqlContext = SQLContext(sc)\n");
        String columns = context.getProperty(OUTPUT_COLUMNS).getValue();
        String null_columns = context.getProperty(NULL_COLUMNS).getValue();
 
        ArrayList<String> colList = new ArrayList<String>();
        for(String column:columns.split(",")){
        	colList.add("'"+column+"'");
        }
        sb.append("\n");
        sb.append(String.format("%s=%s",o_df_name,i_df_name));
        if(dropNulls){
        	if(context.getProperty(NULL_COLUMNS).isSet()){
        		ArrayList<String> nullList = new ArrayList<String>();
                for(String column:null_columns.split(",")){
                	nullList.add("'"+column+"'");
                }
                sb.append(String.format(".dropna(subset=[%s])\n",String.join(",", nullList)));
        	}else{
        		sb.append(".dropna(how='any')\n");
        	}
        }
        context.getProperties().keySet().stream().filter(PropertyDescriptor::isDynamic)
        .forEach((dynamicPropDescriptor) ->sb.append(
        		String.format(
        				".filter(%s)\n",
        				context.getProperty(dynamicPropDescriptor).
        				evaluateAttributeExpressions().getValue()
        				)
        		)
        		);
        sb.append(String.format(".select(%s)"
        		,String.join(",", colList)));
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
