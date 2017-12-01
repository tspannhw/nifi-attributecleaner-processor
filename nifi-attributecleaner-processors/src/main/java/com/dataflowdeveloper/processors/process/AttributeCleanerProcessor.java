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
package com.dataflowdeveloper.processors.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

@Tags({ "attributecleaner" })
@CapabilityDescription("")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class AttributeCleanerProcessor extends AbstractProcessor {

	public static final String ATTRIBUTE_OUTPUT_NAME = "cleaned";

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("Successfully cleaned attribute names.").build();

	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("Failed to clean attributes.").build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
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
		return;
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			flowFile = session.create();
		}
		try {
			Map<String, String> attributes = flowFile.getAttributes();
			Map<String, String> attributesClean = new HashMap<>();
			String tempKey = "";
			
			for (Map.Entry<String, String> entry : attributes.entrySet())
			{
				tempKey = entry.getKey().replaceFirst("[^A-Za-z]", "");
				tempKey = tempKey.replaceAll("[^A-Za-z0-9_]", "");
			    attributesClean.put(tempKey, entry.getValue());
			    session.removeAttribute(flowFile, entry.getKey());
			}
		

			// flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
			// flowFile = session.write(flowFile, new StreamCallback() {
			// @Override
			// public void process(InputStream inputStream, OutputStream outputStream)
			// throws IOException {
			// Tika tika = new Tika();
			// String text = "";
			// try {
			// text = tika.parseToString(inputStream);
			// } catch (TikaException e) {
			// getLogger().error("Failed to parse input " + e.getLocalizedMessage());
			// e.printStackTrace();
			// }
			//
			// outputStream.write(text.getBytes());
			// }
			// });
			session.putAllAttributes(flowFile, attributesClean);
			session.transfer(flowFile, REL_SUCCESS);
			session.commit();
		} catch (final Throwable t) {
			getLogger().error("Unable to process Attribute Cleaner file " + t.getLocalizedMessage());
			getLogger().error("{} failed to process due to {}; rolling back session", new Object[] { this, t });
			throw t;
		}
	}
}
