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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class AttributeCleanerProcessorTest {

	private TestRunner testRunner;

	public static final String ATTRIBUTE_INPUT_NAME = "sentence";

	public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor.Builder().name(ATTRIBUTE_INPUT_NAME)
			.description("This is a test.").required(true).expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	@Before
	public void init() {
		testRunner = TestRunners.newTestRunner(AttributeCleanerProcessor.class);
	}

	@Test
	public void testProcessor() {

		try {
			final Map<String, String> attributeMap = new HashMap<>();
			attributeMap.put("my.field.has.a.badname", "a");
			attributeMap.put("goodname", "b");
			attributeMap.put("1badname", "b");
			attributeMap.put("pdf:PDFVersion", "PDF version 1.0sdfsdf : 5");
			attributeMap.put("bad-Name", "b");
			attributeMap.put("this-_.is.a.bad.name", "b");
			attributeMap.put("_BadNameX", "ab");
			attributeMap.put("_BadNameX", "bc");
			attributeMap.put(".BadNameX", "dc");
			attributeMap.put(".p:PDF:this.BadName", "basdfadsf sdf(D*F(*SD(F*( D*(*DS (*D(* *   (D*F (*DF(SD");
			testRunner.enqueue(new FileInputStream(new File("src/test/resources/d.pdf")), attributeMap);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		// testRunner.setProperty("attribute_is-Invalid.name.here", "data");
		testRunner.setValidateExpressionUsage(false);
		testRunner.run();
		testRunner.assertValid();
		List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(AttributeCleanerProcessor.REL_SUCCESS);

		for (MockFlowFile mockFile : successFiles) {
			try {
				// System.out.println("FILE:" + new String(mockFile.toByteArray(), "UTF-8"));
				
				mockFile.getAttributes().forEach((k, v) -> {
					System.out.println("Result Key : " + k + "=" + v);
				});
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}
}
