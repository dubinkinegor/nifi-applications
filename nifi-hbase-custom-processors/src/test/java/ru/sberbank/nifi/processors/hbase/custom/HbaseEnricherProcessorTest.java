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
package ru.sberbank.nifi.processors.hbase.custom;

import groovy.json.JsonSlurper;
import org.apache.hadoop.hbase.Cell;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ru.sberbank.nifi.processors.hbase.custom.HbaseEnricherProcessor.REL_FAILURE;
import static ru.sberbank.nifi.processors.hbase.custom.HbaseEnricherProcessor.REL_SUCCESS;


public class HbaseEnricherProcessorTest {

    private TestRunner testRunner;
    private static  final String FLAT_JSON__ARRAY_EXAMPLE = "\"test:\" [ { \"dep\": \"First Top\", \"name\": \"First\", \"model\": \"value1\", \"size\": \"320\" }, " +
            "{ \"dep\": \"Second Top\", \"name\": \"Second\", \"model\": \"value2\", \"size\": \"111\" } ]";
    private static  final String FLAT_JSON_EXAMPLE = "{ \"dep\": \"First Top\", \"name\": \"First\", \"model\": \"value1\", \"size\": \"320\" }";

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(HbaseEnricherProcessor.class);
    }

    @Test
    public void testProcessor() throws IOException {
        testRunner.setProperty("hbase-table-name", "HBASE:Z_AC_FIN");
        testRunner.setProperty("attribute-with-key-name", "super_attribute");
        testRunner.setProperty("kerberos-principal", "super-principal");
        testRunner.setProperty("kerberos-keytab", "super-keytab");
        testRunner.setProperty("Hadoop Configuration Files", "superfile1,superfile");
        testRunner.setProperty("hb-lu-return-cols", "cf:sup_col1,xf:sup_col2");
        testRunner.setProperty(new PropertyDescriptor.Builder().name("super_field_1").dynamic(true).build(), "cf:sup_col1");
        testRunner.setProperty(new PropertyDescriptor.Builder().name("super_field_2").dynamic(true).build(), "cf:sup_col2");

        ObjectMapper om = new ObjectMapper();
        HashMap<String, Object> jsonMap = om.readValue(FLAT_JSON_EXAMPLE, HashMap.class);
//        MockFlowFile mockFlowFile1 = new MockFlowFile(1);
//        mockFlowFile1.setEnqueuedIndex(1);
//        mockFlowFile1.putAttributes(Collections.singletonMap("super_attribute", "super_key"));
//        testRunner.enqueue(FLAT_JSON_EXAMPLE.getBytes(StandardCharsets.UTF_8));
        testRunner.enqueue(FLAT_JSON_EXAMPLE, Collections.singletonMap("super_attribute", "super_key"));
        testRunner.enqueue(FLAT_JSON_EXAMPLE, Collections.singletonMap("super_attribute", "super_key"));
        testRunner.enqueue(FLAT_JSON_EXAMPLE, Collections.singletonMap("super_attribute", "super_key"));
        testRunner.enqueue(FLAT_JSON_EXAMPLE, Collections.singletonMap("super_attribute", "super_key"));
        testRunner.enqueue(FLAT_JSON_EXAMPLE, Collections.singletonMap("super_attribute", "super_key"));
        testRunner.enqueue(FLAT_JSON_EXAMPLE, Collections.singletonMap("super_attribute", "super_key"));
        testRunner.enqueue(FLAT_JSON_EXAMPLE, Collections.singletonMap("super_attribute", "super_key"));
        testRunner.enqueue(FLAT_JSON_EXAMPLE, Collections.singletonMap("super_attribute", "super_key"));
        testRunner.enqueue(FLAT_JSON_EXAMPLE, Collections.singletonMap("super_attribute", "super_key"));


        testRunner.run();
        List<MockFlowFile> sff = testRunner.getFlowFilesForRelationship(REL_FAILURE);
        System.out.println(sff);
        List<MockFlowFile> sff1 = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        System.out.println(sff1);
    }

    private class TestCell implements Cell {

        @Override
        public byte[] getRowArray() {
            return new byte[0];
        }

        @Override
        public int getRowOffset() {
            return 0;
        }

        @Override
        public short getRowLength() {
            return 0;
        }

        @Override
        public byte[] getFamilyArray() {
            return new byte[0];
        }

        @Override
        public int getFamilyOffset() {
            return 0;
        }

        @Override
        public byte getFamilyLength() {
            return 0;
        }

        @Override
        public byte[] getQualifierArray() {
            return new byte[0];
        }

        @Override
        public int getQualifierOffset() {
            return 0;
        }

        @Override
        public int getQualifierLength() {
            return 0;
        }

        @Override
        public long getTimestamp() {
            return 0;
        }

        @Override
        public byte getTypeByte() {
            return 0;
        }

        @Override
        public long getMvccVersion() {
            return 0;
        }

        @Override
        public long getSequenceId() {
            return 0;
        }

        @Override
        public byte[] getValueArray() {
            return new byte[0];
        }

        @Override
        public int getValueOffset() {
            return 0;
        }

        @Override
        public int getValueLength() {
            return 0;
        }

        @Override
        public byte[] getTagsArray() {
            return new byte[0];
        }

        @Override
        public int getTagsOffset() {
            return 0;
        }

        @Override
        public int getTagsLength() {
            return 0;
        }

        @Override
        public byte[] getValue() {
            return new byte[0];
        }

        @Override
        public byte[] getFamily() {
            return new byte[0];
        }

        @Override
        public byte[] getQualifier() {
            return new byte[0];
        }

        @Override
        public byte[] getRow() {
            return new byte[0];
        }
    }

}
