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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Tags({"hbase, enrich, json"})
@CapabilityDescription("Processor for json record enrichment with data from specified HBase table. Gets data for key from 'Attribute name containing key' property." +
        "Dynamic(additional) properties specify the name of field to which data will be added - (key), " +
        "and full qualified column name(cf:cn) containing the data itself - value." +
        "\nIMPORTANT! Works with FLAT json records only")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class HbaseEnricherProcessor extends AbstractProcessor {

    static final PropertyDescriptor HBASE_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("hbase-table-name")
            .displayName("HBase table name")
            .description("Specifies the HBase table to get data from")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor ATTRIBUTE_NAME_TO_EXTRACT_KEY_FROM = new PropertyDescriptor.Builder()
            .name("attribute-with-key-name")
            .displayName("Attribute name containing key")
            .description("Attribute name containing key")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor KERBEROS_PRINCIPAL = new PropertyDescriptor.Builder()
            .name("kerberos-principal")
            .displayName("Kerberos principal")
            .description("Kerberos principal")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor KERBEROS_KEYTAB = new PropertyDescriptor.Builder()
            .name("kerberos-keytab")
            .displayName("Kerberos keytab")
            .description("Kerberos keytab")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor HADOOP_CONF_FILES = new PropertyDescriptor.Builder()
            .name("Hadoop Configuration Files")
            .description("Comma-separated list of Hadoop Configuration files," +
                    " such as hbase-site.xml and core-site.xml for kerberos, " +
                    "including full paths to the files.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor GET_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("HBase get batch size")
            .description("Size of batch to be taken from hbase")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully enriched will be routed to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If data is not found in hbase table")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_TABLE_NAME);
        properties.add(ATTRIBUTE_NAME_TO_EXTRACT_KEY_FROM);
        properties.add(GET_BATCH_SIZE);
        properties.add(KERBEROS_PRINCIPAL);
        properties.add(KERBEROS_KEYTAB);
        properties.add(HADOOP_CONF_FILES);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Key - RecordPath that points to the field whose value will be set. Value - name of column in HBase the data will be taken from.")
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }


    private volatile Connection connection;
    private volatile UserGroupInformation ugi;
    private volatile ThreadLocal<Table> table;

    private int batchSize;

    private ConcurrentHashMap<String, String> fieldHbaseColumnMappings = new ConcurrentHashMap<>();

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            prepareConnection(context);
            prepareTable(context);
            prepareFieldHbaseColumnMappings(context);
            this.batchSize = Integer.valueOf(context.getProperty(GET_BATCH_SIZE).getValue());
            getLogger().info("Successfully prepared hbase connection and table-object!");
        } catch (Exception e) {
            getLogger().error("Failed to prepare connection to HBase: ", e);
            throw new RuntimeException("Can not establish connection to HBase!");
        }
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (session.getQueueSize().getObjectCount() < batchSize) {
            return;
        }
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.size() < batchSize) {
            return;
        }

        List<FlowFile> toSuccess = new ArrayList<>();
        List<FlowFile> toFailure = new ArrayList<>();

        String keyAttrName = context.getProperty(ATTRIBUTE_NAME_TO_EXTRACT_KEY_FROM).getValue();
        ObjectMapper objectMapper = new ObjectMapper();

        //TODO: можно сделать get с фильтром(gпо колонкам) + в отдельный метод
        List<FlowFile> notNullKeyFf = new ArrayList<>();
        List<Get> ffGets = new ArrayList<>();
        flowFiles.forEach(ff -> {
            String attrValue = ff.getAttribute(keyAttrName);
            if (attrValue == null || attrValue.equalsIgnoreCase("NULL") || StringUtils.isBlank(attrValue)) {
                toSuccess.add(ff);
            } else {
                notNullKeyFf.add(ff);
                ffGets.add(new Get(Bytes.toBytesBinary(attrValue)));
            }
        });
        getLogger().debug("Total number of input flowFiles: " + flowFiles.size());
        getLogger().debug("FlowFiles with NULL foreign key: " + toSuccess.size());

        Result[] results;
        try {
            results = table.get().get(ffGets);
        } catch (IOException e) {
            getLogger().warn("Problem with getting result from HBase: " + e.getMessage()
                    + "\n" + Arrays.toString(e.getStackTrace()));
            throw new RuntimeException(e);
        }

        Map<FlowFile, Result> matchedFf = new HashMap<>();
        //todo: hbase documentation guarantees the order
        for (int i = 0; i < notNullKeyFf.size(); i++) {
            FlowFile current = notNullKeyFf.get(i);
            if (results[i].isEmpty()) {
                toFailure.add(current);
            } else {
                matchedFf.put(current, results[i]);
            }
        }
        getLogger().debug("Matched floFiles: " + matchedFf.size());
        getLogger().debug("Not matched flowFiles " + toFailure.size());

        for (Map.Entry<FlowFile, Result> entry : matchedFf.entrySet()) {
            FlowFile ff = entry.getKey();
            Result result = entry.getValue();
            try {
                InputStream in = session.read(ff);
                String jsonInput = IOUtils.toString(in, StandardCharsets.UTF_8);
                in.close();
                //getLogger().debug(jsonInput);
                HashMap<String, Object> jsonMap =
                        objectMapper.readValue(jsonInput, HashMap.class);
                for (Map.Entry<String, String> e : fieldHbaseColumnMappings.entrySet()) {
                    String fieldName = e.getKey();
                    String columnName = e.getValue();
                    byte[] cf = Bytes.toBytes(columnName.split(":")[0]);
                    byte[] column = Bytes.toBytes(columnName.split(":")[1]);
                    byte[] hbaseVal = result.getValue(cf, column);
                    jsonMap.put(fieldName, hbaseVal == null ? "null" : Bytes.toString(hbaseVal));
                }
                String jsonOutput = objectMapper.writeValueAsString(jsonMap);
                ff = session.write(ff, out -> IOUtils.write(jsonOutput, out, StandardCharsets.UTF_8));
                toSuccess.add(ff);
            } catch (Exception ex) {
                ex.printStackTrace();
                getLogger().error("Exception while transferring flowFile content: " + ex.getMessage()
                + "\n" + Arrays.toString(ex.getStackTrace()));
                throw new RuntimeException(ex);
            }
        }
        session.transfer(toSuccess, REL_SUCCESS);
        session.transfer(toFailure, REL_FAILURE);
    }

    //copied from nifi sources

    /**
     * As of Apache NiFi 1.5.0, due to changes made to
     * {@link SecurityUtil#loginKerberos(Configuration, String, String)}, which is used by this
     * class to authenticate a principal with Kerberos, HBase controller services no longer
     * attempt relogins explicitly.  For more information, please read the documentation for
     * {@link SecurityUtil#loginKerberos(Configuration, String, String)}.
     * <p/>
     * In previous versions of NiFi, a {@link org.apache.nifi.hadoop.KerberosTicketRenewer} was started
     * when the HBase controller service was enabled.  The use of a separate thread to explicitly relogin could cause
     * race conditions with the implicit relogin attempts made by hadoop/HBase code on a thread that references the same
     * {@link UserGroupInformation} instance.  One of these threads could leave the
     * {@link javax.security.auth.Subject} in {@link UserGroupInformation} to be cleared or in an unexpected state
     * while the other thread is attempting to use the {@link javax.security.auth.Subject}, resulting in failed
     * authentication attempts that would leave the HBase controller service in an unrecoverable state.
     *
     * @see SecurityUtil#loginKerberos(Configuration, String, String)
     */
    public void prepareConnection(final ProcessContext context) throws IOException, InterruptedException {
        this.connection = createConnection(context);

        // connection check
        if (this.connection != null) {
            final Admin admin = this.connection.getAdmin();
            if (admin != null) {
                admin.listTableNames();
            }
        }
    }

    protected Connection createConnection(final ProcessContext context) throws IOException, InterruptedException {
        final String configFiles = context.getProperty(HADOOP_CONF_FILES).evaluateAttributeExpressions().getValue();
        final Configuration hbaseConfig = getConfigurationFromFiles(configFiles);

        if (SecurityUtil.isSecurityEnabled(hbaseConfig)) {
            String principal = context.getProperty(KERBEROS_PRINCIPAL).getValue();
            String keyTab = context.getProperty(KERBEROS_KEYTAB).getValue();

            ugi = SecurityUtil.loginKerberos(hbaseConfig, principal, keyTab);

            return ugi.doAs(new PrivilegedExceptionAction<Connection>() {
                @Override
                public Connection run() throws Exception {
                    return ConnectionFactory.createConnection(hbaseConfig);
                }
            });

        } else {
            getLogger().info("Simple Authentication");
            return ConnectionFactory.createConnection(hbaseConfig);
        }

    }

    protected Configuration getConfigurationFromFiles(final String configFiles) {
        final Configuration hbaseConfig = HBaseConfiguration.create();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                hbaseConfig.addResource(new Path(configFile.trim()));
            }
        }
        return hbaseConfig;
    }

    public void prepareTable(final ProcessContext context) {
        table = ThreadLocal.withInitial(() -> {
            try {
                return this.connection.getTable(TableName.valueOf(context.getProperty(HBASE_TABLE_NAME).getValue()));
            } catch (IOException e) {
                getLogger().error("Failed to prepare connection to HBase: ", e);
            }
            return null;
        });
    }

    public void prepareFieldHbaseColumnMappings(final ProcessContext context) {
        context.getProperties().forEach((pd, val) -> {
            if (pd.isDynamic())
                fieldHbaseColumnMappings.put(pd.getName(), val);
        });
    }


    @OnDisabled
    public void shutdown() {
        if (connection != null) {
            try {
                connection.close();
            } catch (final IOException ioe) {
                getLogger().warn("Failed to close connection to HBase due to {}", new Object[]{ioe});
            }
        }
    }
}
