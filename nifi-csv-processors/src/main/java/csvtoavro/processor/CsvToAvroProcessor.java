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
package csvtoavro.processor;

import com.cdr.CDR;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public class CsvToAvroProcessor extends AbstractProcessor {

    private FlowFile flowFile;
    private String line;

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("Success")
            .description("Success")
            .build();

    public static final Relationship ERROR_RELATIONSHIP = new Relationship.Builder()
            .name("Failure")
            .description("Failure")
            .build();

    private Set<Relationship> relationships;

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(ERROR_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        long timestamp = new Timestamp(System.currentTimeMillis()).getTime();

        String fileName = flowFile.getAttribute("filename");
        session.putAttribute(flowFile, "filename", fileName.substring(0, fileName.lastIndexOf('.')) + "_" + timestamp + ".avro");

        flowFile = session.write(flowFile, (inputStream, outputStream) -> {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

            final DatumWriter<CDR> datumWriter = new SpecificDatumWriter<>(CDR.class);
            try (DataFileWriter<CDR> dataFileWriter = new DataFileWriter<>(datumWriter)) {
                CDR.Builder cdrBuilder = CDR.newBuilder();
                CDR cdr = new CDR();
                dataFileWriter.create(cdr.getSchema(), outputStream);
                while ((line = bufferedReader.readLine()) != null) {
                    String[] arr = line.split(",");
                    cdrBuilder.setMSISDNA(arr[0]);
                    cdrBuilder.setMSISDNB(arr[1]);
                    if(arr[0].substring(0, 3).equals(arr[1].substring(0, 3))){
                        cdrBuilder.setServiceName("sameOperatorCall");
                    }
                    else{
                        cdrBuilder.setServiceName("localCall");
                    }
                    cdrBuilder.setCallTime(Long.parseLong(arr[2]));
                    cdrBuilder.setDuration(Integer.parseInt(arr[3]));
                    cdr = cdrBuilder.build();
                    dataFileWriter.append(cdr);
                }
            } catch (IOException ex) {
                logger.error("\nPROCESSOR ERROR 1: " + ex.getMessage() + "\n");
                bufferedReader.close();
                session.transfer(flowFile, ERROR_RELATIONSHIP);
                return;
            }

            logger.info("Successfully transfer file :)");
            bufferedReader.close();
        });
        session.transfer(flowFile, SUCCESS_RELATIONSHIP);
    }
}
