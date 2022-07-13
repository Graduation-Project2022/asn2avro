package samples.processors.utils;

import com.cdr.CDR;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Asn1ToAvroProcessor extends AbstractProcessor {

    private FlowFile flowFile;
    private Reader streamReader;
    private String strDate;
    private StringBuilder streamData;

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
        try {

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS");
            Date now = new Date();
            strDate = sdf.format(now);
            String fileName = flowFile.getAttribute("filename");
            session.putAttribute(flowFile, "filename", fileName.substring(0, fileName.lastIndexOf('.')) + "_" + strDate + ".avro");


            flowFile = session.write(flowFile, (inputStream, outputStream) -> {

                streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                int data = streamReader.read();
                streamData = new StringBuilder();
                while (data != -1) {
                    streamData.append((char) data);
                    data = streamReader.read();
                }

                List<StringBuilder> allNumberA = getHexNumberA(streamData);
                List<StringBuilder> allNumberB = getHexNumberB(streamData);
                List<StringBuilder> allHexTimestamp = getHexTimestamp(streamData);
                List<StringBuilder> allHexDurations = getHexDuration(streamData);
                List<StringBuilder> serviceName = new ArrayList<>();
                for (int i = 0 ; i< allNumberA.size();i++){
                 if(allNumberA.get(i).substring(0, 2).equals(allNumberB.get(i).substring(0, 2))){
                        serviceName.add(new StringBuilder("sameOperatorCall"));
                 }
                 else {
                     serviceName.add(new StringBuilder("localCall"));
                 }
                };

                allNumberA.replaceAll(Asn1ToAvroProcessor::toPhoneNumber);
                allNumberB.replaceAll(Asn1ToAvroProcessor::toPhoneNumber);

                List<Long> allTimestamp = new ArrayList<>();
                for (StringBuilder hexTimestamp : allHexTimestamp) {
                    allTimestamp.add(toTimestamp(hexTimestamp));
                }
                List<Integer> allDuration = new ArrayList<>();
                for (StringBuilder hexDuration : allHexDurations) {
                    allDuration.add(toDuration(hexDuration));
                }

                final DatumWriter<CDR> datumWriter = new SpecificDatumWriter<>(CDR.class);
                try (DataFileWriter<CDR> dataFileWriter = new DataFileWriter<>(datumWriter)) {
                    CDR.Builder cdrBuilder = CDR.newBuilder();
                    CDR cdr = new CDR();
                    dataFileWriter.create(cdr.getSchema(), outputStream);
                    for (int i = 0; i < allNumberA.size(); i++) {
                        cdrBuilder.setMSISDNA(allNumberA.get(i).toString());
                        cdrBuilder.setMSISDNB(allNumberB.get(i).toString());
                        cdrBuilder.setCallTime(allTimestamp.get(i));
                        cdrBuilder.setServiceName(serviceName.get(i).toString());
                        cdrBuilder.setDuration(allDuration.get(i));
                        cdr = cdrBuilder.build();
                        dataFileWriter.append(cdr);
                    }
                } catch (IOException ex) {
                    try {

                        logger.error("\nPROCESSOR ERROR 1: " + ex.getMessage() + "\n");
                        session.transfer(flowFile, ERROR_RELATIONSHIP);
                        streamReader.close();
                        return;
                    } catch (IOException exception) {
                        logger.error("\nPROCESSOR ERROR 2: " + exception.getMessage() + "\n");
                    }
                }


                logger.info("Successfully transfer file :)");
                streamReader.close();
            });

            session.transfer(flowFile, SUCCESS_RELATIONSHIP);
        } catch (Exception ex) {
            logger.error("\nPROCESSOR ERROR 3: " + ex.getMessage() + "\n");
            session.transfer(flowFile, ERROR_RELATIONSHIP);
            try {
                streamReader.close();
            } catch (IOException exception) {
                logger.error("\nPROCESSOR ERROR 4: " + exception.getMessage() + "\n");
            }
        }
    }


    // Getting methods
    public static List<StringBuilder> getHexNumberA(StringBuilder file) {
        List<StringBuilder> allNumberA = new ArrayList<>();
        Pattern pattern = Pattern.compile("a6 13 81 11 74 65 6c 3a 2b 32 30 31 .. .. .. .. .. .. .. .. .. *");
        Matcher matcher = pattern.matcher(file);
        while (matcher.find()) {
            allNumberA.add(new StringBuilder(matcher.group().substring(36, 62)));
        }
        return allNumberA;
    }

    public static List<StringBuilder> getHexNumberB(StringBuilder file) {
        List<StringBuilder> allNumberB = new ArrayList<>();
        Pattern pattern = Pattern.compile("a7 13 81 11 74 65 6c 3a 2b 32 30 31 .. .. .. .. .. .. .. .. .. *");
        Matcher matcher = pattern.matcher(file);
        while (matcher.find()) {
            allNumberB.add(new StringBuilder(matcher.group().substring(36, 62)));
        }
        return allNumberB;
    }

    public static List<StringBuilder> getHexTimestamp(StringBuilder file) {
        List<StringBuilder> allDateTime = new ArrayList<>();
        Pattern pattern = Pattern.compile("89 09 .. .. .. .. .. .. 2B 02 00 *");
        Matcher matcher = pattern.matcher(file);
        while (matcher.find()) {
            allDateTime.add(new StringBuilder(matcher.group().substring(6, 23)));
        }
        return allDateTime;
    }

    public static List<StringBuilder> getHexDuration(StringBuilder file) {
        List<StringBuilder> allDuration = new ArrayList<>();
        Pattern pattern = Pattern.compile("9f 81 48 0. .. .. ..*");
        Matcher matcher = pattern.matcher(file);
        while (matcher.find()) {
            switch (matcher.group().charAt(10)) {
                case '4':
                    allDuration.add(new StringBuilder(matcher.group().substring(12, 23)));
                    break;
                case '3':
                    allDuration.add(new StringBuilder(matcher.group().substring(12, 20)));
                    break;
            }
        }
        return allDuration;
    }

    public static StringBuilder toPhoneNumber(StringBuilder hexPhone) {
        hexPhone.replace(0, 0, "01");
        for (int i = 2; i < 19; i += 2) {
            hexPhone.deleteCharAt(i);
        }
        return new StringBuilder(hexPhone.toString().replace(" ", ""));
    }

    public static long toTimestamp(StringBuilder hexDateTime) {
        hexDateTime.insert(0, "20");
        Date date = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy MM dd HH mm ss");
        try {
            date = dateFormat.parse(hexDateTime.toString());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if (date != null) {
            return date.getTime();
        } else {
            return 0;
        }
    }

    public static int toDuration(StringBuilder hexDuration) {
        return (Integer.parseInt(hexDuration.toString().replace(" ", ""), 16) / 100000);
    }
}
