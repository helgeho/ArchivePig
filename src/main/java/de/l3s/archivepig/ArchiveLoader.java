/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Helge Holzmann
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.l3s.archivepig;

import de.l3s.archivepig.utils.ContextProperty;
import de.l3s.archivepig.utils.ContextualPigObject;
import de.l3s.archivepig.utils.ContextualPigStatic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static de.l3s.archivepig.Shortcuts.*;
import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class ArchiveLoader extends LoadFunc implements LoadMetadata {
    private static Schema cdxSchema = null;
    private static List<String> inputFormat = list("url", "time", "fullUrl", "type", "status", "checksum", "redirectUrl", "meta", "size", "offset", "filename");

    private static ContextualPigStatic _static = new ContextualPigStatic(ArchiveLoader.class);
    private static ContextProperty<String> staticDataPath = new ContextProperty<>("dataPath");

    public static String dataPath() {
        return _static.get(staticDataPath);
    }

    private String loadLocation;
    private String inputFormatClassName = null;
    private RecordReader reader = null;

    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private ContextualPigObject _ = null;

    public ArchiveLoader(String dataPath) {
        _static.set(staticDataPath, dataPath);
    }

    @Override
    public void setUDFContextSignature(String signature) {
        _ = new ContextualPigObject(this, signature);
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, loadLocation);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        // if not manually set in options string
        if (inputFormatClassName == null) {
            if (loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
                inputFormatClassName = Bzip2TextInputFormat.class.getName();
            } else {
                inputFormatClassName = TextInputFormat.class.getName();
            }
        }
        try {
            return (FileInputFormat) PigContext.resolveClassName(inputFormatClassName).newInstance();
        } catch (InstantiationException e) {
            throw new IOException("Failed creating input format " + inputFormatClassName, e);
        } catch (IllegalAccessException e) {
            throw new IOException("Failed creating input format " + inputFormatClassName, e);
        }
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        reader = recordReader;
    }

    public static DateTime dateTime(String str) {
        try {
            int year = Integer.parseInt(str.substring(0, 4));
            int month = Integer.parseInt(str.substring(4, 6));
            int day = Integer.parseInt(str.substring(6, 8));
            int hour = Integer.parseInt(str.substring(8, 10));
            int minute = Integer.parseInt(str.substring(10, 12));
            int second = Integer.parseInt(str.substring(12, 14));
            return new DateTime(year, month, day, hour, minute, second, 0);
        } catch (Exception e) {
            return null;
        }
    }

    public static int toInt(String str) {
        try {
            return Integer.parseInt(str);
        } catch (Exception e) {
            return -1;
        }
    }

    public static long toLong(String str) {
        try {
            return Long.parseLong(str);
        } catch (Exception e) {
            return -1;
        }
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            while(reader.nextKeyValue()) {
                Text line = (Text) reader.getCurrentValue();
                String[] elements = line.toString().split("[ \t]");

                if (elements.length != inputFormat.size()) continue;

                Tuple cdx = tuple(
                        elements[inputFormat.indexOf("url")],
                        tuple(
                                dateTime(elements[inputFormat.indexOf("time")]),
                                elements[inputFormat.indexOf("fullUrl")],
                                elements[inputFormat.indexOf("type")],
                                toInt(elements[inputFormat.indexOf("status")]),
                                elements[inputFormat.indexOf("checksum")],
                                elements[inputFormat.indexOf("redirectUrl")],
                                elements[inputFormat.indexOf("meta")],
                                tuple(
                                        toLong(elements[inputFormat.indexOf("size")]),
                                        toLong(elements[inputFormat.indexOf("offset")]),
                                        elements[inputFormat.indexOf("filename")],
                                        loadLocation
                                )
                        )
                );

                return tuple(cdx);
            }
            return null;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
        }
    }

    public static Schema getCdxSchema() throws FrontendException {
        if (cdxSchema != null) return cdxSchema;

        cdxSchema = new Schema();
        cdxSchema.add(field("url", DataType.CHARARRAY));
        cdxSchema.add(field("capture", array(
                field("time", DataType.DATETIME),
                field("fullUrl", DataType.CHARARRAY),
                field("type", DataType.CHARARRAY),
                field("status", DataType.CHARARRAY),
                field("checksum", DataType.CHARARRAY),
                field("redirectUrl", DataType.CHARARRAY),
                field("meta", DataType.CHARARRAY),
                field("_record", array(
                        field("size", DataType.LONG),
                        field("offset", DataType.LONG),
                        field("filename", DataType.CHARARRAY),
                        field("cdxFile", DataType.CHARARRAY)
                ))
        )));

        return cdxSchema;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        List<FieldSchema> fieldSchemaList = new ArrayList<FieldSchema>();

        fieldSchemaList.add(new FieldSchema(ArchivePigConst.DATATUPLE, getCdxSchema()));

        return new ResourceSchema(new Schema(fieldSchemaList));
    }

    @Override
    public ResourceStatistics getStatistics(String s, Job job) throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(String s, Job job) throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression expression) throws IOException {
        // intentionally not implemented
    }
}