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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static de.l3s.archivepig.Shortcuts.*;
import static org.apache.pig.ResourceSchema.ResourceFieldSchema;

public class ExtractionStorage extends StoreFunc {
    private static final int JSON_INDENT = 4;

    private static class NamedField implements Serializable {
        public NamedField() {}
        public NamedField(String name, NamedField[] fields) {
            this.name = name;
            this.fields = fields;
        }
        public NamedField(String name) {
            this.name = name;
        }
        public String name;
        public NamedField[] fields = new NamedField[0];
    }

    public static void main(String[] args) throws Exception {
        Map<String, Integer> map = map(
                kv("one", 1),
                kv("two", 2),
                kv("three", 3),
                kv("four", 4)
        );
        Tuple tuple = tuple("1", "2", tuple("3", "4"), map);
        NamedField[] fields = array(
                new NamedField("a"),
                new NamedField("b"),
                new NamedField("c", array(
                        new NamedField("d"),
                        new NamedField("e")
                )),
                new NamedField("map")
        );
        print(toJson(tuple, fields).toString(JSON_INDENT));
    }

    protected RecordWriter writer = null;

    private ContextProperty<NamedField[]> namedFields = new ContextProperty<>("namedFields");

    private ContextualPigObject _ = null;

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        _ = new ContextualPigObject(this, signature);
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new TextOutputFormat<WritableComparable, Text>();
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
        if (location.endsWith(".bz2")) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job,  BZip2Codec.class);
        }  else if (location.endsWith(".gz")) {
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        }
    }

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        Tuple data = (Tuple)tuple.getAll().get(0);
        try {
            JSONObject json = toJson(data, _.get(namedFields));
            String jsonString = null;
            jsonString = json.toString(JSON_INDENT);
            Text text = new Text(jsonString);
            writer.write(null, text);
        } catch (JSONException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static JSONObject toJson(Tuple tuple, NamedField[] fields) throws JSONException {
        JSONObject json = new JSONObject();

        List<Object> values = tuple.getAll();
        int i = 0;
        for (Object val : values) {
            NamedField field = i < fields.length ? fields[i] : new NamedField();
            String name = field.name;

            if (name == null || (name.startsWith("_") && !name.equals("_"))) {
                i++;
                continue;
            }

            if (Tuple.class.isInstance(val)) {
                JSONObject child = toJson((Tuple)val, field.fields);
                json.put(name, child);
            } else if (DataBag.class.isInstance(val)) {
                JSONArray array = new JSONArray();
                DataBag bag = (DataBag) val;
                for (Tuple child : bag) {
                    if (child.size() == 1 && (field.fields[0].name == null)) {
                        try {
                            array.put(child.get(0));
                        } catch (ExecException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        array.put(toJson(child, field.fields));
                    }
                }
                json.put(name, array);
            } else if (Map.class.isInstance(val)) {
                JSONObject child = new JSONObject();
                Map map = (Map) val;
                Set<Map.Entry> entrySet = map.entrySet();
                for (Map.Entry kv : entrySet) {
                    if (kv.getKey() == null) continue;
                    child.put(kv.getKey().toString(), kv.getValue() == null ? JSONObject.NULL : kv.getValue().toString());
                }
                json.put(name, child);
            } else {
                json.put(name, val == null ? JSONObject.NULL : val);
            }

            i++;
        }

        return json;
    }

    @Override
    public void checkSchema(ResourceSchema schema) throws IOException {
        ResourceSchema inputSchema = schema.getFields()[0].getSchema();
        ResourceFieldSchema[] fields = inputSchema.getFields();
        _.set(namedFields, new NamedField[fields.length]);
        for (int i = 0; i < fields.length; i++) _.get(namedFields)[i] = namedField(fields[i]);
    }

    private NamedField namedField(ResourceFieldSchema schema) {
        NamedField field = new NamedField();
        field.name = schema.getName();

        if (schema.getType() == DataType.TUPLE || schema.getType() == DataType.BAG) {
            ResourceFieldSchema[] fields = schema.getSchema().getFields();
            field.fields = new NamedField[fields.length];
            for (int i = 0; i < fields.length; i++) field.fields[i] = namedField(fields[i]);
        }

        return field;
    }
}
