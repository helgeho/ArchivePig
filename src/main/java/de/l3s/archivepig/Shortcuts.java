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

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.*;

import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public abstract class Shortcuts {
    private Shortcuts() {}

    public static FieldSchema field(String name, byte type) {
        return new FieldSchema(name, type);
    }

    public static FieldSchema field(String name, FieldSchema[] tuple) {
        Schema schema = new Schema(new ArrayList(Arrays.asList(tuple)));
        return new FieldSchema(name, schema);
    }

    public static FieldSchema field(String name, byte type, FieldSchema[] children) {
        FieldSchema field = new FieldSchema(name, type);
        field.schema = new Schema(new ArrayList(Arrays.asList(children)));
        return field;
    }

    public static Tuple tuple(Object... objects) {
        return TupleFactory.getInstance().newTuple(Arrays.asList(objects));
    }

    public static <T> List<T> list(T... elements) {
        return Arrays.asList(elements);
    }

    public static <T> T[] array(List<T> elements) {
        return elements.toArray(Shortcuts.<T>array());
    }

    public static <T> T[] array(T... elements) {
        return elements;
    }

    public static void print(String str) {
        System.out.println(str);
    }

    public static Tuple dup(Tuple tuple) {
        return TupleFactory.getInstance().newTuple(tuple.getAll());
    }

    public static Schema dup(Schema schema) {
        if (schema == null) return null;
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        for (FieldSchema field : schema.getFields()) fields.add(dup(field));
        return new Schema(fields);
    }

    public static FieldSchema dup(FieldSchema schema) {
        try {
            return new FieldSchema(schema.alias, dup(schema.schema), schema.type);
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public static String lines(String... lines) {
        String output = "";
        boolean first = true;
        for (String line : lines) {
            if (!first) output += "\n";
            output += line;
            first = false;
        }
        return output;
    }

    public static <K, V> Map.Entry<K, V> kv(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public static <K,V> Map<K, V> map(Map.Entry<K, V>... entries) {
        Map<K, V> map = new HashMap<K, V>();
        for (Map.Entry<K, V> kv : entries) {
            map.put(kv.getKey(), kv.getValue());
        }
        return map;
    }
}
