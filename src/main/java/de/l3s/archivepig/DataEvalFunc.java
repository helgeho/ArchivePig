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

import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.Arrays;
import java.util.List;

import static de.l3s.archivepig.Shortcuts.array;
import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public abstract class DataEvalFunc<T> extends EvalFunc<T> {
    private Schema dataInputSchema = null;

    public void setDataInputSchema(Schema schema) {
        dataInputSchema = schema;
    }

    public Schema getDataInputSchema() {
        return dataInputSchema;
    }

    public Schema dataSchema() {
        try {
            if (dataInputSchema != null) return dataInputSchema.getField(0).schema;
            return getInputSchema().getField(0).schema;
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract T exec(Tuple data, Object... params) throws ExecException;

    @Override
    public T exec(Tuple tuple) throws ExecException {
        List<Object> params = tuple.getAll();
        Tuple data = (Tuple)params.remove(0);

        return exec(data, array(params));
    }

    public <T> T get(Tuple tuple, String field) {
        return get(tuple, dataSchema(), field);
    }

    public <T> T get(Tuple tuple, Schema schema, String field) {
        String[] fields = field.split("\\.");
        for (int i = 0; i < fields.length; i++) {
            boolean wildcard = fields[i].equals("");
            if (wildcard) i++;
            boolean last = (i == (fields.length - 1));
            try {
                FieldSchema fieldSchema = schema.getField(fields[i]);
                if (fieldSchema == null) {
                    if (wildcard) {
                        String subWildcardQuery = "." + StringUtils.join(Arrays.copyOfRange(fields, i, fields.length), ".");
                        for (FieldSchema wildField : schema.getFields()) {
                            if (wildField.type != DataType.TUPLE) continue;
                            try {
                                T wildValue = get((Tuple) tuple.get(schema.getPosition(wildField.alias)), wildField.schema, subWildcardQuery);
                                if (wildValue != null) return wildValue;
                            } catch (Exception e) {
                                continue;
                            }
                        }
                    }
                    return null;
                }
                if (last) {
                    Object value = tuple.get(schema.getPosition(fields[i]));
                    if (fieldSchema.type == DataType.TUPLE && fieldSchema.schema.getAliases().contains(ArchivePigConst.VALUE_FIELDNAME)) {
                        return (T)((Tuple)value).get(fieldSchema.schema.getPosition(ArchivePigConst.VALUE_FIELDNAME));
                    } else {
                        return (T)value;
                    }
                }
                if (fieldSchema.schema == null) return null;
                tuple = (Tuple)tuple.get(schema.getPosition(fields[i]));
                schema = fieldSchema.schema;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema("data", DataType.BYTEARRAY));
    }
}