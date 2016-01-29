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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.ArrayList;
import java.util.List;

import static de.l3s.archivepig.Shortcuts.*;
import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public abstract class EnrichFunc extends DataEvalFunc<Tuple> {
    private FieldSchema[] schemaExtensions = null;

    public abstract FieldSchema[] getSchemaExtensions();

    public abstract void enrich(Tuple data, Tuple enrichment, Object... params) throws Exception;

    public abstract String getEnrichNode();

    public abstract String getExtensionNode();

    public String extensionNode() {
        return getEnrichNode() + "." + getExtensionNode();
    }

    @Override
    public Tuple exec(Tuple data, Object... params) throws ExecException {
        if (checkEnrichmentAvailable()) return data;

        data = prepareData(data);

        Tuple enrichment;
        try {
            enrichment = tuple();
            enrich(data, enrichment, params);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            data = enrichData(dataSchema(), data, enrichment);
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }

        return data;
    }

    protected Tuple enrichData(Schema dataSchema, Tuple data, Tuple enrichment) throws FrontendException, ExecException {
        Tuple enriched = dup(data);
        Schema enrichSchema = dup(dataSchema);
        Tuple enrichNode = enriched;
        String[] fields = extensionNode().split("\\.");
        boolean extending = false;
        for (String field : fields) {
            if (field == null || field.length() == 0) continue;
            extending = extending || !enrichSchema.getAliases().contains(field);
            if (extending) {
                Tuple tuple = tuple();
                enrichNode.append(tuple);
                enrichNode = tuple;

                enrichSchema = null;
            } else {
                FieldSchema fieldSchema = enrichSchema.getField(field);
                int position = enrichSchema.getPosition(field);
                if (fieldSchema.type == DataType.TUPLE) {
                    Tuple tuple = dup((Tuple)enrichNode.get(position));
                    enrichNode.set(position, tuple);
                    enrichNode = tuple;

                    enrichSchema = fieldSchema.schema;
                } else {
                    Tuple tuple = tuple(enrichNode.get(position));
                    enrichNode.set(position, tuple);
                    enrichNode = tuple;

                    enrichSchema = new Schema(new ArrayList<>(list(field(ArchivePigConst.VALUE_FIELDNAME, fieldSchema.type))));
                    extending = true;
                }
            }
        }

        if (enrichSchema == null) enrichSchema = new Schema();
        for (int i = 0; i < schemaExtensions().length; i++) {
            String alias = schemaExtensions[i].alias;
            if (enrichSchema.getAliases().contains(alias)) {
                FieldSchema field = enrichSchema.getField(alias);
                if (field.type == DataType.TUPLE && !field.schema.getAliases().contains(ArchivePigConst.VALUE_FIELDNAME)) {
                    int position = enrichSchema.getPosition(alias);
                    List<Object> elements = new ArrayList<>(((Tuple)enrichNode.get(position)).getAll());
                    elements.add(0, enrichment.size() > i ? enrichment.get(i) : null);
                    enrichNode.set(position, TupleFactory.getInstance().newTuple(elements));
                } else {
                    continue;
                }
            } else {
                enrichNode.append(enrichment.size() > i ? enrichment.get(i) : null);
            }
        }

        return enriched;
    }

    protected Tuple prepareData(Tuple data) {
        return data;
    }

    public <T> T get(Tuple tuple, String field, boolean root) {
        if (!root) field = getEnrichNode() + "." + field;
        return super.get(tuple, field);
    }

    public <T> T get(Tuple tuple, String field) {
        T val = get(tuple, field, false);
        if (val == null) val = get(tuple, field, true);
        return val;
    }

    private FieldSchema[] schemaExtensions() {
        if (schemaExtensions == null) schemaExtensions = getSchemaExtensions();
        return schemaExtensions;
    }

//      TODO: comment in, enable same recursion for outputschema and enrichment in the exec method
//      private boolean checkFieldAvailable(Schema schema, FieldSchema field) throws FrontendException {
//        if (schema == null) return false;
//
//        FieldSchema fieldSchema = schema.getField(field.alias);
//        if (fieldSchema == null) return false;
//        if (field.schema != null) {
//            if (fieldSchema.schema == null) return false;
//            for (FieldSchema child : field.schema.getFields()) {
//                if (!checkFieldAvailable(fieldSchema.schema, child)) return false;
//            }
//        }
//
//        return true;
//    }

    private boolean checkEnrichmentAvailable() {
        String[] fields = extensionNode().split("\\.");
        Schema schema = dataSchema();
        for (int i = 0; i < fields.length; i++) {
            try {
                FieldSchema fieldSchema = schema.getField(fields[i]);
                if (fieldSchema == null || fieldSchema.schema == null) return false;
                schema = fieldSchema.schema;
            } catch (FrontendException e) {
                throw new RuntimeException(e);
            }
        }

        for (FieldSchema field : schemaExtensions()) {
            if (!schema.getAliases().contains(field.alias)) return false;
            try {
                FieldSchema fieldSchema = schema.getField(field.alias);
                if (fieldSchema.type == DataType.TUPLE) {
                    if (!fieldSchema.schema.getAliases().contains(ArchivePigConst.VALUE_FIELDNAME)) return false;
                }
            } catch (FrontendException e) {
                throw new RuntimeException(e);
            }
        }

        return true;
    }

    @Override
    public Schema outputSchema(Schema input) {
        Schema enrichedSchema, enrichNode;
        try {
            enrichNode = enrichedSchema = dup(input.getField(0).schema);

            String[] fields = extensionNode().split("\\.");
            boolean extending = false;
            for (String field : fields) {
                if (field.length() == 0) continue;

                extending = extending || !enrichNode.getAliases().contains(field);
                if (extending) {
                    FieldSchema fieldSchema = field(field, DataType.TUPLE);
                    enrichNode.add(fieldSchema);
                    enrichNode = new Schema();
                    fieldSchema.schema = enrichNode;
                } else {
                    FieldSchema fieldSchema = enrichNode.getField(field);
                    if (fieldSchema.type == DataType.TUPLE) {
                        enrichNode = fieldSchema.schema;
                    } else {
                        enrichNode = new Schema(new ArrayList<>(list(field(ArchivePigConst.VALUE_FIELDNAME, fieldSchema.type))));
                        fieldSchema.schema = enrichNode;
                        fieldSchema.type = DataType.TUPLE;
                        extending = true;
                    }
                }
            }

            for (FieldSchema field : schemaExtensions()) {
                field = dup(field);
                FieldSchema inputField = enrichNode.getField(field.alias);
                if (inputField == null) {
                    enrichNode.add(field);
                } else if (inputField.type == DataType.TUPLE && !inputField.schema.getAliases().contains(ArchivePigConst.VALUE_FIELDNAME)) {
                    field.alias = ArchivePigConst.VALUE_FIELDNAME;
                    List<FieldSchema> fieldFields = new ArrayList<>(inputField.schema.getFields());
                    fieldFields.add(0, field);
                    inputField.schema = new Schema(fieldFields);
                }

//                TODO: comment in, implement recursion, same for checkEnrichmentAvailable and enrichment in the exec method
//                else if (inputField.type == DataType.TUPLE) {
//                    if (field.type == DataType.TUPLE) {
//                        inputField.schema.merge(field.schema, false);
//                    } else if(inputField.schema.getField(valueFieldName) == null) {
//                        inputField.schema.add(new FieldSchema(valueFieldName, inputField.type));
//                    }
//                } else if (field.type == DataType.TUPLE) {
//                    Schema fieldSchema = new Schema(list(new FieldSchema(valueFieldName, inputField.type)));
//                    inputField.schema = fieldSchema;
//                    inputField.type = DataType.TUPLE;
//                    fieldSchema.merge(field.schema, false);
//                }
            }
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }

        return new Schema(list(new FieldSchema(ArchivePigConst.DATATUPLE, enrichedSchema)));
    }
}