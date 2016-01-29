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

package de.l3s.archivepig.enrich;

import de.l3s.archivepig.DependencyFunc;
import de.l3s.archivepig.DependentEnrichFunc;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import static de.l3s.archivepig.Shortcuts.array;
import static de.l3s.archivepig.Shortcuts.field;
import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class StringContent extends DependentEnrichFunc implements DependencyFunc {
    private String enrichField = "content";

    @Override
    public void enrich(Tuple data, Tuple enrichment, Object... params) throws Exception {
        byte[] payload = get(data, enrichField);
        HttpEntity entity = new ByteArrayEntity(payload);
        enrichment.append(EntityUtils.toString(entity).trim());
    }

    @Override
    public void initDefaultDependency() {
        setDependency(new Response());
    }

    @Override
    public String getExtensionNode() {
        return dependency().getResultingFieldName(enrichField);
    }

    @Override
    public FieldSchema[] getSchemaExtensions() {
        return array(
                field("string", DataType.CHARARRAY)
        );
    }

    @Override
    public String getResultingFieldName(String mapping) {
        switch (mapping) {
            case "text": return "string";
        }
        return null;
    }
}
