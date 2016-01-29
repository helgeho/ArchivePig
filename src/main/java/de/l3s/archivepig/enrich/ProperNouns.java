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

import de.l3s.archivepig.DependentEnrichFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import java.util.ArrayList;
import java.util.List;

import static de.l3s.archivepig.Shortcuts.*;
import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * This EnrichFunc is only meant for demo purposes and does not actually extract proper nouns,
 * instead it tokenizes the text on special characters and return those tokens that start with a capital letter.
 * A real implementation of an EnrichFunc that extracts proper nouns would be very similar though,
 * however, it would require some external library that identifies proper nouns in the enrich(...) method.
 */
public class ProperNouns extends DependentEnrichFunc {
    private String enrichField = "text";

    @Override
    public void enrich(Tuple data, Tuple enrichment, Object... params) throws Exception {
        String text = get(data, enrichField);

        List<Tuple> nouns = new ArrayList<>();
        for(String token : text.split("[\\W]+")) {
            if (token.length() == 0) continue;
            String first = token.substring(0, 1);
            if (first.equals(first.toUpperCase())) nouns.add(tuple(token));
        }
        enrichment.append(BagFactory.getInstance().newDefaultBag(nouns));
    }

    @Override
    public void initDefaultDependency() {
        setDependency(new HtmlText());
    }

    @Override
    public String getExtensionNode() {
        return dependency().getResultingFieldName(enrichField);
    }

    @Override
    public FieldSchema[] getSchemaExtensions() {
        return array(
                field("nouns", DataType.BAG, array(
                        field(null, array(
                                field(null, DataType.CHARARRAY)
                        ))
                ))
        );
    }
}
