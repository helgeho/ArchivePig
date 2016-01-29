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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import static de.l3s.archivepig.Shortcuts.array;
import static de.l3s.archivepig.Shortcuts.field;
import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class HtmlText extends DependentEnrichFunc implements DependencyFunc {
    private String enrichField = "html";

    public HtmlText() {}

    public HtmlText(String fieldName, String selector, String index) {
        setDependency(new Html(fieldName, selector, index));
    }

    public HtmlText(String fieldName, String selector) {
        setDependency(new Html(fieldName, selector));
    }
    public HtmlText(String fieldName) {
        setDependency(new Html(fieldName));
    }

    @Override
    public void enrich(Tuple data, Tuple enrichment, Object... params) throws Exception {
        String html = get(data, enrichField);
        Element element = Jsoup.parse(html);
        enrichment.append(element.text());
    }

    @Override
    public void initDefaultDependency() {
        setDependency(new Html("body", "body"));
    }

    @Override
    public String getExtensionNode() {
        return dependency().getResultingFieldName(enrichField);
    }

    @Override
    public FieldSchema[] getSchemaExtensions() {
        return array(
                field("text", DataType.CHARARRAY)
        );
    }

    @Override
    public String getResultingFieldName(String mapping) {
        switch (mapping) {
            case "text": return "text";
        }
        return null;
    }
}
