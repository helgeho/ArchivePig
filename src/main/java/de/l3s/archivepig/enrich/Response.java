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

import com.google.common.collect.Lists;
import de.l3s.archivepig.ArchiveLoader;
import de.l3s.archivepig.DependencyFunc;
import de.l3s.archivepig.EnrichFunc;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.archive.format.http.HttpHeader;
import org.archive.format.http.HttpHeaders;
import org.archive.format.http.HttpResponse;
import org.archive.format.http.HttpResponseParser;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import static de.l3s.archivepig.Shortcuts.*;
import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class Response extends EnrichFunc implements DependencyFunc {
    @Override
    public void enrich(Tuple data, Tuple enrichment, Object... params) throws Exception {
        long size = get(data, "_record.size");
        long offset = get(data, "_record.offset");
        String filename = get(data, "_record.filename");
        String cdxFile = get(data, "_record.cdxFile");

        if (size < 0 || offset < 0) return;

        FileSystem fs = FileSystem.get(UDFContext.getUDFContext().getJobConf());

        Deque<String> cdxSegments = new ArrayDeque<String>(Lists.reverse(list(cdxFile.split("\\/"))));
        cdxSegments.pop(); // remove filename
        String pathExtension = "";
        Path path = new Path(ArchiveLoader.dataPath(), pathExtension + filename);
        while (!fs.exists(path)) {
            if (cdxSegments.isEmpty()) {
                enrichment.append(new HashMap<String, String>());
                enrichment.append(new HashMap<String, String>());
                enrichment.append(null);
                return;
            }
            String cdxSegment = cdxSegments.pop();
            if (cdxSegment.endsWith(".har")) cdxSegment = cdxSegment.substring(0, cdxSegment.length() - 4);
            pathExtension = cdxSegment + "/" + pathExtension;
            path = new Path(ArchiveLoader.dataPath(), pathExtension + filename);
        }
        FSDataInputStream fsin = fs.open(path);
        fsin.seek(offset);
        InputStream in = fsin;

        ByteArrayOutputStream recordOutput = new ByteArrayOutputStream();
        try {
            try (BoundedInputStream boundedIn = new BoundedInputStream(in, size);
                 ArchiveReader reader = ArchiveReaderFactory.get(filename, boundedIn, false);) {
                ArchiveRecord record;
                record = reader.get();

                ArchiveRecordHeader header = record.getHeader();
                enrichment.append(header.getHeaderFields());

                record.dump(recordOutput);
            } catch (Exception e) {
                return;
            } finally {
                in.close();
                recordOutput.close();
            }
        } catch (Exception e) {
            return;
        }

        try (InputStream httpResponse = new ByteArrayInputStream(recordOutput.toByteArray())) {
            // ALL COMMENTS ARE NEW VERSION VARIANTS FOR HTTP-CORE 4.3, currently in use 4.2.5
            //        SessionInputBufferImpl sessionInputBuffer = new SessionInputBufferImpl(new HttpTransportMetricsImpl(), 2048);
            //        sessionInputBuffer.bind(httpResponse);
            //        DefaultHttpResponseParserFactory responseParserFactory = new DefaultHttpResponseParserFactory();
            //        HttpMessageParser<HttpResponse> responseParser = responseParserFactory.create(sessionInputBuffer, MessageConstraints.DEFAULT);
            //        HttpResponse response = responseParser.parse();
            //        Header[] httpHeaders = response.getAllHeaders();

            HttpResponseParser parser = new HttpResponseParser();
            HttpResponse response = parser.parse(httpResponse);
            HttpHeaders httpHeaders = response.getHeaders();

            Map<String, String> httpHeadersMap = new HashMap<String, String>();
            for (HttpHeader httpHeader : httpHeaders) {
                httpHeadersMap.put(httpHeader.getName(), httpHeader.getValue());
            }
            enrichment.append(httpHeadersMap);

            //        byte[] payload = new byte[sessionInputBuffer.length()];
            //        sessionInputBuffer.read(payload);

            byte[] payload = IOUtils.toByteArray(response);

            enrichment.append(payload);

            //        HttpEntity entity = new ByteArrayEntity(payload);
            //        output.append(entity == null ? null : EntityUtils.toString(entity));
        } catch (Exception ignored) { }
    }

    @Override
    public String getEnrichNode() {
        return "capture";
    }

    @Override
    public String getExtensionNode() {
        return "response";
    }

    @Override
    public FieldSchema[] getSchemaExtensions() {
        return array(
                field("header", DataType.MAP),
                field("httpHeader", DataType.MAP),
                field("payload", DataType.BYTEARRAY)
        );
    }

    @Override
    public String getResultingFieldName(String mapping) {
        switch (mapping) {
            case "content": return "payload";
        }
        return null;
    }
}
