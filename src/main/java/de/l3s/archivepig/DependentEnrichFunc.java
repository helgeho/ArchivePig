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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.ArrayList;
import java.util.List;

import static de.l3s.archivepig.Shortcuts.list;

public abstract class DependentEnrichFunc extends EnrichFunc implements DependentFunc {
    private DependencyFunc dependencyFunc = null;
    private Object[] dependencyArgs = null;
    private Tuple originalData = null;

    private boolean applyDependency = true;
    private boolean applyDependencyManuallySet = false;

    @Override
    public void setApplyDependency(boolean enrich) {
        applyDependencyManuallySet = true;
        applyDependency = enrich;
    }

    public void setDependency(DependencyFunc dependency, Object... args) {
        setDependency(dependency);
        setDependencyArguments(args);
    }

    @Override
    public Schema getInputSchema() {
        Schema schema = super.getInputSchema();
        if (schema != null) return schema;
        return getDataInputSchema();
    }

    @Override
    public void setInputSchema(Schema input) {
        // only for internal use, so it is only called from Pig itself, when applying this UDF directly
        if (!applyDependencyManuallySet) applyDependency = false;
        super.setInputSchema(input);
    }

    @Override
    public void setDependency(DependencyFunc dependency) {
        dependencyFunc = dependency;
    }

    public void initDefaultDependency() {
        throw new RuntimeException("missing default dependency initialization");
    }

    public DependencyFunc dependency() {
        if (dependencyFunc == null) initDefaultDependency();
        return dependencyFunc;
    }

    @Override
    public void setDependencyArguments(Object... args) {
        dependencyArgs = args;
    }

    @Override
    protected Tuple prepareData(Tuple data) {
        // run dependency function and return resulting tuple as input input tuple for this (dependent) function

        originalData = data;

        Object[] args = dependencyArgs == null ? new Object[0] : dependencyArgs;
        List<Object> argsList = new ArrayList<>(list(args));
        argsList.add(0, data);
        Tuple argsTuple = TupleFactory.getInstance().newTuple(argsList);

        try {
            DependencyFunc dependency = dependency();
            dependency.setDataInputSchema(getInputSchema());
            setDataInputSchema(dependency.outputSchema(getInputSchema()));
            return dependency.exec(argsTuple);
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Tuple enrichData(Schema dataSchema, Tuple data, Tuple enrichment) throws FrontendException, ExecException {
        if (applyDependency) return super.enrichData(dataSchema, data, enrichment);
        return super.enrichData(getInputSchema().getField(0).schema, originalData, enrichment);
    }

    @Override
    public Schema outputSchema(Schema input) {
        if (applyDependency) return super.outputSchema(dependency().outputSchema(input));
        return super.outputSchema(input);
    }

    @Override
    public <T> T get(Tuple tuple, String field) {
        String dependencyField = dependency().getResultingFieldName(field);
        return super.get(tuple, dependencyField == null ? field : dependencyField);
    }

    @Override
    public String getEnrichNode() {
        return dependency().extensionNode();
    }
}
