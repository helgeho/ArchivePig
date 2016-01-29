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

package de.l3s.archivepig.utils;

import org.apache.pig.impl.util.UDFContext;

import java.io.Serializable;
import java.util.Properties;

public class ContextualPigObject implements Serializable {
    private Class funcClass;
    private String signature;

    public ContextualPigObject(Object func, String instanceName) {
        this(func.getClass(), instanceName);
    }

    public ContextualPigObject(Class funcClass, String instanceName) {
        this.funcClass = funcClass;
        this.signature = instanceName;
    }

    private Properties getContextProperties() {
        UDFContext context = UDFContext.getUDFContext();
        Properties properties = context.getUDFProperties(funcClass);
        return properties;
    }

    private Properties getInstanceProperties() {
        Properties contextProperties = getContextProperties();
        if (!contextProperties.containsKey(signature)) {
            contextProperties.put(signature, new Properties());
        }
        return (Properties) contextProperties.get(signature);
    }

    /**
     * Can be overridden in an anonymous class block in order to provide initial values for the fields, such as:
     * switch (property.name) {
     *     case "field1": return new Field1();
     *     ...
     * }
     * return super.init(property);
     */
    protected <T> T init(String name) {
        return null;
    }

    public <T> T get(ContextProperty<T> property) {
        if (!getInstanceProperties().containsKey(property.name)) set(property, this.<T>init(property.name));
        return (T)getInstanceProperties().get(property.name);
    }

    public <T> void set(ContextProperty<T> property, T value) {
        getInstanceProperties().put(property.name, value);
    }
}
