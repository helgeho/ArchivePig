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

import datafu.pig.util.ContextualEvalFunc;

public abstract class SimplifiedContextualEvalFunc<T> extends ContextualEvalFunc<T> {
    protected <T> T _init(ContextProperty<T> property) {
        return null;
    }

    protected <T> T _(ContextProperty<T> property) {
        return _get(property);
    }

    protected <T> void _(ContextProperty<T> property, T value) {
        _set(property, value);
    }

    protected <T> T _get(ContextProperty<T> property) {
        if (!getInstanceProperties().containsKey(property.name)) _(property, _init(property));
        return (T)getInstanceProperties().get(property.name);
    }

    protected <T> void _set(ContextProperty<T> property, T value) {
        getInstanceProperties().put(property.name, value);
    }
}
