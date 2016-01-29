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
import de.l3s.archivepig.DependentFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static de.l3s.archivepig.Shortcuts.array;
import static de.l3s.archivepig.Shortcuts.list;

public class Pipe extends EvalFunc<Tuple> {
    private List<EvalFunc<Tuple>> funcs = new ArrayList<EvalFunc<Tuple>>();

    public Pipe(String... funcSignatures) throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        EvalFunc<Tuple> prev = null;
        for (String signature : funcSignatures) {
            String[] parts = signature.split("\\(");
            String funcName = parts[0];
            EvalFunc<Tuple> func = null;
            if (parts.length > 1) {
                String parameterList = parts[1];
                String[] parameters = parameterList.substring(0, parameterList.length() - 1).split(","); // without closing )
                for (int i = 0; i < parameters.length; i++) {
                    parameters[i] = parameters[i].trim();
                }
                for(Constructor constructor : Class.forName(funcName).getConstructors()) {
                    if (constructor.getParameterTypes().length == parameters.length) {
                        func = (EvalFunc<Tuple>)constructor.newInstance(parameters);
                        break;
                    }
                }
                if (func == null) throw new RuntimeException("UDF in pipe not found: " + funcName);
            } else {
                func = (EvalFunc<Tuple>)Class.forName(funcName).newInstance();
            }
            funcs.add(func);
            if (prev != null) ((DependentFunc)func).setDependency((DependencyFunc)prev);
            prev = func;
        }
    }

    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        List<Object> args = tuple.getAll();
        args.add(null); // last element in the iteration
        Tuple data = (Tuple)args.remove(0);

        Object execArgs = null;
        if (args.size() > 0) {
            execArgs = args.remove(args.size() - 1);

            if (args.size() > 0) {
                EvalFunc[] parameterizedDependencyFuncs = Arrays.copyOfRange(funcs.toArray(new EvalFunc[0]), funcs.size() - args.size(), funcs.size());
                for (int i = 0; i < args.size(); i++) {
                    Object dependencyArg = args.get(i);
                    Object[] dependencyArgs = Tuple.class.isInstance(dependencyArg) ? array(((Tuple) dependencyArg).getAll()) : array(dependencyArg);
                    ((DependentFunc)parameterizedDependencyFuncs[i]).setDependencyArguments(dependencyArgs);
                }
            }
        }

        // execute last function in pipe
        EvalFunc<Tuple> execFunc = funcs.get(funcs.size() - 1);
        execArgs = execArgs == null ? new Object[0] : execArgs;
        List<Object> argsList = new ArrayList<Object>(list(execArgs));
        argsList.add(0, data);
        Tuple argsTuple = TupleFactory.getInstance().newTuple(argsList);

        execFunc.setInputSchema(getInputSchema());
        return execFunc.exec(argsTuple);
    }

    @Override
    public Schema outputSchema(Schema input) {
        EvalFunc execFunc = funcs.get(funcs.size() - 1);
        execFunc.setInputSchema(input);
        return execFunc.outputSchema(input);
    }
}
