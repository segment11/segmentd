package org.segment.d.json

import groovy.transform.CompileStatic

@CompileStatic
interface JsonTransformer {
    String json(Object obj)

    public <T> T read(String string, Class<T> clz)
}