package org.segment.d.json

import com.fasterxml.jackson.core.type.TypeReference
import groovy.transform.CompileStatic

@CompileStatic
interface JsonTransformer {
    String json(Object obj)

    public <T> T read(String string, Class<T> clz)

    public <T> T read(String string, TypeReference<T> clz)
}