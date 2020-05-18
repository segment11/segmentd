package org.segment.d.json

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.CompileStatic

@CompileStatic
class DefaultJsonTransformer implements JsonTransformer {
    @Override
    String json(Object obj) {
        new ObjectMapper().writeValueAsString(obj)
    }

    @Override
    <T> T read(String string, Class<T> clz) {
        new ObjectMapper().readValue(string, clz)
    }
}
