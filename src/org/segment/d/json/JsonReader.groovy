package org.segment.d.json

import groovy.transform.CompileStatic

@CompileStatic
@Singleton
class JsonReader {
    JsonTransformer jsonTransformer = new DefaultJsonTransformer()

    public <T> T read(String string, Class<T> clz) {
        jsonTransformer.read(string, clz)
    }
}
