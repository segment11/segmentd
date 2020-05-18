package org.segment.d.json

import groovy.transform.CompileStatic

@CompileStatic
@Singleton
class JsonWriter {
    JsonTransformer jsonTransformer = new DefaultJsonTransformer()

    String json(Object obj) {
        jsonTransformer.json(obj)
    }
}
