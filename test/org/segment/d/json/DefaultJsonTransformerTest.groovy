package org.segment.d.json

import spock.lang.Specification

class DefaultJsonTransformerTest extends Specification {
    def "json"() {
        given:
        def t = new DefaultJsonTransformer()
        expect:
        t.json('1') == '"1"'
    }

    def "read"() {
        given:
        def t = new DefaultJsonTransformer()
        expect:
        t.read('"1"', String) == '1'
    }
}
