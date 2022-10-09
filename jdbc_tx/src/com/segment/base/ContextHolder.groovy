package com.segment.base

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.context.support.GenericGroovyApplicationContext

@CompileStatic
@Singleton
@Slf4j
class ContextHolder {
    GenericGroovyApplicationContext context

    void init(ClassLoader classLoader, String resources) {
        if (context) {
            return
        }

        context = new GenericGroovyApplicationContext()
        context.classLoader = classLoader == null ? this.class.classLoader : classLoader
        log.info('groovy application context loading...')
        context.load(resources)
        context.refresh()
        log.info('groovy application context loading done')
    }

    void close() {
        if (context) {
            log.info('groovy application context closing...')
            context.close()
            log.info('groovy application context closing done')
        }
    }

    Object getBean(String name) {
        if (!context) {
            return null
        }
        context.getBean(name)
    }
}
