package org.segment.d

import com.esotericsoftware.reflectasm.MethodAccess
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.lang.reflect.Field
import java.util.concurrent.ConcurrentHashMap

@CompileStatic
@Slf4j
class BeanReflector {

    private MethodAccess ma

    private int maIndex = -1

    private BeanReflector(MethodAccess ma, int maIndex) {
        this.ma = ma
        this.maIndex = maIndex
    }

    Class getReturnType() {
        Class[] rt = ma.returnTypes
        rt.length > maIndex ? rt[maIndex] : null
    }

    Object invoke(Object bean, Object... args) {
        if (!ma || maIndex == -1) {
            return null
        }
        ma.invoke(bean, maIndex, args)
    }

    private static ConcurrentHashMap<String, MethodAccess> cachedMa = new ConcurrentHashMap<>()
    private static ConcurrentHashMap<String, Integer> cachedMaIndex = new ConcurrentHashMap<>()


    static MethodAccess getMaCached(Class clz) {
        String key = clz.getName()
        def ma = cachedMa[key]
        if (ma != null) {
            return ma
        }

        def ma2 = MethodAccess.get(clz)
        def old = cachedMa.putIfAbsent(key, ma2)
        if (old) {
            return old
        } else {
            return ma2
        }
    }

    static BeanReflector get(Class clz, String methodName, Class... paramTypes) {
        String key = clz.getName() + '.' + methodName + '(' + (paramTypes ? paramTypes.collect { ((Class) it).name }.join(',') : '') + ')'
        def maIndexCached = cachedMaIndex[key]
        if (maIndexCached != null && maIndexCached.intValue() == -1) {
            return null
        }

        def ma = getMaCached(clz)
        if (maIndexCached != null && maIndexCached.intValue() != -1) {
            return new BeanReflector(ma, maIndexCached)
        }

        def maIndex = paramTypes == null ? ma.getIndex(methodName) : ma.getIndex(methodName, paramTypes)
        def old = cachedMaIndex.putIfAbsent(key, maIndex)
        return new BeanReflector(ma, old != null ? old : maIndex)
    }

    private static Set<String> skipGroovyObjectFields = new HashSet<>()
    static {
        skipGroovyObjectFields << '$callSiteArray'
        skipGroovyObjectFields << '$staticClassInfo'
        skipGroovyObjectFields << '__$stMC'
        skipGroovyObjectFields << 'metaClass'
    }

    private static ConcurrentHashMap<String, List<Field>> fieldsByClassName = new ConcurrentHashMap<>()

    static List<Field> getClassFields(Class clz, Class untilUpperClz = null) {
        String key = clz.getName() + (untilUpperClz ? '_' + untilUpperClz.getName() : '')
        def list = fieldsByClassName[key]
        if (list != null) {
            return list
        }

        List<Field> r = []

        Class tmp = clz
        while (tmp != null) {
            def fields = tmp.getDeclaredFields()
            for (field in fields) {
                if (field.name in skipGroovyObjectFields) {
                    continue
                }
                r << field
            }
            tmp = tmp.superclass
            if (untilUpperClz != null && tmp == untilUpperClz) {
                break
            }
        }
        fieldsByClassName[key] = r
        r
    }
}
