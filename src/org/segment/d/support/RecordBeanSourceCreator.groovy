package org.segment.d.support

import groovy.transform.CompileStatic
import org.segment.d.D

import java.sql.ResultSet

@CompileStatic
class RecordBeanSourceCreator {
    @CompileStatic
    enum DialectType {
        MySQL, PG, Oracle
    }

    D d

    String srcDirPath = './src'

    DialectType dialectType = DialectType.MySQL

    private Map<String, Class> rowToBean(String table) {
        Map<String, Class> r = [:]
        String sql = "select * from ${table} limit 1"
        d.db.query(sql) { ResultSet rs ->
            def md = rs.metaData
            for (int i = 1; i <= md.columnCount; i++) {
                def name = md.getColumnName(i)
                def type = md.getColumnType(i)
                r[D.toCamel(name)] = D.getClassTypeBySqlType(type)
            }
        }
        r
    }

    void createRecordClass(String table, String pk, String pkg) {
        String clzName = D.toCamel(table, false) + 'DTO'
        def dir = new File(srcDirPath + '/' + pkg.replaceAll(/\./, '/'))
        dir.mkdirs()

        def f = new File(dir, clzName + '.groovy')
        Map<String, Class> bean = rowToBean(table)
        def fields = bean.collect {
            """
    ${it.value.name.split(/\./)[-1]} ${it.key}
"""
        }.join('')

        def imports = bean.values().collect { it.name }.findAll {
            !it.startsWith('java.lang.') && !it.startsWith('java.util.')
        }.collect {
            "import ${it}"
        }.join("\r\n")

        f.text = """
package ${pkg}

import org.segment.d.D
import org.segment.d.Ds
import org.segment.d.dialect.${dialectType.name()}Dialect
import org.segment.d.Record
import groovy.transform.CompileStatic
${imports.trim()}
@CompileStatic
class ${clzName} extends Record {
    ${fields}
    @Override
    String pk() {
        '${pk}'
    }
    
    @Override
    D useD() {
        new D(Ds.one(), new ${dialectType.name()}Dialect())
    }    
}
""".trim()
        println 'done create class - ' + clzName
    }

}
