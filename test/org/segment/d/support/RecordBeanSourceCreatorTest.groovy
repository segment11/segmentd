package org.segment.d.support

import org.segment.d.D
import org.segment.d.Ds
import org.segment.d.dialect.MySQLDialect
import org.segment.d.json.JSONFiled
import spock.lang.Specification

class RecordBeanSourceCreatorTest extends Specification {
    static class DetailField implements JSONFiled {
        Integer age
        Date birth
    }

    def "create record class"() {
        given:
        def ds = Ds.h2mem('test')
        def d = new D(ds, new MySQLDialect())
        d.exe('''
create table test (
id int auto_increment primary key,
name varchar(50),
detail_field varchar(50)
)
''')
        and:
        def creator = new RecordBeanSourceCreator()
        creator.d = d
        creator.dialectType = RecordBeanSourceCreator.DialectType.MySQL
        creator.srcDirPath = './src'
        creator.createRecordClass('test', 'id', 'model', [detail_field: DetailField])
        and:
        def f = new File('./src/model/TestDTO.groovy')
        expect:
        f.exists()
        f.text.contains('extends Record')
        cleanup:
        ds.closeConnect()
    }
}
