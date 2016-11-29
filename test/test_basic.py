import unittest

from rowpack import RowpackReader, RowpackWriter, Schema

class TestBasic(unittest.TestCase):

    def test_basic(self):

        s = Schema()

        for i in range(10):
            s.add_column(name='col'+str(i), datatype=int)

        rpw = RowpackWriter('/tmp/foo.rp', s)

        for i in range(10):

            row = range(10)

            rpw.write_row(row)

        rpw.close()

        rpr = RowpackReader('/tmp/foo.rp')
        print "Start: ", rpr.data_start
        print "End: ", rpr.data_end
        print "---- Columns "
        print [ str(c) for c in rpr.schema]
        print "---- Rows "

        headers = rpr.schema.headers

        for row in rpr:
            print dict(zip(headers, row))

    def test_schema(self):

        s = Schema()
        for i in range(10):
            s.add_column(name=i, description=i, datatype=int)

        s2 = Schema.from_rows(s.to_rows())

        for c in s2:
            print c.pos, c.name, c.datatype

    def make_simple_rw_data(self):
        import datetime
        from random import randint, random
        from uuid import uuid4

        N = 50000

        # Basic read/ write tests.

        row = lambda n: (n, randint(0, 100), str(uuid4()), str(random()), float(n ** 2))

        headers = 'id rand uuid randstr float'.split()

        rows = [row(i) for i in range(N)]

        return N, headers, rows, [type(e) for e in row(i)]

    def test_avro_read_write(self):
        import fastavro
        from contexttimer import Timer

        N, headers, rows, types = self.make_simple_rw_data()

        avro_schema = {
            "type": "record",
            "name": "Test",
            "fields": [
                {"type": "int", "name": "id"},
                {"type": "int", "name": "rand"},
                {"type": "string", "name": "uuid"},
                {"type": "string", "name": "randstr"},
                {"type": "float", "name": "float"},
            ]
        }

        rp_schema = Schema()
        for e in avro_schema['fields']:
            rp_schema.add_column(name=e['name'], datatype=e['type'])


        with Timer() as t:

            rpw = RowpackWriter('/tmp/foo.rp', rp_schema)

            for row in rows:
                rpw.write_row(row)

            rpw.close()

        print('Write RP               ', float(N) / t.elapsed)

        with Timer() as t:

            rpw = RowpackWriter('/tmp/foo_rows.rp', rp_schema)

            rpw.write_rows(rows)

            rpw.close()

        print('Write RP rows          ', float(N) / t.elapsed)

        with Timer() as t:

            rpr = RowpackReader('/tmp/foo.rp')

            sum = 0
            for row in rpr:
                sum += row[0]

            rpw.close()

        print('Read RP                ', float(N) / t.elapsed)

        # This runs about twice as fast under pypy
        with Timer() as t:
            with open('/tmp/avro_records.avro', 'wb') as out:
                avr = fastavro.Writer(out, avro_schema)
                for row in rows:
                    d = dict(zip(headers, row))
                    avr.write(d)

        print('Write AVRO records     ', float(N) / t.elapsed)

        with Timer() as t:
            with open('/tmp/avro_records.avro','rb') as fo:
                avr = fastavro.reader(fo)
                sum = 0
                for row in avr:
                    sum += row['rand']


        print('Read AVRO records      ', float(N) / t.elapsed)

        avro_schema = {
            "type": "array",
            "name": "Test",
            "items": [ e['type'] for e in avro_schema['fields']]
        }


        # Oddly, AVRO records are smaller and faster than arrays.
        with Timer() as t:
            with open('/tmp/avro_array2.avro', 'wb') as out:
                avr = fastavro.Writer(out, avro_schema)
                for row in rows:
                    avr.write(row)

        print('Write AVRO array       ', float(N) / t.elapsed)

        with Timer() as t:
            with open('/tmp/avro_array2.avro', 'rb') as out:
                avr = fastavro.reader(out, avro_schema)
                sum = 0
                for row in avr:
                    sum += row[0]

        print('Read AVRO array        ', float(N) / t.elapsed)


if __name__ == '__main__':
    unittest.main()
