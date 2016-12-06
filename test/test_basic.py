import unittest

from rowpack import RowpackReader, RowpackWriter, Schema


class TestBasic(unittest.TestCase):

    def test_basic(self):

        s = Schema()

        for i in range(10):
            s.add_column(name='col' + str(i), datatype=int)

        with RowpackWriter('/tmp/foo.rp', 'wb') as rpw:
            for i in range(10):
                row = range(10)
                rpw.write_row(row)

            rpw.schema = s
            rpw.meta = {
                'foo': 'bar'
            }

        with RowpackReader('/tmp/foo.rp') as rpr:
            self.assertEquals(42, rpr.data_start)
            self.assertEquals(83, rpr.data_end)
            self.assertEquals(1676, rpr.meta_end)
            self.assertEquals({u'foo': u'bar'}, rpr.meta)
            self.assertEquals(
                [u'col0', u'col1', u'col2', u'col3', u'col4', u'col5', u'col6', u'col7', u'col8', u'col9'],
                rpr.headers)

            rows = [dict(zip(rpr.schema.headers, row)) for row in rpr]

            self.assertEqual(10, len(rows))
            self.assertEqual(range(10), sorted(rows[0].values()))

        with RowpackWriter('/tmp/foo.rp', 'r+b') as rpw:
            rpw.meta['bingo'] = 'baz'

        with RowpackReader('/tmp/foo.rp') as rpr:
            self.assertEquals({u'bingo': u'baz', u'foo': u'bar'}, rpr.meta)
            self.assertEquals(
                [u'col0', u'col1', u'col2', u'col3', u'col4', u'col5', u'col6', u'col7', u'col8', u'col9'],
                rpr.headers)

    def test_schema(self):

        s = Schema()
        for i in range(10):
            s.add_column(name=i, description=i, datatype=int)

        s2 = Schema.from_rows(s.to_rows())

        for c in s2:
            print c.pos, c.name, c.datatype

    def make_simple_rw_data(self, n=None):
        import datetime
        from random import randint, random
        from uuid import uuid4

        N = n if n is not None else 50000

        # Basic read/ write tests.

        row = lambda n: (n, randint(0, 100), str(uuid4()), str(random()), float(n ** 2))

        headers = 'id rand uuid randstr float'.split()

        rows = [row(i) for i in range(N)]

        return N, headers, rows, [type(e) for e in row(i)]

    def schemas(self):

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

        return avro_schema, rp_schema

    def test_stats(self):

        N, headers, rows, types = self.make_simple_rw_data(1000)

        avro_schema, rp_schema = self.schemas();

        rpw = RowpackWriter('/tmp/foo.rp', schema=rp_schema)

        for row in rows:
            rpw.write_row(row)

        rpw.stats()

        rpw.close()

        with RowpackReader('/tmp/foo.rp') as rpr:
            sum = 0
            count = 0
            for row in rpr:
                sum += row[0]
                count += 1

            self.assertEqual(1000, rpr.schema[0].count)
            self.assertEqual(499.0, rpr.schema[0].median)
            self.assertEqual('rand', rpr.schema[1].name)
            self.assertEqual(1000, rpr.schema[2].nuniques)

    def test_rowintuit(self):
        from rowpack import intuit_rows
        from rowgenerators import RowGenerator
        from itertools import islice

        rg = RowGenerator(url='http://public.source.civicknowledge.com/example.com/sources/renter_cost.csv')

        path = '/tmp/foo.rp'

        with RowpackWriter(path) as rpw:
            for row in rg:
                rpw.write_row(row)

        intuit_rows(path)

    def test_typeintuit(self):
        from rowpack import intuit_types, intuit_rows, run_stats
        from rowgenerators import RowGenerator

        path = '/tmp/foo.rp'

        if False:
            rg = RowGenerator(url='http://public.source.civicknowledge.com/example.com/sources/renter_cost.csv')

            with RowpackWriter(path) as rpw:
                for row in rg:
                    rpw.write_row(row)

        intuit_rows(path)

        intuit_types(path)

        run_stats(path)


    def test_speed(self):
        from contexttimer import Timer
        import csv

        N, headers, rows, types = self.make_simple_rw_data(n=200000)

        avro_schema, rp_schema = self.schemas();

        with Timer() as t:

            rpw = RowpackWriter('/tmp/foo.rp')

            for row in rows:
                rpw.write_row(row)

            rpw.close()

        print('Write RP               ', float(N) / t.elapsed)

        with Timer() as t:

            with RowpackReader('/tmp/foo.rp') as rpr:
                sum = 0
                count = 0
                for row in rpr:
                    sum += row[0]
                    count += 1

        print('Read RP                ', float(N) / t.elapsed)

        self.assertEquals(N, count)
        if N == 50000:
            self.assertEquals(1249975000, sum)

        with Timer() as t:

            with RowpackWriter('/tmp/foo_rows.rp') as rpw:
                rpw.write_rows(rows)

        print('Write RP rows          ', float(N) / t.elapsed)

        with Timer() as t:

            rpr = RowpackReader('/tmp/foo.rp')

            sum = 0
            count = 0
            for row in rpr:
                sum += row[0]
                count += 1

            rpw.close()

        print('Read RP rows           ', float(N) / t.elapsed)

        self.assertEquals(N, count)
        if N == 50000:
            self.assertEquals(1249975000, sum)


        with Timer() as t:
            with open('/tmp/foo.csv','w') as f:
                w = csv.writer(f)

                for row in rows:
                    w.writerow(row)

        print('Write CSV              ', float(N) / t.elapsed)

        with Timer() as t:
            with open('/tmp/foo.csv') as f:
                r = csv.reader(f)

                sum = 0
                count = 0
                for row in r:

                    row = [int(row[0]), int(row[1]), row[2], row[3], float(row[4]) ]
                    sum += row[0]
                    count += 1

        print('Read CSV               ', float(N) / t.elapsed)

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

            with RowpackWriter('/tmp/foo.rp') as rpw:

                for row in rows:
                    rpw.write_row(row)


        print('Write RP               ', float(N) / t.elapsed)

        with Timer() as t:

            rpw = RowpackWriter('/tmp/foo_rows.rp')

            rpw.write_rows(rows)

            rpw.close()

        print('Write RP rows          ', float(N) / t.elapsed)

        with Timer() as t:

            rpr = RowpackReader('/tmp/foo.rp')

            sum = 0
            count = 0
            for row in rpr:
                sum += row[0]
                count += 1

            rpw.close()

        print('Read RP                ', float(N) / t.elapsed)
        print count, sum

        # This runs about twice as fast under pypy
        with Timer() as t:
            with open('/tmp/avro_records.avro', 'wb') as out:
                avr = fastavro.Writer(out, avro_schema)
                for row in rows:
                    d = dict(zip(headers, row))
                    avr.write(d)

        print('Write AVRO records     ', float(N) / t.elapsed)

        with Timer() as t:
            with open('/tmp/avro_records.avro', 'rb') as fo:
                avr = fastavro.reader(fo)
                sum = 0
                count = 0
                for row in avr:
                    sum += row['id']
                    count += 1

        print('Read AVRO records      ', float(N) / t.elapsed)
        print count, sum

        avro_schema = {
            "type": "array",
            "name": "Test",
            "items": [e['type'] for e in avro_schema['fields']]
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
                count = 0
                for row in avr:
                    sum += row[0]
                    count += 1

        print('Read AVRO array        ', float(N) / t.elapsed)
        print count, sum


if __name__ == '__main__':
    unittest.main()
