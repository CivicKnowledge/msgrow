import unittest


class TestZipfile(unittest.TestCase):

    def test_write_multiple(self):

        from zipfile import ZipFile

        zf = ZipFile('/tmp/foofile.zip','w')

        for i in range(10):
            zf.writestr('quick.txt', 'Quick '*10+'\n')
            zf.writestr('brown.txt', 'Brown ' * 10 + '\n')
            zf.writestr('fox.txt',   'Fox ' * 10 + '\n')


        zf.close()


if __name__ == '__main__':
    unittest.main()
