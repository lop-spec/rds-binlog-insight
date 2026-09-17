import unittest
from tools.recovery_fixture33.archive_probe import verify_rows

class ArchiveOracleTests(unittest.TestCase):
    def setUp(self):self.rows=[{'instance_id':'rm-test000001','event_id':str(i),'event_epoch_us':123,'sql_text':f'INSERT {i}'} for i in range(137)]
    def test_complete_content_and_identity(self):
        self.assertEqual(len(verify_rows(list(reversed(self.rows)),self.rows)),137)
    def test_duplicate_missing_changed_identity_and_content_rejected(self):
        cases=[self.rows+self.rows[:1],self.rows[:-1],[dict(r,event_id='changed') if i==0 else r for i,r in enumerate(self.rows)],[dict(r,sql_text='wrong') if i==0 else r for i,r in enumerate(self.rows)]]
        for rows in cases:
            with self.subTest(length=len(rows)),self.assertRaises(AssertionError):verify_rows(rows,self.rows)
