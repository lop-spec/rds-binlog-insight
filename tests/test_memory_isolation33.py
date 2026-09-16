import unittest
from tools.memory_isolation33 import CAP,EPOCH,STEP,ROWS,WIDE_ROWS,WIDTH,cases,normalized,summarize

class MemoryIsolationContracts(unittest.TestCase):
    def test_frozen_window_and_explicit_cap(self):
        self.assertEqual(CAP,500000000)
        self.assertLessEqual(EPOCH+(ROWS-1)*STEP,EPOCH+2592000000000)
    def test_independent_pages_have_no_overlap(self):
        c=cases()
        for first,second in [(c[0],c[1]),(c[2],c[3])]:
            self.assertEqual(len(first['expected']),50)
            self.assertEqual(len(second['expected']),50)
            self.assertFalse({r['id'] for r in first['expected']} & {r['id'] for r in second['expected']})
            self.assertGreater(first['expected'][-1]['id'],second['expected'][0]['id'])
    def test_wide_oracle_is_not_derived_from_clickhouse(self):
        self.assertEqual(cases()[-1]['expected'],[{'n':WIDE_ROWS,'bytes':WIDTH*WIDE_ROWS}])
        self.assertEqual(normalized([{'n':'3','bytes':'24'}]),[{'n':3,'bytes':24}])
    def test_distinguishes_tracking_from_cgroup_categories(self):
        phase={'samples':[{'current':1000,'stat':{'anon':500,'file':100,'sock':10,'kernel':40},'events':{'oom_kill':1},'metrics':[{'metric':'MemoryTracking','value':300}]}],'queries':[]}
        summary=summarize(phase)
        self.assertEqual(summary['medianCgroupMinusTracking'],700)
        self.assertEqual(summary['medianCgroupMinusCategories'],350)
        self.assertEqual(summary['oomEvents'],1)
    def test_missing_cleanup_cannot_count_as_pass(self):
        summary=summarize({'samples':[],'queries':[{'error':'timed out','cleanupRemaining':None,'oracle':False}]})
        self.assertEqual(summary['cleanupFailures'],1)
        self.assertEqual(summary['failedQueries'],1)
        self.assertEqual(summary['oracleFailures'],1)
        self.assertEqual(summary['successfulWithin60'],0)

if __name__=='__main__':unittest.main()
