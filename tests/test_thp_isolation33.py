import copy,os,unittest
from unittest.mock import patch
from tools.thp_isolation33 import gap,main,reproducer_gate,WRAPPER

class ThpIsolationContracts(unittest.TestCase):
    def phases(self):
        return [{'disabled':d,'retainedOracle':True,'prGetThpDisable':int(d),'anonymousLruGaps':[0 if d else 96*1024**2]*5,'fullyMapped':{'smaps':{'AnonHugePages':128*1024**2}},'samples':[{'events':{'oom_kill':0}}]} for d in [False,True,True,False]]
    def test_missing_or_unstressed_baseline_never_passes(self):
        self.assertFalse(reproducer_gate([]));p=self.phases();p[0]['anonymousLruGaps']=[0];self.assertFalse(reproducer_gate(p))
    def test_same_retained_data_and_bounded_gap_pass(self):
        self.assertTrue(reproducer_gate(self.phases()))
    def test_oom_or_wrong_data_fails(self):
        for key in ['oom','data']:
            p=self.phases()
            if key=='oom':p[0]['samples'][0]['events']['oom_kill']=1
            else:p[2]['retainedOracle']=False
            self.assertFalse(reproducer_gate(p))
    def test_accounts_shmem_without_double_counting(self):
        self.assertEqual(gap({'stat':{'active_anon':100,'inactive_anon':20,'anon':60,'shmem':5,'swapcached':3}}),52)
    def test_scope_guard_precedes_docker(self):
        with patch.dict(os.environ,{'GITHUB_ACTIONS':'false'}),patch('tools.memory_isolation33.run') as run:
            with self.assertRaises(AssertionError):main()
            run.assert_not_called()
    def test_wrapper_is_process_only_and_fail_closed(self):
        self.assertIn('PR_SET_THP_DISABLE',WRAPPER);self.assertIn('PR_GET_THP_DISABLE',WRAPPER)
        self.assertNotIn('/sys/',WRAPPER);self.assertIn('return 78',WRAPPER)

if __name__=='__main__':unittest.main()
