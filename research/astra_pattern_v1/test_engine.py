from datetime import datetime,timedelta,timezone
from zoneinfo import ZoneInfo
import pytest
from engine import *
T=datetime(2025,9,3,15,tzinfo=UTC)
def bars(start,n,contract=None):return [Bar(start+timedelta(minutes=i),100+i*.01,101+i*.01,99+i*.01,100.1+i*.01,10,6,contract) for i in range(n)]
def test_window_reject_early():
 with pytest.raises(ValueError):guard('equity',datetime(2025,8,31,tzinfo=UTC),T)
def test_window_reject_future():
 with pytest.raises(ValueError):guard('equity',T,datetime(2026,9,1,tzinfo=UTC))
def test_crypto_seal():
 with pytest.raises(ValueError):guard('crypto',datetime(2026,8,24,tzinfo=UTC),datetime(2026,8,24,2,tzinfo=UTC))
def test_live_protected_overlap():
 with pytest.raises(ValueError):guard('equity',T,T+timedelta(hours=2),[(T+timedelta(hours=1),T+timedelta(hours=3))])
def test_valid_range():guard('equity',T,T+timedelta(hours=1))
def test_source_interval_start():
 with pytest.raises(ValueError):features(bars(T-timedelta(hours=1),60),T,T)
def test_features_hand_math():
 f=features(bars(T-timedelta(hours=1),60),T,T+timedelta(seconds=30));assert f['r60']==pytest.approx(100.69/100-1);assert f['taker_fraction']==.6
@pytest.mark.parametrize('n',[59,61])
def test_missing_extra(n):
 with pytest.raises(ValueError):features(bars(T-timedelta(hours=1),n),T,T+timedelta(seconds=30))
def test_duplicate():
 a=bars(T-timedelta(hours=1),60);a[-1]=a[-2]
 with pytest.raises(ValueError):features(a,T,T+timedelta(seconds=30))
def test_invalid_ohlc():
 a=bars(T-timedelta(hours=1),60);a[0]=Bar(a[0].ts,100,90,80,101)
 with pytest.raises(ValueError):features(a,T,T+timedelta(seconds=30))
def test_adjustment_scale_invariance():
 a=bars(T-timedelta(hours=1),60);b=[Bar(x.ts,x.open/2,x.high/2,x.low/2,x.close/2,x.volume*2,x.taker*2) for x in a];assert features(a,T,T+timedelta(seconds=30))['r60']==pytest.approx(features(b,T,T+timedelta(seconds=30))['r60'])
def test_roll_crossing():
 a=bars(T-timedelta(hours=1),60,'ESU5');a[-1]=Bar(a[-1].ts,110,111,109,110,contract='ESZ5')
 with pytest.raises(ValueError):features(a,T,T+timedelta(seconds=30))
def test_censor_split():assert label(bars(T,61),T,T+timedelta(hours=1))['status']=='censored'
def test_label_missing_not_flat():assert label(bars(T,60),T,T+timedelta(days=1))['status']=='missing_or_invalid'
def test_entry_exit_open():assert label(bars(T,61),T,T+timedelta(days=1))['gross_bps']==pytest.approx(60)
def test_mae_excludes_exit_bar_range():
 a=bars(T,61);a[-1]=Bar(a[-1].ts,100.6,200,1,100.6);assert label(a,T,T+timedelta(days=1))['mae_bps']==pytest.approx(-100)
def test_cost_arithmetic():assert [net_bps(25,10,s) for s in (1,1.5,2)]==[15,10,5]
def test_nonoverlap():assert_nonoverlap([(T,T+timedelta(hours=1)),(T+timedelta(hours=2),T+timedelta(hours=3))])
def test_overlapping_labels():
 with pytest.raises(ValueError):assert_nonoverlap([(T,T+timedelta(hours=2)),(T+timedelta(hours=1),T+timedelta(hours=3))])
def test_portfolio_no_independent_capital():assert portfolio([(T,T+timedelta(hours=1),1,100)]*2,2)[0][1]==100
@pytest.mark.parametrize('month,offset',[(10,15),(11,16)])
def test_dst(month,offset):assert datetime(2025,month,3,11,tzinfo=ZoneInfo('America/New_York')).astimezone(UTC).hour==offset
def test_ambiguous_touch_stop_first():assert conservative_touch(Bar(T,100,105,95,100),104,96)=='stop'
def test_deterministic():assert block_stats([1,2,-1,3]*10)==block_stats([1,2,-1,3]*10)
def test_holm():assert holm([.001,.04,.2])==pytest.approx([.003,.08,.2])
def test_no_trade_denominator():assert signal('A1',{'r60':.001,'relative_r60':0}) is False

@pytest.mark.parametrize('value',[None,float('nan'),float('inf')])
def test_nonfinite_missing_bar(value):
 assert not valid_bar(Bar(T,value,102,99,101,1))
def test_open_ended_crypto_seal():
 with pytest.raises(ValueError):guard('crypto',datetime(2026,8,15,tzinfo=UTC),datetime(2026,8,15,2,tzinfo=UTC))
def test_feature_signal_interface():
 f=features(bars(T-timedelta(hours=1),60),T,T+timedelta(seconds=30))
 assert 'return_last20' in f
 assert signal('F1',f) is True
def test_short_ratio_bootstrap_no_false_precision():
 d=[['2026-08-13',44,5,1,4],['2026-08-14',44,6,-1,3]]
 r=block_ratio_stats(d,4,1)
 assert r['ci95'] is None and r['p']==1
def test_ratio_bootstrap_deterministic():
 d=[[str(i),32 if i%7 else 16,float((i%5)-2),float((i%3)-1),4] for i in range(41)]
 assert block_ratio_stats(d,16,9)==block_ratio_stats(d,16,9)
