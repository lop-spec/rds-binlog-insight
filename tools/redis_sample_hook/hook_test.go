package redissample

import (
 "context"
 "encoding/json"
 "math"
 "strings"
 "testing"
 "time"
 "github.com/go-redis/redis/v8"
)
func fixture()*Hook{return &Hook{rate:1,queue:make(chan Sample,5),prefix:"fixtureprefix",pod:"fixture-worker"}}
func TestNoValuesOrScriptBody(t *testing.T){
 h:=fixture();ctx:=WithCategory(context.Background(),"claim_due")
 cmd:=redis.NewCmd(ctx,"eval","return 'secret-value'",1,"pre:follow_order:outbox:zset:{fo:25}","secret-arg")
 out,_:=h.BeforeProcess(ctx,cmd);h.AfterProcess(out,cmd);s:=<-h.queue;b,_:=json.Marshal(s)
 if strings.Contains(string(b),"secret") || s.Category!="claim_due" || len(s.Script)!=40{t.Fatal(string(b))}
}
func TestPipelineCostOnce(t *testing.T){
 h:=fixture();ctx:=context.Background();cmds:=[]redis.Cmder{redis.NewCmd(ctx,"get","pre:ws:binding:42"),redis.NewCmd(ctx,"get","pre:ws:binding:43")}
 out,_:=h.BeforeProcessPipeline(ctx,cmds);time.Sleep(time.Millisecond);h.AfterProcessPipeline(out,cmds)
 s:=<-h.queue;if s.Command!="pipeline" || s.ClientUs<1000 || len(h.queue)!=0{t.Fatal(s)}
}
func TestConfigAndPrivacy(t *testing.T){
 for _,rate:=range []float64{0,1,math.NaN()}{if _,err:=New("fixture","https://example.invalid/api/ingest/redis-samples",strings.Repeat("x",40),"worker",rate);err==nil{t.Fatal(rate)}}
 if _,err:=New("fixture","http://example.invalid/api/ingest/redis-samples",strings.Repeat("x",40),"worker",.001);err==nil{t.Fatal("plaintext")}
 for _,k:=range []string{"pre:ws:binding:secret-user","pre:follow_order:outbox:secret{fo:25}"}{if strings.Contains(keyShape(k),"secret"){t.Fatal(keyShape(k))}}
}
func TestQueueNeverBlocks(t *testing.T){
 h:=fixture();ctx:=context.Background();cmd:=redis.NewCmd(ctx,"get","key")
 for i:=0;i<8;i++{out,_:=h.BeforeProcess(ctx,cmd);h.AfterProcess(out,cmd)}
 if h.Dropped()!=3{t.Fatal(h.Dropped())}
}
