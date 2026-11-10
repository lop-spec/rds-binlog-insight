// Package redissample provides optional client-side evidence, not Redis CPU time.
package redissample

import (
 "bytes"
 "context"
 "crypto/sha1"
 "crypto/sha256"
 "encoding/hex"
 "encoding/json"
 "math/rand"
 "math"
 "regexp"
 "net/http"
 "net/url"
 "strconv"
 "strings"
 "sync/atomic"
 "time"

 "github.com/go-redis/redis/v8"
)

type Sample struct {
 ID string `json:"id"`
 Time float64 `json:"time"`
 Command string `json:"command"`
 Keys []string `json:"keys"`
 Script string `json:"script"`
 Category string `json:"category"`
 Pod string `json:"pod"`
 Rate float64 `json:"sample_rate"`
 ClientUs int64 `json:"client_us"`
 Failed bool `json:"failed"`
}
type sampleKey struct{ hook *Hook }
type categoryKey struct{}
// WithCategory is for the two known outbox operations, not inferred SQL meaning.
func WithCategory(ctx context.Context,category string)context.Context {
 if category!="claim_due" && category!="recover_timeout"{category="unclassified_lua"}
 return context.WithValue(ctx,categoryKey{},category)
}
var outboxKey=regexp.MustCompile(`^(pre:)?follow_order:outbox:(zset|hash|ver):\{fo:([0-9]|[1-5][0-9]|6[0-3])\}$`)
type started struct{ at time.Time; sample Sample }
type Hook struct {
 instance, endpoint, token, pod string
 rate float64
 queue chan Sample
 dropped atomic.Uint64
 sequence atomic.Uint64
 prefix string
}
// New requires an already-authorized TLS endpoint, never a public plaintext token.
// Run must be called in a goroutine under the application's shutdown context.
func New(instance, endpoint, token, pod string, rate float64) (*Hook,error) {
 u,err:=url.Parse(endpoint)
 if err!=nil || u.Scheme!="https" || u.User!=nil || u.Host=="" || u.Path!="/api/ingest/redis-samples" || u.RawQuery!="" || len(token)<32 || math.IsNaN(rate) || rate<=0 || rate>0.01 {
  return nil, &configError{}
 }
 prefix:=sha256.Sum256([]byte(pod+time.Now().String()))
 return &Hook{instance:instance,endpoint:endpoint,token:token,pod:pod,rate:rate,queue:make(chan Sample,512),prefix:hex.EncodeToString(prefix[:8])},nil
}
type configError struct{}
func (*configError) Error() string{return "invalid Redis sample configuration"}
func (h *Hook) Dropped() uint64{return h.dropped.Load()}
func (h *Hook) begin(ctx context.Context,cmds []redis.Cmder,pipeline bool) context.Context {
 if rand.Float64()>=h.rate || len(cmds)==0{return ctx}
 s:=Sample{ID:h.prefix+"_"+strconv.FormatUint(h.sequence.Add(1),10),Pod:h.pod,Rate:h.rate,Keys:[]string{}}
 if pipeline{s.Command="pipeline"}else{s.Command=strings.ToLower(cmds[0].Name())}
 for _,cmd:=range cmds {
  args:=cmd.Args();name:=strings.ToLower(cmd.Name())
  if name=="eval" || name=="evalsha" {
   if len(args)<3{continue};n,_:=strconv.Atoi(str(args[2]));if n<0 || n>len(args)-3{continue}
   script:=str(args[1]);if name=="eval"{sum:=sha1.Sum([]byte(script));s.Script=hex.EncodeToString(sum[:])}else if len(script)==40{s.Script=script}
   for _,a:=range args[3:3+n]{if len(s.Keys)<16{s.Keys=append(s.Keys,keyShape(str(a)))}}
   // Business identity must be supplied by a wrapper; never guess from EVAL.
   s.Category="unclassified_lua";if category,ok:=ctx.Value(categoryKey{}).(string);ok{s.Category=category}
  }else if len(args)>1 {
   switch name {
   case "del","unlink","mget":
    for _,a:=range args[1:]{if len(s.Keys)<16{s.Keys=append(s.Keys,keyShape(str(a)))}}
   case "get","set","exists","zrangebyscore","zadd","zrem","hget","hset","expire","pexpire":
    if len(s.Keys)<16{s.Keys=append(s.Keys,keyShape(str(args[1])))}
   }
  }
 }
 now:=time.Now();s.Time=float64(now.UnixNano())/1e9
 return context.WithValue(ctx,sampleKey{h},started{now,s})
}
func str(v interface{})string{switch x:=v.(type){case string:return x;case []byte:return string(x);case int:return strconv.Itoa(x);case int64:return strconv.FormatInt(x,10);default:return ""}}
func keyShape(k string)string {
 if outboxKey.MatchString(k) {
  a:=strings.Index(k,"{fo:");if a>=0 && strings.HasSuffix(k,"}"){if n,e:=strconv.Atoi(k[a+4:len(k)-1]);e==nil && n>=0 && n<64{return k}}
 }
 p:=strings.Split(k,":");if len(p)>3{p=p[:3]}
 for i,s:=range p{if len(s)==0 || len(s)>40{p[i]="*";continue};for _,r:=range s{if (r<'a'||r>'z') && r!='_'{p[i]="*";break}}}
 sum:=sha256.Sum256([]byte(k));return strings.Join(p,":")+":#"+hex.EncodeToString(sum[:6])
}
func(h *Hook) finish(ctx context.Context,cmds []redis.Cmder){
 v,ok:=ctx.Value(sampleKey{h}).(started);if !ok{return};s:=v.sample;s.ClientUs=time.Since(v.at).Microseconds()
 for _,cmd:=range cmds{if cmd.Err()!=nil && cmd.Err()!=redis.Nil{s.Failed=true}}
 select{case h.queue<-s:default:h.dropped.Add(1)}
}
func(h *Hook)BeforeProcess(ctx context.Context,cmd redis.Cmder)(context.Context,error){return h.begin(ctx,[]redis.Cmder{cmd},false),nil}
func(h *Hook)AfterProcess(ctx context.Context,cmd redis.Cmder)error{h.finish(ctx,[]redis.Cmder{cmd});return nil}
func(h *Hook)BeforeProcessPipeline(ctx context.Context,cmds []redis.Cmder)(context.Context,error){return h.begin(ctx,cmds,true),nil}
func(h *Hook)AfterProcessPipeline(ctx context.Context,cmds []redis.Cmder)error{h.finish(ctx,cmds);return nil}
// Run sends at most 100 samples per second. Failed batches are dropped and counted,
// never retried on a business request. No command/value reconstruction is attempted.
func(h *Hook)Run(ctx context.Context){
 client:=&http.Client{Timeout:2*time.Second,CheckRedirect:func(*http.Request,[]*http.Request)error{return http.ErrUseLastResponse}}
 ticker:=time.NewTicker(time.Second);defer ticker.Stop()
 for{select{case <-ctx.Done():return;case <-ticker.C:
  batch:=make([]Sample,0,100)
 drain:for len(batch)<100{select{case s:=<-h.queue:batch=append(batch,s);default:break drain}}
  if len(batch)==0{continue}
  body,err:=json.Marshal(struct{Instance string `json:"instance"`;Samples []Sample `json:"samples"`}{h.instance,batch});if err!=nil{h.dropped.Add(uint64(len(batch)));continue}
  req,err:=http.NewRequestWithContext(ctx,"POST",h.endpoint,bytes.NewReader(body));if err!=nil{h.dropped.Add(uint64(len(batch)));continue}
  req.Header.Set("Authorization","Bearer "+h.token);req.Header.Set("Content-Type","application/json")
  response,err:=client.Do(req);if err!=nil{h.dropped.Add(uint64(len(batch)));continue};response.Body.Close();if response.StatusCode!=202{h.dropped.Add(uint64(len(batch)))}
 }}
}
