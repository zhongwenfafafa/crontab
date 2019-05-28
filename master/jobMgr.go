package master

import (
	"context"
	"encoding/json"
	"github.com/zhongwenfafafa/crontab/common"
	"go.etcd.io/etcd/v3/clientv3"
	"go.etcd.io/etcd/v3/mvcc/mvccpb"
	"time"
)

type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)

	// 初始化etcd配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,                                     // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 得到kv和lease的api子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}

// 保存任务
func (jobMgr *JobMgr) SaveJob(job common.Job) (oldJob *common.Job, err error) {
	// 把任务保存在/cron/jobs/任务名 -> json
	var (
		jobKey    string
		jobValue  []byte
		putResp   *clientv3.PutResponse
		oldJobObj common.Job
	)
	// etcd的保存健
	jobKey = common.JOB_SAVE_DIR + job.Name
	// 任务信息json
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}

	// 保存早etcd
	if putResp, err = jobMgr.kv.Put(context.TODO(),
		jobKey, string(jobValue),
		clientv3.WithPrevKV()); err != nil {
		return
	}

	// 如果是更新，那么返回旧值
	if putResp.PrevKv != nil {
		// 对旧值反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}

		oldJob = &oldJobObj
	}

	return
}

// 删除任务
func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey    string
		oldJobObj common.Job
		delResp   *clientv3.DeleteResponse
	)

	// 删除任务key
	jobKey = common.JOB_SAVE_DIR + name

	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	// 返回被删除的信息
	if len(delResp.PrevKvs) != 0 {
		// 解析返回的旧值
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return
}

// 任务列表
func (jobMgr *JobMgr) ListJobs(keyStart string, pageSize int64) (listJobs *common.ListJobs, err error) {
	var (
		dirKey   string
		getResp  *clientv3.GetResponse
		keyValue *mvccpb.KeyValue
		jobs     []common.Job
		job      common.Job
		endKey   string
	)

	dirKey = common.JOB_SAVE_DIR

	if getResp, err = jobMgr.kv.Get(context.TODO(), keyStart+"\x00",
		clientv3.WithLimit(pageSize),
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(dirKey))); err != nil {
		return
	}

	if len(getResp.Kvs) != 0 {
		for _, keyValue = range getResp.Kvs {
			if err = json.Unmarshal(keyValue.Value, &job); err != nil {
				continue
			}

			endKey = string(keyValue.Key)
			jobs = append(jobs, job)
		}

		listJobs = &common.ListJobs{
			Jobs:   jobs,
			EndKey: endKey,
		}
	}

	return
}

// 杀死任务接口
func (jobMgr *JobMgr) KillJob(name string) (err error) {
	var (
		killKey        string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
	)

	// 通知worker杀死队列任务
	killKey = common.JOB_KILLER_DIR + name

	//让worker监听一次put操作，创建一个租约自动过期即可
	if leaseGrantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	leaseId = leaseGrantResp.ID

	// 设置killer标记
	if _, err = jobMgr.kv.Put(context.TODO(),
		killKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}

	return
}
