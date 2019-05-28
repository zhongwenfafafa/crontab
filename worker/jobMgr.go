package worker

import (
	"context"
	"github.com/zhongwenfafafa/crontab/common"
	"go.etcd.io/etcd/v3/clientv3"
	"go.etcd.io/etcd/v3/mvcc/mvccpb"
	"time"
)

type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 监听任务的变化
func (jobMgr *JobMgr) watchJobs() (err error) {
	var (
		getResp       *clientv3.GetResponse
		kvpair        *mvccpb.KeyValue
		job           *common.Job
		jobEvent      *common.JobEvent
		watchStartRev int64
	)

	// get一下etcd中/cron/jobs目录下的所有任务，并且获知当前集群中的revision
	if getResp, err = jobMgr.kv.Get(context.TODO(),
		common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}

	for _, kvpair = range getResp.Kvs {
		// 反序列化得到job
		if job, err = common.UnpackJob(kvpair.Value); err != nil {
			continue
		}

		jobEvent = common.BuildJobEvent(common.JOB_EVENT_PUT, job)
		G_scheduler.PushJobEvent(jobEvent)
	}

	watchStartRev = getResp.Header.Revision + 1

	// 从该revision向后监听变化
	go watchRev(watchStartRev)

	return
}

// 监听任务变化
func watchRev(watchStartRev int64) {
	var (
		err        error
		job        *common.Job
		watchResp  clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName    string
		jobEvent   *common.JobEvent
		watchChan  clientv3.WatchChan
	)

	watchChan = G_jobMgr.watcher.Watch(context.TODO(),
		common.JOB_SAVE_DIR,
		clientv3.WithPrefix(),
		clientv3.WithRev(watchStartRev))

	// 遍历监听chan
	for watchResp = range watchChan {
		for _, watchEvent = range watchResp.Events {
			switch watchEvent.Type {
			case mvccpb.PUT:
				// 反解析出发生更新的任务
				if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
					continue
				}

				// 创建一个更新事件
				jobEvent = common.BuildJobEvent(common.JOB_EVENT_PUT, job)
			case mvccpb.DELETE:
				// 获取被删除的任务名
				jobName = common.ExtractJobName(string(watchEvent.Kv.Key))

				// 创建一个删除事件
				job = &common.Job{Name: jobName}

				jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
			}

			// 推送给scheduler
			G_scheduler.PushJobEvent(jobEvent)
		}
	}
}

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
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
	watcher = clientv3.NewWatcher(client)

	// 赋值单例
	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	// 启动任务监听
	return G_jobMgr.watchJobs()
}
