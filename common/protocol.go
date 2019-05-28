package common

import (
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

// 定时任务
type Job struct {
	Name     string `json:"name"`     // 任务名
	Command  string `json:"command"`  // shell命令
	CronExpr string `json:"cronExpr"` // cron表达式
}

// 任务调度计划
type JobSchedulePlan struct {
	Job      *Job                 // 任务
	Expr     *cronexpr.Expression // 解析好的cronexpr表达式
	NextTime time.Time            // 下次调度事件
}

// 任务执行状态
type JobExecuteInfo struct {
	Job      *Job      // 任务信息
	PlanTime time.Time // 理论上的调度时间
	RealTime time.Time // 实际调度时间
}

// 任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo // 执行状态
	Output      []byte          // 脚本输出
	Err         error           // 脚本错误原因
	StartTime   time.Time       // 启动时间
	EndTime     time.Time       // 结束时间
}

type DelJob struct {
	Name string `json:"name"` // 任务名
}

type ListJobsReqArg struct {
	LsatKeyName string `json:"lsatKeyName"`
	PageSize    int64  `json:"pageSize"`
}

type KillJob struct {
	JobName string `json:"jobName"` // 任务名
}

// JobList接口返回响应体
type ListJobs struct {
	Jobs   []Job
	EndKey string
}

type JobEvent struct {
	EventType int
	Job       *Job
}

// HTTP接口应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

// 应答方法
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	// 定义一个response
	var (
		response Response
	)

	response = Response{
		Errno: errno,
		Msg:   msg,
		Data:  data,
	}

	// 序列化
	resp, err = json.Marshal(response)

	return
}

// 反序列化Job
func UnpackJob(value []byte) (ret *Job, err error) {
	var (
		job *Job
	)

	job = &Job{}

	if err = json.Unmarshal(value, job); err != nil {
		return
	}

	ret = job

	return
}

// 从etcd的key中提取任务名
func ExtractJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

// 任务变化事件有两种 删除和更新
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

// 构造执行计划
func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var (
		expr *cronexpr.Expression
	)
	// 解析表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}

	// 生成下一次调度计划对象
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}

	return
}

func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job:      jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime,
		RealTime: time.Now(), //真实调度时间
	}

	return
}
