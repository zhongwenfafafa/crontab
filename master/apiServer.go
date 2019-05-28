package master

import (
	"encoding/json"
	"fmt"
	"github.com/zhongwenfafafa/crontab/common"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"time"
)

var (
	// 单例对象
	G_apiServer *ApiServer
)

// 任务的HTTP接口
type ApiServer struct {
	httpServer *http.Server
}

// 保存任务接口
// POST {"name": "job", "command": "echo hello", "cornExpr":"* * * * *"}
func handleJobSave(resp http.ResponseWriter, req *http.Request) {
	var (
		err     error
		postJob []byte
		job     common.Job
		oldJob  *common.Job
		bytes   []byte
	)
	// 任务保存在etcd中
	// 解析POST表单
	if postJob, err = ioutil.ReadAll(req.Body); err != nil {
		goto ERR
	}

	// 反序列化job
	if err = json.Unmarshal(postJob, &job); err != nil {
		goto ERR
	}

	// 保存到etcd
	if oldJob, err = G_jobMgr.SaveJob(job); err != nil {
		goto ERR
	}

	// 返回正常应答
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Header().Add("Content-Type", "application/json")
		resp.Write(bytes)
	}

	return
ERR:
	// 返回错误应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Header().Add("Content-Type", "application/json")
		resp.Write(bytes)
	}
}

// 删除任务接口
func handleJobDelete(resp http.ResponseWriter, req *http.Request) {
	var (
		err    error
		oldJob *common.Job
		bytes  []byte
		body   []byte
		delJob common.DelJob
	)

	if body, err = ioutil.ReadAll(req.Body); err != nil {
		goto ERR
	}

	if err = json.Unmarshal(body, &delJob); err != nil {
		goto ERR
	}

	// 去删除任务
	if oldJob, err = G_jobMgr.DeleteJob(delJob.Name); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		resp.Header().Add("Content-Type", "application/json")
		resp.Write(bytes)
	}

	return
ERR:
	// 错误处理
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Header().Add("Content-Type", "application/json")
		resp.Write(bytes)
	}
}

// 列举所有crontab任务
func handleJobList(resp http.ResponseWriter, req *http.Request) {
	var (
		err     error
		jobs    *common.ListJobs
		bytes   []byte
		body    []byte
		reqArgs *common.ListJobsReqArg
	)
	// 获取请求体
	if body, err = ioutil.ReadAll(req.Body); err != nil {
		fmt.Println(err, 1)
		goto ERR
	}

	if len(body) != 0 {
		// 参数解析反序列化
		if err = json.Unmarshal(body, &reqArgs); err != nil {
			fmt.Println(err, 2)
			goto ERR
		}
	} else {
		reqArgs = &common.ListJobsReqArg{LsatKeyName: common.JOB_SAVE_DIR, PageSize: 20}
	}

	// 去etcd拿取分页数据
	if jobs, err = G_jobMgr.ListJobs(reqArgs.LsatKeyName, reqArgs.PageSize); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", jobs); err == nil {
		resp.Header().Add("Content-Type", "application/json")
		resp.Write(bytes)
	}

	return

ERR:
	// 错误处理
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Header().Add("Content-Type", "application/json")
		resp.Write(bytes)
	}
}

// 强制杀死某个任务
func handleJobKill(resp http.ResponseWriter, req *http.Request) {
	var (
		err         error
		body        []byte
		killReqArgs common.KillJob
		bytes       []byte
	)

	if body, err = ioutil.ReadAll(req.Body); err != nil {
		goto ERR
	}

	if err = json.Unmarshal(body, &killReqArgs); err != nil {
		goto ERR
	}

	if err = G_jobMgr.KillJob(killReqArgs.JobName); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		resp.Header().Add("Content-Type", "application/json")
		resp.Write(bytes)
	}

	return
ERR:
	// 异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		resp.Header().Add("Content-Type", "application/json")
		resp.Write(bytes)
	}
}

// 初始化服务
func InitApiServer() error {
	var (
		mux        *http.ServeMux
		listener   net.Listener
		httpServer *http.Server
		err        error
	)
	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)

	// 启动监听端口
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
		return err
	}

	// 创建一个http服务
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}
	// 赋值单例
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	// 启动了服务端
	go httpServer.Serve(listener)

	return nil
}
