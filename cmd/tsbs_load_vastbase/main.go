package main

import (
	"fmt"
	"github.com/blagojts/viper"
	"github.com/shirou/gopsutil/process"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/vastbase"

	"log"
	"os"
	"strings"
	"time"
)

// Parse args:
func initProgramOptions() (*vastbase.LoadingOptions, load.BenchmarkRunner, *load.BenchmarkRunnerConfig) {
	target := vastbase.NewTarget()
	loaderConf := load.BenchmarkRunnerConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	opts := vastbase.LoadingOptions{}
	viper.SetTypeByDefaultValue(true)
	opts.PostgresConnect = viper.GetString("postgres")
	opts.Host = viper.GetString("host")
	opts.Port = viper.GetString("port")
	opts.User = viper.GetString("user")
	opts.Pass = viper.GetString("pass")
	opts.ConnDB = viper.GetString("admin-db-name")
	opts.LogBatches = viper.GetBool("log-batches")

	opts.UseHypertable = viper.GetBool("use-hypertable")
	opts.ChunkTime = viper.GetDuration("chunk-time")

	opts.UseJSON = viper.GetBool("use-jsonb-tags")

	// This must be set to 'true' if you are going to test
	// distributed hypertable queries and insert. Replication
	// factor must also be set to true for distributed hypertables
	opts.InTableTag = viper.GetBool("in-table-partition-tag")

	// 	We currently use `create_hypertable` for all variations. When
	//   `replication-factor`>=1, we automatically create a distributed
	//   hypertable.
	opts.ReplicationFactor = viper.GetInt("replication-factor")
	// Currently ignored for distributed hypertables. We assume all
	// data nodes will be used based on the partition-column above
	opts.NumberPartitions = viper.GetInt("partitions")

	opts.TimeIndex = viper.GetBool("time-index")
	opts.TimePartitionIndex = viper.GetBool("time-partition-index")
	opts.PartitionIndex = viper.GetBool("partition-index")
	opts.FieldIndex = viper.GetString("field-index")
	opts.FieldIndexCount = viper.GetInt("field-index-count")

	opts.ProfileFile = viper.GetString("write-profile")
	opts.ReplicationStatsFile = viper.GetString("write-replication-stats")
	opts.CreateMetricsTable = viper.GetBool("create-metrics-table")

	opts.ForceTextFormat = viper.GetBool("force-text-format")
	opts.UseInsert = viper.GetBool("use-insert")

	loader := load.GetBenchmarkRunner(loaderConf)
	return &opts, loader, &loaderConf
}

func main() {
	opts, loader, loaderConf := initProgramOptions()

	// If specified, generate a performance profile
	if len(opts.ProfileFile) > 0 {
		// 在后台周期性地监测特定进程的CPU和内存使用情况，并将数据写入指定的文件
		go profileCPUAndMem(opts.ProfileFile)
	}

	// 检查是否指定了复制统计文件（opts.ReplicationStatsFile不为空）。如果指定了复制统计文件，
	// 代码使用go关键字启动一个并发的goroutine，调用OutputReplicationStats函数来输出复制统
	// 计信息到指定的文件，并将replicationStatsWaitGroup传递给该函数以便在输出完成后进行等待
	//var replicationStatsWaitGroup sync.WaitGroup
	//if len(opts.ReplicationStatsFile) > 0 {
	//	go OutputReplicationStats(
	//		opts.GetConnectString(loader.DatabaseName()), opts.ReplicationStatsFile, &replicationStatsWaitGroup,
	//	)
	//}

	benchmark, err := vastbase.NewBenchmark(loaderConf.DBName, opts, &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
	})
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)

}

func profileCPUAndMem(file string) {
	// 创建一个文件，并在出现错误时记录日志并终止程序
	f, err := os.Create(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var proc *process.Process
	// 进入一个无限循环，在每秒钟的定时器触发时执行以下操作
	for _ = range time.NewTicker(1 * time.Second).C {
		// 如果proc为nil，表示尚未找到目标进程。代码通过调用process.Processes()获取当前所有进程的列表，
		// 并遍历每个进程。对于每个进程，获取其命令行信息，如果命令行包含"postgres"和"INSERT"两个关键字，
		// 则将该进程赋值给proc并终止循环。
		if proc == nil {
			procs, err := process.Processes()
			if err != nil {
				panic(err)
			}
			for _, p := range procs {
				cmd, _ := p.Cmdline()
				if strings.Contains(cmd, "postgres") && strings.Contains(cmd, "INSERT") {
					proc = p
					break
				}
			}
		} else {
			// 如果proc不为nil，表示已经找到目标进程。代码通过调用proc.CPUPercent()获取该进程的CPU使用率，
			// 并通过调用proc.MemoryInfo()获取该进程的内存信息。如果获取CPU使用率或内存信息时出现错误，
			// 则将proc设置为nil并继续下一次循环。
			cpu, err := proc.CPUPercent()
			if err != nil {
				proc = nil
				continue
			}
			mem, err := proc.MemoryInfo()
			if err != nil {
				proc = nil
				continue
			}
			// 如果成功获取了CPU使用率和内存信息，代码将这些数据格式化为字符串，
			// 并使用fmt.Fprintf()将其写入文件中。数据格式为CPU使用率,实际内存使用量,
			// 虚拟内存使用量,交换内存使用量。
			fmt.Fprintf(f, "%f,%d,%d,%d\n", cpu, mem.RSS, mem.VMS, mem.Swap)
		}
	}
}
