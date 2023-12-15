package vastbase

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"time"
)

func NewTarget() targets.ImplementedTarget {
	return &vastbaseTarget{}
}

type vastbaseTarget struct {
}

func (vb *vastbaseTarget) Benchmark(targetDB string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	var loadingOptions LoadingOptions
	if err := v.Unmarshal(&loadingOptions); err != nil {
		return nil, err
	}
	return NewBenchmark(targetDB, &loadingOptions, dataSourceConfig)
}

func (vb *vastbaseTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (vb *vastbaseTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	////TODO 待确认
	//flagSet.String(flagPrefix+"ilp-bind-to", "127.0.0.1:9009", "QuestDB influx line protocol TCP ip:port")
	flagSet.String(flagPrefix+"postgres", "sslmode=disable", "PostgreSQL connection string")
	//用于指定 TimescaleDB（PostgreSQL）实例的主机名，默认为 "localhost"。
	flagSet.String(flagPrefix+"host", "localhost", "Hostname of vastbase (PostgreSQL) instance")
	// port：用于指定要连接的数据库主机上的端口号，默认为 "5432"。
	flagSet.String(flagPrefix+"port", "5432", "Which port to connect to on the database host")
	//user：用于指定连接到 PostgreSQL 的用户名，默认为 "postgres"。
	flagSet.String(flagPrefix+"user", "postgres", "User to connect to PostgreSQL as")
	//pass：用于指定连接到 PostgreSQL 的用户密码，默认为空（如果没有密码保护，留空）。
	flagSet.String(flagPrefix+"pass", "", "Password for user connecting to PostgreSQL (leave blank if not password protected)")
	//admin-db-name：用于指定用于创建额外基准数据库的连接数据库，默认与 user 相同（如果都未设置，则为 "postgres"）。
	flagSet.String(flagPrefix+"admin-db-name", "postgres", "Database to connect to in order to create additional benchmark databases.\n"+
		"By default this is the same as the `user` (i.e., `postgres` if neither is set),\n"+
		"but sometimes a user does not have its own database.")

	//log-batches：用于指定是否记录每个批次的时间。
	flagSet.Bool(flagPrefix+"log-batches", false, "Whether to time individual batches.")

	//use-hypertable：用于指定是否将表作为 hypertable（超级表）。将此标志设置为 false 可以将输入写入速度与常规 PostgreSQL 进行比较。
	flagSet.Bool(flagPrefix+"use-hypertable", true, "Whether to make the table a hypertable. Set this flag to false to check input write speed against regular PostgreSQL.")
	//use-jsonb-tags：用于指定是否将标签存储为 JSONB（而不是使用单独的带有模式的表）。
	flagSet.Bool(flagPrefix+"use-jsonb-tags", false, "Whether tags should be stored as JSONB (instead of a separate table with schema)")
	//in-table-partition-tag：用于指定分区键（例如主机名）是否也应该存在于指标 hypertable 中。
	flagSet.Bool(flagPrefix+"in-table-partition-tag", false, "Whether the partition key (e.g. hostname) should also be in the metrics hypertable")

	//replication-factor：用于设置复制因子，大于等于 1 会创建一个分布式 hypertable。
	flagSet.Int(flagPrefix+"replication-factor", 0, "Setting replication factor >= 1 will create a distributed hypertable")
	//partitions：用于指定分区数量。
	flagSet.Int(flagPrefix+"partitions", 0, "Number of partitions")
	//chunk-time：用于指定每个数据块的持续时间，例如 12 小时。
	flagSet.Duration(flagPrefix+"chunk-time", 12*time.Hour, "Duration that each chunk should represent, e.g., 12h")

	//time-index：用于指定是否在时间维度上建立索引。
	flagSet.Bool(flagPrefix+"time-index", true, "Whether to build an index on the time dimension")
	//time-partition-index：用于指定是否在时间维度上建立索引，与分区一起使用。
	flagSet.Bool(flagPrefix+"time-partition-index", false, "Whether to build an index on the time dimension, compounded with partition")
	//partition-index：用于指定是否在分区键上建立索引。
	flagSet.Bool(flagPrefix+"partition-index", true, "Whether to build an index on the partition key")
	//field-index：用于指定标签的索引类型（以逗号分隔的字符串）。
	flagSet.String(flagPrefix+"field-index", ValueTimeIdx, "index types for tags (comma delimited)")
	//field-index-count：用于指定索引字段的数量（-1 表示全部）。
	flagSet.Int(flagPrefix+"field-index-count", 0, "Number of indexed fields (-1 for all)")

	//write-profile：用于指定输出 CPU/memory profile 的文件。
	flagSet.String(flagPrefix+"write-profile", "", "File to output CPU/memory profile to")
	//write-replication-stats：用于指定输出复制统计信息的文件。
	flagSet.String(flagPrefix+"write-replication-stats", "", "File to output replication stats to")
	//create-metrics-table：用于指定是否删除现有的指标表并创建新的指标表。可用于常规表和 hypertable。
	flagSet.Bool(flagPrefix+"create-metrics-table", true, "Drops existing and creates new metrics table. Can be used for both regular and hypertable")

	//use-insert：用于提供使用批量 INSERT 命令而不是首选的 COPY 函数进行数据插入的选项。
	flagSet.Bool(flagPrefix+"use-insert", false, "Provides the option to test data inserts with batched INSERT commands rather than the preferred COPY function")
	//force-text-format：用于以文本格式发送/接收数据。
	flagSet.Bool(flagPrefix+"force-text-format", false, "Send/receive data in text format")
}

func (vb *vastbaseTarget) TargetName() string {
	return constants.FormatVastbase
}
