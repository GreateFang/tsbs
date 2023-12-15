package vastbase

import (
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
)

const pgxDriver = "pgx"
const pqDriver = "postgres"

type benchmark struct {
	opts   *LoadingOptions
	ds     targets.DataSource
	dbName string
}

func NewBenchmark(dbName string, opts *LoadingOptions, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	var ds targets.DataSource
	if dataSourceConfig.Type == source.FileDataSourceType {
		ds = newFileDataSource(dataSourceConfig.File.Location)
	} else {
		panic("not implement")
	}

	return &benchmark{
		opts:   opts,
		ds:     ds,
		dbName: dbName,
	}, nil
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.ds
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	if maxPartitions > 1 {
		return &hostnameIndexer{partitions: maxPartitions}
	}
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return newProcessor(b.opts, getDriver(b.opts.ForceTextFormat), b.dbName)
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{
		opts:    b.opts,
		connDB:  b.opts.ConnDB,
		ds:      b.ds,
		driver:  getDriver(b.opts.ForceTextFormat),
		connStr: b.opts.GetConnectString(b.dbName),
	}
}

func getDriver(forceTextFormat bool) string {
	if forceTextFormat {
		return pqDriver
	} else {
		return pgxDriver
	}
}
