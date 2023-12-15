package vastbase

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

const goTimeFmt = "2006-01-02 15:04:05.999999 -0700"

type BaseGenerator struct {
	UseJSON       bool
	UseTags       bool
	UseTimeBucket bool
}

func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewVastbaseDB()
}

func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, sql string) {
	q := qi.(*query.VastbaseDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	//q.Hypertable = []byte(table)
	q.SqlQuery = []byte(sql)
}

func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}

func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := iot.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	iot := &IoT{
		BaseGenerator: g,
		Core:          core,
	}

	return iot, nil
}
