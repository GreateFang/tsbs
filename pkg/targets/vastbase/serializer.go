package vastbase

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"io"
)

type Serializer struct{}

//将生成的数据点序列化为目标数据库所需要的格式
// Serialize函数将Point p写入给定的Writer w，以便可以通过TimescaleDB加载器加载。格式为CSV，每个Point占两行，第一行是标签（tags），第二行是字段值（field values）。

// 例如：
// tags,<tag1>,<tag2>,<tag3>,...
// <measurement>,<timestamp>,<field1>,<field2>,<field3>,...
func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	buf := make([]byte, 0, 256)
	buf = append(buf, []byte("tags")...)
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	for i, v := range tagValues {
		buf = append(buf, ',')
		buf = append(buf, tagKeys[i]...)
		buf = append(buf, '=')
		buf = serialize.FastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err := w.Write(buf)
	if err != nil {
		return err
	}

	// Field row second
	buf = make([]byte, 0, 256)
	buf = append(buf, p.MeasurementName()...)
	buf = append(buf, ',')
	buf = append(buf, []byte(fmt.Sprintf("%d", p.Timestamp().UTC().UnixNano()))...)
	fieldValues := p.FieldValues()
	for _, v := range fieldValues {
		buf = append(buf, ',')
		buf = serialize.FastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err = w.Write(buf)
	return err
}
