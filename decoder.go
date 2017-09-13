package sala

import "encoding/json"

type Decoder interface {
	DecodeValue([]byte) (map[string]interface{}, error)
}

type JsonDecoder uint

func (jd JsonDecoder) DecodeValue(data []byte) (map[string]interface{}, error) {
	rs := make(map[string]interface{})
	if err := json.Unmarshal(data, rs); err != nil {
		return nil, err
	}
	return rs, nil
}

var DefaultDecoder Decoder = JsonDecoder(0)
