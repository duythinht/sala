package avro

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"

	schemaregistry "github.com/datamountaineer/schema-registry"
	"github.com/linkedin/goavro"
)

type AvroDecoder struct {
	schemaRegistryUrl string
	codecs            *sync.Map
}

func (ad *AvroDecoder) DecodeValue(data []byte) (map[string]interface{}, error) {

	if schemaId, err := getSchemaId(data); err != nil {
		return nil, err
	} else {
		codec, err := ad.getCodecById(schemaId)

		if err != nil {
			return nil, err
		}
		native, _, err := codec.NativeFromBinary(data[5:])
		if err != nil {
			return nil, err
		}

		textual, err := codec.TextualFromNative(nil, native)
		result := make(map[string]interface{})
		if err := json.Unmarshal(textual, &result); err != nil {
			return nil, err
		}
		return result, nil
	}

	return nil, errors.New("Unknown error")
}

func (ad *AvroDecoder) getCodecById(id int) (*goavro.Codec, error) {
	if codec, ok := ad.codecs.Load(id); ok {
		return codec.(*goavro.Codec), nil
	}

	client, err := schemaregistry.NewClient(ad.schemaRegistryUrl)
	if err != nil {
		return nil, err
	}

	schema, err := client.GetSchemaById(id)
	if err != nil {
		return nil, err
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}

	ad.codecs.Store(id, codec)
	return codec, nil
}

func getSchemaId(data []byte) (int, error) {
	if len(data) > 5 {
		return int(binary.BigEndian.Uint32(data[1:5])), nil
	}
	return 0, errors.New("Message is too short")
}
