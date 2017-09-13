package avro

import (
	"encoding/binary"
	"errors"
	"sync"

	schemaregistry "github.com/datamountaineer/schema-registry"
	"github.com/linkedin/goavro"
)

type AvroDecoder struct {
	schemaRegistryUrl string
	codecs            *sync.Map
}

func NewAvroDecoder(registry string) *AvroDecoder {
	return &AvroDecoder{
		schemaRegistryUrl: registry,
		codecs:            new(sync.Map),
	}

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

		result, ok := native.(map[string]interface{})
		if !ok {
			return nil, errors.New("decode error, invalid map type")
		}

		return result, nil
	}
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
