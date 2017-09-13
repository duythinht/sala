package sala

import "log"

func fatalOrNext(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
