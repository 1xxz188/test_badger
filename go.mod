module test_badger

go 1.18

require (
	github.com/allegro/bigcache/v3 v3.0.2
	github.com/dgraph-io/badger/v3 v3.2103.2
	github.com/stretchr/testify v1.4.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15
)

require (
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/protobuf v1.3.1 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/flatbuffers v1.12.1 // indirect
	github.com/klauspost/compress v1.12.3 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/orcaman/concurrent-map v1.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opencensus.io v0.22.5 // indirect
	golang.org/x/net v0.0.0-20201021035429-f5854403a974 // indirect
	golang.org/x/sys v0.0.0-20210124154548-22da62e12c0c // indirect
	gopkg.in/yaml.v2 v2.2.2 // indirect
)

replace github.com/dgraph-io/ristretto v0.1.0 => github.com/46bit/ristretto v0.1.0-with-arm-fix
