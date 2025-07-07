# Progetto SDCC

protoc     -I api/proto     -I third_party/protobuf/src     --go_out=api/gen          --go_opt=paths=source_relative     --go-grpc_out=api/gen     --go-grpc_opt=paths=source_relative     api/proto/node/node.proto
