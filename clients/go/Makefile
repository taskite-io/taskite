GATEWAY_PROTO_ROOT := ../../gateway-protocol/src/main/resources
GATEWAY_PROTO := $(GATEWAY_PROTO_ROOT)/gateway.proto
PLACEMENT_DRIVER_PROTO_ROOT := ../../placement-driver-protocol/src/main/resources
PLACEMENT_DRIVER_PROTO := $(PLACEMENT_DRIVER_PROTO_ROOT)/placement_driver.proto
PLACEMENT_DRIVER_GO_OUT := pkg/pb/placementdriver
TOOLS_DIR := $(CURDIR)/.tools
PROTOC_GEN_GO := $(TOOLS_DIR)/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(TOOLS_DIR)/protoc-gen-go-grpc

.PHONY: generate test build

generate: $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC)
	PATH="$(TOOLS_DIR):$$PATH" protoc \
		--proto_path=$(GATEWAY_PROTO_ROOT) \
		--go_out=pkg/pb \
		--go_opt=paths=source_relative \
		--go-grpc_out=pkg/pb \
		--go-grpc_opt=paths=source_relative \
		$(GATEWAY_PROTO)
	mkdir -p $(PLACEMENT_DRIVER_GO_OUT)
	PATH="$(TOOLS_DIR):$$PATH" protoc \
		--proto_path=$(PLACEMENT_DRIVER_PROTO_ROOT) \
		--go_out=$(PLACEMENT_DRIVER_GO_OUT) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(PLACEMENT_DRIVER_GO_OUT) \
		--go-grpc_opt=paths=source_relative \
		$(PLACEMENT_DRIVER_PROTO)

$(PROTOC_GEN_GO):
	mkdir -p $(TOOLS_DIR)
	GOBIN="$(TOOLS_DIR)" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.1

$(PROTOC_GEN_GO_GRPC):
	mkdir -p $(TOOLS_DIR)
	GOBIN="$(TOOLS_DIR)" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

test:
	go test ./...

build:
	mkdir -p bin
	go build -o bin/tkctl ./cmd/tkctl
