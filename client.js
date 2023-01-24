// Load up dependencies
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
// Path to proto file
const PROTO_FILE = "./service_def.proto";

// Options needed for loading Proto file
const options = {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
};

// Load Proto File
const pkgDefs = protoLoader.loadSync(PROTO_FILE, options);
// Load Definition into gRPC
const batchService = grpc.loadPackageDefinition(pkgDefs).BatchTransferService;

// Create the Client
const batchServiceClient = new batchService(
  "localhost:8080",
  grpc.credentials.createInsecure()
);



var RFW = {
  RFW_ID: 1,
  BenchmarkType: "NDBench",
  WorkloadMetric: "MemoryUtilization_Average",
  BatchUnit: 10,
  BatchID: 5,
  BatchSize: 5,
  DataType: "testing",
  DataAnalytics: "10p"
}

batchServiceClient.Batches(RFW, (error, RFD) => {
  if (error) {
    console.log(error);
  } else {
    console.log(RFD.message);
  }
});

batchServiceClient.MyBatches(RFW, (error, RFD) => {
  if (error) {
    console.log(error);
  } else {
    console.log(RFD);
  }
})