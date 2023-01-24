// import dogjson from './dog.json' assert {type: "json"}

//load dependencies 
const dogjson=require("./dog.json")

const dvdjsontraining=require("./DVD-training.json")
const dvdjsontesting=require("./DVD-testing.json")
const ndjsontraining=require("./NDBench-training.json")
const ndjsontesting=require("./NDBench-testing.json")

const grpc =require("@grpc/grpc-js")
const protoLoader=require("@grpc/proto-loader")

const percentile=require("percentile")

//path to our proto file

const PROTO_FILE="./service_def.proto"

const options ={
keepCase: true,
longs: String,
enums: String,
defaults: true,
oneofs: true,
};

//Load the proto file 
const pkgDefs= protoLoader.loadSync(PROTO_FILE, options)

//Load defs into grpc
const proto = grpc.loadPackageDefinition(pkgDefs)

//create grpc server
const server = new grpc.Server()

var batches = [];

server.addService(proto.BatchTransferService.service, {
    Batches: (RFW, callback) => {
        var BenchmarkType = RFW.request.BenchmarkType;
        var DataType = RFW.request.DataType;

        var mydata;

        if (BenchmarkType == "DVDStore" && DataType == "testing") {
            mydata = JSON.parse(JSON.stringify(dvdjsontesting));
        }
        else if (BenchmarkType == "NDBench" && DataType == "testing"){
            mydata = JSON.parse(JSON.stringify(ndjsontesting));
        }
        else if (BenchmarkType == "DVDStore" && DataType == "training") {
            mydata = JSON.parse(JSON.stringify(dvdjsontraining));
        }
        else if (BenchmarkType == "NDBench" && DataType == "training"){
            mydata = JSON.parse(JSON.stringify(ndjsontraining));
        }
        var batchLength = RFW.request.BatchUnit;

        var batch = [];

        for(j=0; j<mydata.length; j++){
            for(i=0; i<batchLength; i++){
                var datapoint = {
                    CPUUtilization_Average: mydata[i].CPUUtilization_Average,
                    NetworkIn_Average: mydata[i].NetworkIn_Average,
                    NetworkOut_Average: mydata[i].NetworkOut_Average,
                    MemoryUtilization_Average: mydata[i].MemoryUtilization_Average,
                    Final_Target: mydata[i].Final_Target
                }
                batch.push(datapoint)
            }
            batches.push({list: batch})
            batch = []
        }
        callback(null, {message: "Batches Created"})
    },
    MyBatches: (RFW, callback) => {
        var batchID = RFW.request.BatchID;
        var numberOfBatches = RFW.request.BatchSize;
        var workloadMetric = RFW.request.WorkloadMetric;

        var multiplebatches = [];
        var workloadlist = [];

        for(j=batchID; j<batchID+numberOfBatches; j++){
            var currentBatch = batches[j].list;
            for (i=0; i<currentBatch.length; i++){
                workloadlist.push(currentBatch[i][workloadMetric])
            }
            multiplebatches.push(batches[j])
        }

        var analysisValue = analysis(workloadlist, RFW.request.DataAnalytics)

        RFD = {"RFW_ID": RFW.request.RFW_ID,
               "BatchID": batchID+numberOfBatches,
               "batches": multiplebatches,
               "analytics": analysisValue}
        try{
            callback(null, RFD)
        } catch(error){
            callback(error, null)
        }
    }
});

function average(nums) {
    return nums.reduce((a, b) => (a + b)) / nums.length;
}

function std_dev (array) {
    const n = array.length
    const mean = array.reduce((a, b) => a + b) / n
    return Math.sqrt(array.map(x => Math.pow(x - mean, 2)).reduce((a, b) => a + b) / n)
}

function analysis(list, analysis) {
    if (analysis == "avg")
        return average(list)
    else if (analysis == "10p")
        return percentile(10, list)
    else if (analysis == "50p")
        return percentile(50, list)
    else if (analysis == "95p")
        return percentile(95, list)
    else if (analysis == "99p")
        return percentile(99, list)
    else if (analysis == "std")
        return std_dev(list)
    else if (analysis == "min")
        return Math.min(...list)
    else if (analysis == "max")
        return Math.max(...list)
}

server.bindAsync("127.0.0.1:8080", 
grpc.ServerCredentials.createInsecure(),
function (error, port) {
    console.log(`listening on port ${port}`);
    server.start();
})