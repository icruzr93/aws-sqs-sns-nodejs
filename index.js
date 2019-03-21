var async = require('async');
var AWS = require('aws-sdk');

AWS.config.region = 'us-east-1';

var sqs = new AWS.SQS();

var queueURL;
var params = {
    QueueName: "backspace-lab",
    Attributes: {
        ReceiveMessageWaitTimeSeconds: '20',
        VisibilityTimeout: "60"
    }
};

sqs.createQueue(params, function(err, data) {
    if(err) console.log(err, err.stack);
    else {
        console.log("Successfully created SQS queue URL" + data.QueueUrl)
        queueUrl = data.QueueUrl;
        waitingSQS = false;
        createMessageSNS(data.QueueUrl);
    }
});

var sns = new AWS.SNS();
function createMessageSNS() {
    var message = 'This is a message from Amazon SQS';
    sns.publish({
        Message: message,
        TargetArn:'ARN_QUEUE'
    }, function(err, data) {
        if (err) {
            console.log(err.stack);
        }
        else {
            console.log('Message sent by SNS: ' + data);
        }
    })
}

function createMessageSQS(queueUrl) {

    var messages = [];
    for (var a =0; a < 50; a++){
        messages[a] = 'This is the content for message ' + a + ".";
    }

    async.each(messages, function(content) {
        console.log("Sending message: " + content)
        tempKey = content;
        params = {
            MessageBody: content,
            QueueUrl: queueUrl
        };
        sqs.sendMessage(params, function(err, data) {
            if(err) console.log(err, err.stack);
            else console.log(data);
        })
    });
}

function createMessagesBatch(queueUrl) {
    
    var messages = [];
    for (var a =0; a < 5; a++){
        messages[a] = [];
        for (var b =0; b < 10; b++){
            messages[a][b] = 'This is the content for message ' + a + ".";
        }
    }
    
    var a = 0;
    async.each(messages, function(content) {
        console.log("Sending message: " + JSON.stringify(content))
        params = {
            Entries: [],
            QueueUrl: queueUrl
        };
        for (var b =0; b < 10; b++){
            params.Entries.push({
                MessageBody: content[b],
                Id: "Message" + (a*10+b)
            });
        }
        a++;

        sqs.sendMessageBatch(params, function(err, data) {
            if(err) console.log(err, err.stack);
            else console.log(data);
        })
    });
}


var waitingSQS = false;
var queueCounter = 0;

setInterval(function() {
    if(!waitingSQS) {  // Still busy with previous request
        if(queueCounter <= 0) {
            receiveMessages();
        }
        else --queueCounter;
    }
}, 1000);

function receiveMessages() {
    var params = {
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        VisibilityTimeout:60,
        WaitTimeSeconds: 20
    };
    waitingSQS = true;
    sqs.receiveMessage(params, function(err, data) {
        if(err) {
            waitingSQS = false;
            console.log(err, err.stack)
        }
        else {
            waitingSQS = false;
            if((typeof data.Messages !== 'undefined')&&(data.Messages.length !== 0)) {
                console.log('Received ' + data.Messages.length + " messages from SQS queue")
                processMessages(data.Messages);
            }else {
                queueCounter = 60;
                console.log('SQS queue empty, waiting for ' +  queueCounter + 's.')
            }
        }
    })
}

function processMessages(messageSQS) {
    async.each(messageSQS, function(content) {
        console.log("Processing message: " + JSON.stringify(content))
        var params = {
            QueueUrl: queueUrl, 
            ReceiptHandle: content.ReceiptHandle
        };
        sqs.deleteMessage(params, function(err, data) {
            if (err) console.log(err, err.stack)
            else {
                console.log("Delete message RequestId:" + JSON.stringify(data.ResponseMetadata.RequestId))
            }
        });
    })
}