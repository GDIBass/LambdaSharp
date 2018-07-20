using System;
using System.IO;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Messages.Tables;
using MindTouch.LambdaSharp;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace Messages.SNSMessages {
    public class MyMessage {

        //--- Properties ---
        public string Text { get; set; }
    }

    public class Function : ALambdaEventFunction<MyMessage> {

        //-- Fields ---
        private MessageTable _table;

        //--- Methods ---
        public override Task InitializeAsync(LambdaConfig config)
        {
            var tableName = config.ReadText("MessageTable");
            _table = new MessageTable(tableName);
            return Task.CompletedTask;
        }

        public override async Task ProcessMessageAsync(MyMessage message, ILambdaContext context) {
            LogInfo(message.Text);
            var SNSMessage = new Message
            {
                MessageId = Guid.NewGuid().ToString(),
                Source = "SNS",
                Text = message.Text
            };
            await _table.InsertMessageAsync(SNSMessage);

        }
    }
}

