using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Messages.Tables;
using MindTouch.LambdaSharp;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace Messages.DeleteMessages {

    public class Function : ALambdaApiGatewayFunction {

        private MessageTable _table;

        //--- Methods ---
        public override Task InitializeAsync(LambdaConfig config)
        {
            var tableName = config.ReadText("MessageTable");
            _table = new MessageTable(tableName);
            return Task.CompletedTask;
        }

        public override async Task<APIGatewayProxyResponse> HandleRequestAsync(APIGatewayProxyRequest request, ILambdaContext context) {
            
            await _table.BatchDeleteMessagesAsync((await _table.ListMessagesAsync()).Select(m => m.MessageId));
            return new APIGatewayProxyResponse()
            {
                Body = "ok",
                StatusCode = 200,
                IsBase64Encoded = false,

            };
        }
    }
}