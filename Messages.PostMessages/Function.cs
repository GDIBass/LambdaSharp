/*
 * MindTouch λ#
 * Copyright (C) 2018 MindTouch, Inc.
 * www.mindtouch.com  oss@mindtouch.com
 *
 * For community documentation and downloads visit mindtouch.com;
 * please review the licensing section.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using Messages.Tables;
using MindTouch.LambdaSharp;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace Messages.PostMessages {

    public class Function : ALambdaApiGatewayFunction {

        //-- Fields ---
        private MessageTable _table;

        //--- Methods ---
        public override async Task<APIGatewayProxyResponse> HandleRequestAsync(APIGatewayProxyRequest request, ILambdaContext context) {
            
            // add a new message to the message table
            await _table.InsertMessageAsync(new Message {
                Source = "API",
                Text = request.Body
            });
            Console.WriteLine("Message sent.");
            var message = new {WroteMessage = request.Body};

            return new APIGatewayProxyResponse {
                StatusCode = 200,
                Body = JsonConvert.SerializeObject(message)
            };
        }

        public override Task InitializeAsync(LambdaConfig config) {
            var tableName = config.ReadText("MessageTable");
            _table = new MessageTable(tableName);
            return Task.CompletedTask;
        }
    }
}
