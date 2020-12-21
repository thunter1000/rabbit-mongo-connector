using System;
using System.Runtime.Loader;
using System.Text;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace rabbit_mongo_connector
{
    class Program
    {

        private static string MongoConnectionString = "mongodb://mongo:mongo@localhost:33157";
        private static string DBName = "dump";
        private static string CollectionName = "dump";
        private static string RabbitMQHost = "localhost";
        private static string RabbitQueueName = "";

        static void Main(string[] args)
        {
            var exitProgram = new ManualResetEvent(false);
            AssemblyLoadContext.Default.Unloading += ctx => exitProgram.Set();

            var client = new MongoClient(MongoConnectionString);

            var db = client.GetDatabase(DBName);

            var collection = db.GetCollection<BsonDocument>(CollectionName);

            var rabbitConnectionFactory = new ConnectionFactory()
            {
                HostName = RabbitMQHost
            };

            Console.WriteLine("Connecting to RabbitMQ");
            using (var rabbitConnection = rabbitConnectionFactory.CreateConnection())
            using (var channel = rabbitConnection.CreateModel())
            {

                channel.QueueDeclare(queue: RabbitQueueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                channel.BasicQos(0, 100, false);




                var consumer = new EventingBasicConsumer(channel);

                Console.WriteLine($"Starting to consume from RabbitMQ : {RabbitQueueName}");
                consumer.Received += (ch, ea) =>
                {
                    try
                    {
                        var j = JObject.Parse(Encoding.UTF8.GetString(ea.Body));

                        // Apply JSON manipulation here.

                        collection.InsertOneAsync(BsonDocument.Parse(j.ToString()));
                        channel.BasicAck(ea.DeliveryTag, false);
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine("Failed to parse");
                        Console.WriteLine(Encoding.UTF8.GetString(ea.Body));
                        Console.WriteLine(e);
                        throw;
                    }
                };
                channel.BasicConsume(queue: RabbitQueueName, autoAck: false, consumer: consumer);
                exitProgram.WaitOne();
            }
        }
    }
}
