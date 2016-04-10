using System;
using Microsoft.ServiceBus.Messaging;
using System.IO;
using System.Threading;
using System.Runtime.Serialization;
using System.Xml;

namespace BussHelvete
{
    class Program
    {
        const string newFolder = @"C:\tmp\files\new";
        const string sentFolder = @"C:\tmp\files\sent";
        const string errorFolder = @"C:\tmp\files\error";
        const string receivedFolder = @"C:\tmp\files\received";

        const string queueName = "myqueue2";
        const string errorQueueName = "myerrorqueue";

        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Valid arguments : -SEND, -RECEIVE and -ERROR" );
                return;
            }
            var mode = args[0];
            if (mode.ToUpper().Contains("SEND"))
            {
                Send();
            }
            else if (mode.ToUpper().Contains("RECEIVE"))
            {
                Receive();
            }
            else if (mode.ToUpper().Contains("ERROR"))
            {
                ProcessErrorQueue();
            }
        }

        // Send messages from the local file system to the remote queue
        private static void Send()
        {
            Console.WriteLine("------------- SEND FILES TO QUEUE ------------------");
            while (true)
            {
                try {
                    var directoryInfo = new DirectoryInfo(newFolder);
                    var fileInfos = directoryInfo.GetFiles();
                    if (fileInfos.Length == 0)
                    {
                        Console.WriteLine("No files found");
                        continue;
                    }

                    var queueClient = QueueClient.Create(queueName);
                    foreach (var fileInfo in fileInfos)
                    {
                        ProcessLocalFile(fileInfo);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }
                finally
                {
                    Thread.Sleep(5000);
                }
            }
        }

        // Receive messages from the queue
        private static void Receive()
        {
            Console.WriteLine("------------- RECEIVE MESSAGE FROM QUEUE ------------------");
            var queueClient = QueueClient.Create(queueName, ReceiveMode.ReceiveAndDelete);
            while (true)
            {
                try
                {
                    var message = queueClient.Receive(TimeSpan.FromSeconds(10));

                    if (message == null)
                    {
                        Console.WriteLine("No message found");
                        continue;
                    }

                    ProcessMessage(message);
                }
                catch (TimeoutException)
                {
                    Console.WriteLine("No message found");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }
            }
        }

        // Send a message to the error queue
        private static void SendToErrorQueue(BrokeredMessage message, string errorMessage)
        {
            try
            {
                message.Properties.Add("Error", errorMessage);
                var queueClient = QueueClient.Create(errorQueueName);
                queueClient.Send(message);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to send to Error queue: " + ex.Message);
            }
        }

        // Process a single message in the queue - save it to the received folder 
        // If there are errors, send the message to the error queue
        private static void ProcessMessage(BrokeredMessage message)
        {
            var originalMessage = message.Clone();

            try
            {
                Console.WriteLine("Processing message " + message.MessageId);

                if (!message.Properties.ContainsKey("fileName"))
                {
                    throw new Exception("No filename found");
                }

                object fileNameObj;
                var fileName = "";

                if (message.Properties.TryGetValue("fileName", out fileNameObj)) {
                    fileName = fileNameObj.ToString();
                }

                if (string.IsNullOrEmpty(fileName))
                {
                    throw new Exception("Filename is empty");
                }

                if (!fileName.Contains(".txt"))
                {
                    throw new Exception("Filename must be a text file!");
                }

                var content = message.GetBody<byte[]>();
                File.WriteAllBytes(receivedFolder + "\\" + fileName, content);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " +  ex.Message);
                SendToErrorQueue(originalMessage, ex.Message);
            }
        }

        // Process a single error message in the queue - save it to file
        private static void ProcessErrorMessage(BrokeredMessage message)
        {
            try
            {
                Console.WriteLine("Processing message " + message.MessageId);
                var messageFileName = "message_" + message.MessageId;
                var reportFileName = "message_" + message.MessageId + "_error.txt";

                object errorMessageObj;
                var errorMessage = "Unknown error";

                if (message.Properties.TryGetValue("error", out errorMessageObj))
                {
                    errorMessage = errorMessageObj.ToString();
                }

                var content = message.GetBody<byte[]>();
                File.WriteAllBytes(errorFolder + "\\" + messageFileName, content);
                File.WriteAllText(errorFolder + "\\" + reportFileName, errorMessage);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        // Process messages in the error queue
        private static void ProcessErrorQueue()
        {
            var queueClient = QueueClient.Create(errorQueueName, ReceiveMode.ReceiveAndDelete);
            Console.WriteLine("------------- PROCESSING ERROR QUEUE ------------------");
            while (true)
            {
                try
                {
                    var message = queueClient.Receive(TimeSpan.FromSeconds(10));

                    if (message == null)
                    {
                        Console.WriteLine("No message found");
                        continue;
                    }

                    ProcessErrorMessage(message);
                }
                catch (TimeoutException)
                {
                    Console.WriteLine("No message found");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }
            }
        }

        // Process a local file, send it to the queue and move the file
        private static void ProcessLocalFile(FileInfo fileInfo)
        {
            try
            {
                var queueClient = QueueClient.Create(queueName);

                var fileContents = File.ReadAllBytes(fileInfo.FullName, );
                if (fileContents.Length == 0)
                {
                    throw new Exception("File is empty");
                }

                var message = new BrokeredMessage(fileContents);
                message.Properties.Add("FileName", fileInfo.Name);

                queueClient.Send(message);

                File.Move(fileInfo.FullName, sentFolder + "\\" + fileInfo.Name);

                Console.WriteLine("Processed file " + fileInfo.Name);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                File.Move(fileInfo.FullName, errorFolder + "\\" + fileInfo.Name);
            }
        }
    }
}
