using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Azure.Messaging.ServiceBus;

namespace Rasputin.BookService
{
    public class QueueTriggerBookService
    {
        [FunctionName("QueueTriggerBookService")]
        public async Task RunAsync([ServiceBusTrigger("ms-books", Connection = "rasputinServicebus")]string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {myQueueItem}");
            var message = JsonSerializer.Deserialize<Message>(myQueueItem, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
            var cmd = JsonSerializer.Deserialize<CmdUpdateBook>(message.Body, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
            var book = cmd.Book;
            if (cmd.Command == "create")
            {
                await InsertBookAsync(message, book, log);
            } else if (cmd.Command == "list")
            {
                await ListBooksAsync(message, book.ISBN, log);
            } else {
                log.LogError($"Command {cmd.Command} not supported");
            }
        }

        private async Task ListBooksAsync(Message receivedMessage, string iSBNList, ILogger log)
        {
            List<Books> books = new List<Books>();
            var str = Environment.GetEnvironmentVariable("sqldb_connection");
            string query = "SELECT * FROM Books";
            if (iSBNList != null)
            {
                query += " WHERE ISBN IN (";
                var isbns = iSBNList.Split(',');
                bool first = true;
                for (int i = 0; i < isbns.Length; i++)
                {
                    query += (first ? "":",") + "@Id" + i;
                    first = false;
                }
                query += ")";
            }
            using (SqlConnection connection = new SqlConnection(str))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand(query, connection)) {
                    if (iSBNList != null)
                    {
                        var isbns = iSBNList.Split(',');
                        for (int i = 0; i < isbns.Length; i++)
                        {
                            command.Parameters.AddWithValue("@Id" + i, isbns[i]);
                        }
                    }
                    using (SqlDataReader reader = await command.ExecuteReaderAsync()) {
                        while (reader.Read())
                        {
                            var book = new Books
                            {
                                ISBN = reader.GetString(0),
                                Title = reader.GetString(1),
                                Author = reader.GetString(2),
                                PublicationDate = reader.GetDateTime(3),
                                Price = reader.GetDecimal(4).ToString()
                            };
                            books.Add(book);   
                        }
                    }
                }
            }
            var message = new Message
            {
                Headers = receivedMessage.Headers,
                Body = JsonSerializer.Serialize(books, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            MessageHelper.AddContentHeader(message, "Books");
            await MessageHelper.QueueMessageAsync("api-router", message, log);
        }

        private async Task InsertBookAsync(Message receivedMessage, Books book, ILogger log)
        {
            var str = Environment.GetEnvironmentVariable("sqldb_connection");
            string query = "INSERT INTO Books (ISBN, Title, Author, publication_date, Price) VALUES (@ISBN, @Title,@Author,@PublicationDate,@Price)";
            using (SqlConnection connection = new SqlConnection(str))
            {
                connection.Open();
                using (var command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@ISBN", book.ISBN);
                    command.Parameters.AddWithValue("@Title", book.Title);
                    command.Parameters.AddWithValue("@Author", book.Author);
                    command.Parameters.AddWithValue("@PublicationDate", book.PublicationDate);
                    command.Parameters.AddWithValue("@Price", book.Price);
                    await command.ExecuteNonQueryAsync();
                }
            }
            var message = new Message
            {
                Headers = receivedMessage.Headers,
                Body = JsonSerializer.Serialize(book, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            await MessageHelper.QueueMessageAsync("api-router", message, log);

        }
    }
}
