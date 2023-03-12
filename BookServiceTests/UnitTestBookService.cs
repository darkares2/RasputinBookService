using System;
using Xunit;
using Rasputin.BookService;
using Moq;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace BookServiceTests;

public class UnitTestBookService
{
    [Fact]
    public async Task UnitTestBookServiceInvalidCommandAsync()
    {
        var sut = new QueueTriggerBookService();
        var message = new Message
        {
            Body = "{\"command\":\"invalid\",\"book\":{\"ISBN\":\"978-3-16-148410-0\",\"Title\":\"The Hitchhiker's Guide to the Galaxy\",\"Author\":\"Douglas Adams\",\"Price\":12.99}}"
        };
        var loggerMock = new Mock<ILogger>();

        // RunAsync and expect ArgumentNullException
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await sut.RunAsync(message.Body, loggerMock.Object));
    }
}