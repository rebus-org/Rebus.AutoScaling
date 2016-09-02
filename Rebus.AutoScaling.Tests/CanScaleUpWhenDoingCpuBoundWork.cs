using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Utilities;
using Rebus.Transport.InMem;
#pragma warning disable 1998

namespace Rebus.AutoScaling.Tests
{
    [TestFixture]
    public class CanScaleUpWhenDoingCpuBoundWork : FixtureBase
    {
        BuiltinHandlerActivator _activator;
        WorkerCounter _workerCounter;

        protected override void SetUp()
        {
            _activator = new BuiltinHandlerActivator();

            Using(_activator);

            Configure.With(_activator)
                .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "scaling-test"))
                .Options(o =>
                {
                    o.EnableAutoScaling(10);
                })
                .Start();

            _workerCounter = new WorkerCounter(_activator.Bus);

            Using(_workerCounter);
        }


        [Test]
        public async Task AutoScalingSavesTheDay()
        {
            var messageCount = 10;
            var counter = new SharedCounter(messageCount);

            _activator.Handle<string>(async str =>
            {
                // stall the worker thread for five seconds
                var threadId = Thread.CurrentThread.ManagedThreadId;
                Console.WriteLine($"Thread {threadId} waiting... ");
                Thread.Sleep(5000);
                Console.WriteLine($"Thread {threadId} done!");
                counter.Decrement();
            });

            await Task.WhenAll(Enumerable.Range(0, messageCount)
                .Select(number => _activator.Bus.SendLocal($"THIS IS MESSAGE {number}")));

            // 10 * 5 seconds will take about 50 s to process serially - auto-scaling to the resque!!

            counter.WaitForResetEvent(30);
        }
    }
}