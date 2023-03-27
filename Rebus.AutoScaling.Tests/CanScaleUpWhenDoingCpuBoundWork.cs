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
// ReSharper disable ArgumentsStyleLiteral
// ReSharper disable RedundantArgumentDefaultValue
// ReSharper disable AccessToDisposedClosure

#pragma warning disable 1998

namespace Rebus.AutoScaling.Tests;

[TestFixture]
public class CanScaleUpWhenDoingCpuBoundWork : FixtureBase
{
    BuiltinHandlerActivator _activator;
    WorkerCounter _workerCounter;
    ListLoggerFactory _listLoggerFactory;
    IBusStarter _starter;

    protected override void SetUp()
    {
        _activator = new BuiltinHandlerActivator();

        Using(_activator);

        _listLoggerFactory = new ListLoggerFactory(outputToConsole: false);

        _starter = Configure.With(_activator)
            .Logging(l => l.Use(_listLoggerFactory))
            .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "scaling-test"))
            .Options(o => o.EnableAutoScaling(maxNumberOfWorkers: 10, adjustmentIntervalSeconds: 1))
            .Create();

        _workerCounter = new WorkerCounter(_activator.Bus);

        Using(_workerCounter);
    }

    /*
     
2016-09-04T12:26:29: * (1)
2016-09-04T12:26:30: * (1)
2016-09-04T12:26:31: * (1)
2016-09-04T12:26:32: * (1)
2016-09-04T12:26:33: * (1)
2016-09-04T12:26:34: ** (2)
2016-09-04T12:26:35: **** (4)
2016-09-04T12:26:36: **** (4)
2016-09-04T12:26:37: ***** (5)
2016-09-04T12:26:38: ****** (6)
2016-09-04T12:26:39: ******** (8)
2016-09-04T12:26:40: ********* (9)
2016-09-04T12:26:41: ********* (9)
2016-09-04T12:26:42: ********* (9)
2016-09-04T12:26:43: ********* (9)
2016-09-04T12:26:50: ******** (8)
2016-09-04T12:26:51: ******* (7)
2016-09-04T12:26:52: ****** (6)
2016-09-04T12:26:53: ****** (6)
2016-09-04T12:26:54: ***** (5)
2016-09-04T12:26:55: **** (4)
2016-09-04T12:26:56: *** (3)
2016-09-04T12:26:57: ** (2)
2016-09-04T12:26:58: * (1)
2016-09-04T12:26:59: * (1)
2016-09-04T12:27:00: * (1)
2016-09-04T12:27:01: * (1)
2016-09-04T12:27:02: * (1)
2016-09-04T12:27:03: * (1)
2016-09-04T12:27:04: * (1)             
         
         
    */
    [Test]
    public async Task AutoScalingSavesTheDay_SlowHandler()
    {
        _workerCounter.ReadingAdded += Console.WriteLine;

        var messageCount = 10;
        
        using var counter = new SharedCounter(messageCount);

        _activator.Handle<string>(async _ =>
        {
            // stall the worker thread for five seconds
            var threadId = Thread.CurrentThread.ManagedThreadId;
            Console.WriteLine($"Thread {threadId} waiting... ");
            Thread.Sleep(10000);
            Console.WriteLine($"Thread {threadId} done!");
            counter.Decrement();
        });

        _starter.Start();

        Thread.Sleep(TimeSpan.FromSeconds(3));

        await Task.WhenAll(Enumerable.Range(0, messageCount)
            .Select(number => _activator.Bus.SendLocal($"THIS IS MESSAGE {number}")));

        // 10 * 10 seconds will take about 100 s to process serially - auto-scaling to the resque!!
        counter.WaitForResetEvent(30);

        Thread.Sleep(TimeSpan.FromSeconds(13));

        CleanUpDisposables();

        Console.WriteLine();
        Console.WriteLine();
        Console.WriteLine();
        Console.WriteLine();
        Console.WriteLine();

        var readings = _workerCounter.Readings
            .GroupBy(r => r.Time.RoundTo(TimeSpan.FromSeconds(1)))
            .Select(g => new WorkerCounter.Reading(g.Key, g.Average(r => r.WorkersCount)));

        Console.WriteLine(string.Join(Environment.NewLine, readings));
    }

    [Test]
    public async Task AutoScalingSavesTheDay_ManyMessages()
    {
        _workerCounter.ReadingAdded += Console.WriteLine;

        var messageCount = 1000000;
        var counter = new SharedCounter(messageCount);

        _activator.Handle<string>(async _ => counter.Decrement());

        _starter.Start();

        Thread.Sleep(TimeSpan.FromSeconds(3));

        await Task.WhenAll(Enumerable.Range(0, messageCount)
            .Select(number => _activator.Bus.SendLocal($"THIS IS MESSAGE {number}")));

        _activator.Bus.Advanced.Workers.SetNumberOfWorkers(1);

        // 10 * 10 seconds will take about 100 s to process serially - auto-scaling to the resque!!
        counter.WaitForResetEvent(40);

        Thread.Sleep(TimeSpan.FromSeconds(8));

        CleanUpDisposables();

        Console.WriteLine();
        Console.WriteLine();
        Console.WriteLine();
        Console.WriteLine();
        Console.WriteLine();

        var readings = _workerCounter.Readings
            .GroupBy(r => r.Time.RoundTo(TimeSpan.FromSeconds(1)))
            .Select(g => new WorkerCounter.Reading(g.Key, g.Average(r => r.WorkersCount)));

        Console.WriteLine(string.Join(Environment.NewLine, readings));
    }
}