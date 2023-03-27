using System;

namespace Rebus.AutoScaling.Tests;

public static class DtEx
{
    public static DateTimeOffset RoundTo(this DateTimeOffset time, TimeSpan precision)
    {
        var roundedTicks = precision.Ticks*(time.Ticks/ precision.Ticks);

        return new DateTimeOffset(roundedTicks, time.Offset);
    }
}