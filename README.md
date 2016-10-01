# Rebus.TODO

[![install from nuget](https://img.shields.io/nuget/v/Rebus.AutoScaling.svg?style=flat-square)](https://www.nuget.org/packages/Rebus.AutoScaling)

Provides an experimental auto-scaling extension for [Rebus](https://github.com/rebus-org/Rebus).

![](https://raw.githubusercontent.com/rebus-org/Rebus/master/artwork/little_rebusbus2_copy-200x200.png)

---

The auto-scaling extension will add/remove worker threads as necessary, depending on some criteria.

You enable it like this:

	Configure.With(...)
		.(...)
		.Options(o =>
		{
			o.EnableAutoScaling(10);
		})
		.Start();

in order to enable auto-scaling, adding up to 10 worker threads when things heat up.